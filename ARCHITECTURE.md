# ARCHITECTURE.md — solana-harpoon

## Overview

solana-harpoon is a modular Rust toolkit for working with historical Solana data from Old Faithful CAR archives. It replaces a multi-language pipeline (Rust + Python + Node.js) with a single-pass Rust implementation.

```
                                    ┌──────────────┐
                                    │  IDL JSON     │
                                    │  (Anchor)     │
                                    └──────┬───────┘
                                           │
┌─────────────┐    ┌──────────────┐    ┌───▼──────────┐    ┌──────────────┐
│ CAR source  │───▶│ harpoon-car  │───▶│harpoon-solana│───▶│harpoon-decode│
│ (file/http) │    │ Node parser  │    │ tx + meta    │    │ borsh → JSON │
└─────────────┘    └──────────────┘    └───┬──────────┘    └──────┬───────┘
                                           │                      │
                                           ▼                      ▼
                                    ┌──────────────┐    ┌──────────────┐
                                    │harpoon-export│    │harpoon-index │
                                    │ Parquet/CSV  │    │ redb (later) │
                                    └──────────────┘    └──────────────┘
```

## Crate dependency graph

```
harpoon-cli
├── harpoon-car
├── harpoon-solana  (depends on harpoon-car)
├── harpoon-decode  (standalone — only needs IDL JSON + raw bytes)
├── harpoon-export  (depends on harpoon-solana, harpoon-decode)
└── harpoon-index   (depends on harpoon-solana) [Phase 3]
```

## Key design decisions

### 1. Why workspace of crates, not a single binary?

The old code was a single binary (`solana_extractor`) that did everything. This made it impossible to reuse the CAR parser without pulling in Parquet, Arrow, Solana SDK, etc. By splitting into crates:

- `harpoon-car` can be used by anyone who just needs to read CAR files (no Solana dependency)
- `harpoon-decode` can decode Anchor instructions offline without the CAR parser
- Users can compose crates in their own pipelines
- Each crate has focused tests and clear API boundaries

### 2. CAR reading: AsyncRead vs mmap

Two code paths:

- **Local files**: `memmap2::Mmap` → wrap in a `Cursor<&[u8]>` → synchronous iterator over `RawNode`. Zero-copy, OS handles prefetch, no async overhead. This is the fast path for downloaded CAR files.
- **HTTP streaming**: `reqwest` → `StreamReader` → `AsyncRead` → existing `NodeReader`. Used for `--stream-download` mode when we don't want to save the CAR.

Both produce the same type: an iterator/stream of `RawNode`. The CAR crate should expose a unified abstraction:

```rust
pub enum CarSource {
    Mmap(MmapReader),
    Stream(Box<dyn AsyncRead + Unpin + Send>),
}
```

### 3. Anchor IDL decoder design

This replaces the Node.js worker pool. The approach:

**Loading:**
1. Parse IDL JSON (Anchor format) into internal representation
2. For each instruction: compute discriminator = `sha256("global:<ix_name>")[..8]`
3. For each event: compute discriminator = `sha256("event:<event_name>")[..8]`
4. Build `HashMap<[u8; 8], Layout>` where `Layout` describes the borsh field sequence

**Decoding:**
1. Take raw instruction data bytes (after base58 decode)
2. Read first 8 bytes → lookup in discriminator map
3. Deserialize remaining bytes according to Layout → produce `serde_json::Value`

**Layout types to support (from Anchor IDL spec):**
- Primitives: bool, u8, u16, u32, u64, u128, i8..i128, f32, f64
- `publicKey` → 32 bytes → base58 string in output
- `string` → borsh string (u32 length prefix + utf8)
- `bytes` → borsh bytes (u32 length prefix) → base64 in output
- `Vec<T>` → u32 length prefix + N items
- `Option<T>` → u8 tag (0=None, 1=Some) + value
- `defined` references → resolve from IDL `types` section (structs and enums)
- Struct → sequential fields
- Enum → u8 variant index + variant fields

**Edge cases:**
- Some programs use non-standard discriminators (not sha256-based). Support manual discriminator override in config.
- Legacy Anchor versions may have slightly different IDL format. Handle both.
- Unknown discriminators should be reported (like old pipeline's `unknown_discs` counter) but not crash.

### 4. Pipeline architecture for `ingest`

```
Stage 1 (I/O):          Stage 2 (CPU):              Stage 3 (I/O):
tokio task               rayon thread pool            dedicated thread
reads CAR nodes    →     decodes tx + meta      →     builds Arrow batches
groups by block          filters by program           writes Parquet
                         decodes IDL (if enabled)

    ╔══════════╗         ╔══════════╗              ╔══════════╗
    ║ bounded  ║         ║ bounded  ║              ║ bounded  ║
    ║ channel  ║────────▶║ channel  ║─────────────▶║ channel  ║
    ║ (blocks) ║         ║ (records)║              ║ (batches)║
    ╚══════════╝         ╚══════════╝              ╚══════════╝
```

Channel sizes provide backpressure. If writer is slow (disk I/O), CPU stage blocks, which blocks reader. No unbounded memory growth.

The old code had a similar pattern (sync_channel of size 4) but mixed async reading with rayon processing in the same task. The new version separates them cleanly.

### 5. Why redb for index (not RocksDB, not SQLite)

- redb is pure Rust, single file, zero config, no C dependencies
- ACID transactions, crash-safe
- Zero-copy reads via mmap
- Much simpler than RocksDB (no column families, no tuning)
- Perfect for: "build once, query many times" pattern
- Alternative considered: sorted Parquet with binary search (simpler but slower for point queries)

### 6. Output schema

The default Parquet schema for `ingest` (without IDL):

```
slot:           u64
block_time:     i64 (nullable)
signatures:     list<utf8>
accounts:       list<utf8>
program_ids:    list<utf8>
fee:            u64
instructions:   list<struct{program_id: utf8, data: utf8, accounts: list<utf8>}>
log_messages:   list<utf8> (nullable)
err:            utf8 (nullable)
account_balance_deltas:   list<struct{account, pre, post, delta}>
token_balance_deltas:     list<struct{mint, owner, index, pre_amount, post_amount}>
```

With `--idl`, additional columns per decoded event/instruction type, written to separate Parquet files partitioned by event name (like old pipeline).

### 7. Download strategy

- Default: `aria2c` for parallel chunk download (16 connections), then process locally. This is the fastest for batch processing.
- `--stream-download`: HTTP streaming directly into pipeline. Slower but no disk usage for CAR.
- Future: support local CAR directory (`--car-dir`) for pre-downloaded archives.
- CAR files are ~50-120 GB per epoch. Default behavior: delete after processing (`--keep-car` to retain).

### 8. Error handling philosophy

- CAR parse errors → skip node, log warning, continue (one corrupt node shouldn't kill an epoch)
- Transaction decode errors → skip transaction, increment counter, continue
- Metadata decode errors → emit transaction without metadata (optional fields become null)
- IDL decode errors → emit transaction with raw data, log unknown discriminator
- I/O errors → fatal, abort epoch, report clearly
- Every run produces a summary report (like old `ParanoidStats`) with counts of all error types

## Migration checklist from old code

### From solana_extractor.rs → multiple crates

| Old location | New location | Notes |
|---|---|---|
| `NodeReader`, `RawNode`, `Node`, `Nodes` | `harpoon-car` | As-is, add mmap path |
| `decode_protobuf_bincode()` | `harpoon-solana` | Keep dual-decode logic |
| `process_prepared_tx()` | `harpoon-solana` | Split into decode + filter + build_record |
| `matches_outer()`, `matches_inner()` | `harpoon-solana` | Program matching logic |
| `resolve_program_key()` | `harpoon-solana` | Account key resolution |
| `WhalesV1Record`, `ParsedInstruction` | `harpoon-solana` | Public structs |
| `build_schema()`, `flush_batch_sync()` | `harpoon-export` | Arrow/Parquet writing |
| `TimingStats` | `harpoon-cli` | Keep for profiling |
| `Stats`, `print_progress` | `harpoon-cli` | Progress reporting |
| download logic (aria2c) | `harpoon-cli` | Keep as subprocess call |

### From Python scripts → eliminated

| Old script | Replacement | Why |
|---|---|---|
| `00_prepare_epochs.py` | Built into `harpoon ingest` | Single-pass, no intermediate parquet |
| `01_flatten_pump_ix.py` | Built into `harpoon ingest` | Filter happens during extraction |
| `02_decode_fast.py` + Node.js | `harpoon-decode` crate | Native borsh, no IPC overhead |
| `run_pipeline.py` | `harpoon-cli` subcommands | No Python orchestration needed |

## File naming conventions

- Parquet output: `{epoch}_{program_name}.parquet` (e.g. `880_pumpfun.parquet`)
- Decoded output: `{epoch}_{event_name}.parquet` (e.g. `880_buy.parquet`, `880_sell.parquet`)
- Index files: `index.redb` (single file per dataset)
- Config: `harpoon.toml` (optional, CLI args take precedence)
