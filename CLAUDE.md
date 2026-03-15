# CLAUDE.md — solana-harpoon

## What this project is

A modular Rust toolkit for extracting, decoding, and indexing historical Solana transactions from Old Faithful CAR archives. Designed as reusable infrastructure — not tied to any specific trading strategy or program.

Target audience: researchers, auditors, bot developers, anyone who needs offline access to Solana historical data.

## Repository layout (target state)

```
solana-harpoon/
├── Cargo.toml              # workspace root
├── CLAUDE.md               # this file
├── ARCHITECTURE.md         # detailed design decisions
├── README.md
├── crates/
│   ├── harpoon-car/        # CAR format parser (AsyncRead + mmap)
│   ├── harpoon-solana/     # Solana tx/meta deserialization
│   ├── harpoon-decode/     # Generic Anchor IDL decoder (borsh, no Node.js)
│   ├── harpoon-index/      # Persistent index (redb or flat parquet)
│   ├── harpoon-export/     # Parquet / CSV / Arrow IPC writer
│   └── harpoon-cli/        # CLI binary with subcommands
├── idls/                   # Example IDL files (PumpFun, Raydium, etc.)
├── benches/                # Criterion benchmarks
└── tests/                  # Integration tests with small .car fixtures
```

## Existing code being migrated

The project is being rewritten from three separate codebases:

1. **solana-harpoon (old)** — Rust CAR parser + extractor. Located in `legacy/solana-harpoon/`.
   - `src/node.rs`, `src/node/*.rs` — CAR node parsing (CBOR). **Reuse as-is** in `harpoon-car`.
   - `src/varint.rs`, `src/util.rs` — low-level helpers. **Reuse** in `harpoon-car`.
   - `src/bin/solana_extractor.rs` — monolithic binary doing ingest+filter+parquet. **Split** logic across crates.
   - `src/bin/counter.rs` — diagnostic tool. **Becomes** `harpoon inspect` subcommand.

2. **scripts/ (Python)** — post-processing pipeline. Located in `legacy/scripts/`.
   - `00_prepare_epochs.py` — cleans raw parquet, extracts tx_id/instructions. **Eliminated** — done in single Rust pass.
   - `01_flatten_pump_ix.py` — filters by program, explodes instructions. **Eliminated** — done in Rust.
   - `02_decode_fast.py` + Node.js worker pool — Anchor IDL decoding via stdin/stdout IPC. **Replaced** by `harpoon-decode` crate doing native borsh deserialization.
   - `run_pipeline.py` — orchestrator. **Replaced** by `harpoon-cli` subcommands.
   - `export_bot_data.py` — exports model data for trading bot. **NOT included** (private/commercial).

3. **old-faithful-rpc (Rust)** — gRPC streaming client. Located in `legacy/solana_parser/`.
   - SOL balance tracking + counterparty detection. **NOT included in v1** — future feature.
   - gRPC as alternative data source may be added later.

## Build order (implement crates in this sequence)

### Phase 1: Core library crates

**Step 1: `harpoon-car`**
- Migrate `node.rs`, `node/*.rs`, `varint.rs`, `util.rs` from old codebase
- Keep the existing CBOR-based parsing as-is (it works, has tests)
- Add `mmap` support via `memmap2` for local files alongside existing `AsyncRead`
- Expose: `NodeReader`, `RawNode`, `Node`, `Nodes`, `Kind`
- Key trait: a unified iterator/stream over `RawNode` regardless of source (file, mmap, HTTP stream)
- Tests: existing unit tests from `transaction.rs` etc. should be migrated

**Step 2: `harpoon-solana`**
- Extract tx/meta decoding logic from `solana_extractor.rs` lines 402-700
- Functions: `decode_transaction(bytes) -> VersionedTransaction`
- Functions: `decode_metadata(bytes, slot) -> Option<TransactionStatusMeta>` (try protobuf first, fallback bincode)
- Functions: `resolve_full_account_keys(tx, meta) -> Vec<Pubkey>` (handles v0 loaded addresses)
- Functions: `matches_programs(tx, meta, target_programs) -> bool` (outer + inner instruction check)
- Functions: `extract_instructions(tx, meta, account_keys) -> Vec<ParsedInstruction>` (top-level + CPI)
- Keep the existing `PreparedTx` / `WhalesV1Record` pattern but as public structs
- This crate depends on: `solana-sdk`, `solana-storage-proto`, `solana-transaction-status`, `bincode`, `prost`, `bs58`

**Step 3: `harpoon-decode`**
- THIS IS THE KEY NEW CRATE — replaces the entire Python + Node.js decode pipeline
- Load Anchor IDL JSON at runtime → build `HashMap<[u8; 8], InstructionLayout>`
- Discriminator = first 8 bytes of instruction data (Anchor convention: `sha256("global:<ix_name>")[..8]`)
- For events: discriminator = `sha256("event:<event_name>")[..8]`
- Deserialize remaining bytes as borsh according to field layout from IDL
- Output: `serde_json::Value` with field names from IDL
- Must handle: nested structs, enums, Vec, Option, all borsh primitive types
- Must handle: legacy discriminator formats (some programs use different schemes)
- Public API:
  ```rust
  pub struct IdlDecoder { /* ... */ }
  impl IdlDecoder {
      pub fn from_idl_path(path: &Path) -> Result<Self>;
      pub fn from_idl_json(json: &str) -> Result<Self>;
      pub fn decode_instruction(&self, data: &[u8]) -> Result<DecodedInstruction>;
      pub fn decode_event(&self, data: &[u8]) -> Result<DecodedEvent>;
  }
  ```
- Test with PumpFun IDL (`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P.json`) — compare output against known decoded transactions from old pipeline

**Step 4: `harpoon-export`**
- Arrow RecordBatch construction + Parquet writer (migrate from `flush_batch_sync` in extractor)
- Support multiple output formats: Parquet (ZSTD), CSV, JSON Lines
- Configurable schema: user chooses which fields to include via config
- Partitioned output: split by event type (like old decode pipeline did per event_name)
- Streaming writer: don't buffer entire epoch in memory, write in batches

### Phase 2: CLI + pipeline

**Step 5: `harpoon-cli`**
Subcommands:

```
harpoon ingest --epochs 880..895 --program <pubkey> [--idl <path>] --output ./data
    # Full pipeline: download CAR → parse → filter → decode → write Parquet
    # If --idl provided: also decode instructions/events via IDL
    # If not: write raw instruction data (b58) without decoding

harpoon decode --idl <path> --input ./data/raw --output ./data/decoded
    # Offline decode: read existing Parquet with raw ix data, apply IDL decoder
    # Useful for: adding new IDL to already-extracted data

harpoon inspect --car <path>  OR  --epoch <N>
    # Stats: node counts by kind, slot range, program frequency, tx count
    # Replaces old counter.rs binary

harpoon index --input ./data --output ./index  [PHASE 3 - later]
    # Build persistent index for point queries

harpoon query --index ./index --account <pubkey>  [PHASE 3 - later]
    # Query by account/program/slot using index
```

Pipeline architecture for `ingest`:
```
[Download/mmap] → [CAR NodeReader] → [channel] → [rayon: decode tx + filter + IDL decode] → [channel] → [writer thread: Parquet]
```
Three stages, all parallel, backpressure via bounded channels.

### Phase 3: Index + query (after v1 release)

**Step 6: `harpoon-index`**
- Build during `ingest` or as separate pass
- Store: (program_id, slot, tx_offset) tuples in redb or sorted Parquet
- Enable: "give me all PumpFun txs for wallet X in epochs 880-890" without full scan

## Code style and conventions

- Rust 2024 edition (already set in old Cargo.toml)
- `thiserror` for library errors, `anyhow` only in CLI binary
- No `unwrap()` in library code, only in tests
- `#[must_use]` on builder/factory methods
- Structs that cross crate boundaries: derive `Debug, Clone`
- Keep `tikv-jemallocator` as global allocator in CLI binary
- Keep `rayon` for CPU-bound parallel processing of transaction batches
- Keep `tokio` for async I/O (CAR reading, HTTP streaming)
- Use `indicatif` for progress bars in CLI
- Use `clap` with derive macros for CLI args
- Profile with `--features timing` (carry over from old codebase)
- `cargo deny` for dependency auditing (deny.toml exists)
- `rustfmt.toml` and `.editorconfig` exist — follow them

## Performance targets

Old pipeline (Rust extract + Python flatten + Node.js decode): ~50K-100K ix/s for decode phase.
Target for new single-pass pipeline: 200K+ tx/s for ingest with IDL decode.

Key optimizations to implement:
1. mmap for local CAR files (skip async overhead)
2. Filter by program BEFORE full tx decode (check program_id_index in raw instruction)
3. Native borsh decode instead of IPC to Node.js
4. Overlap I/O and CPU via bounded channels between stages
5. Batch Arrow RecordBatch construction (current code is good, keep it)

## What NOT to include

- No trading strategies, ML models, feature engineering, or alpha-related code
- No wallet scoring, cabal detection, or bot classification
- No `export_bot_data.py` logic
- No private research notebooks or blueprints
- The `pumpfun_quant_research_blueprint.md` and `cheat_list.md` are private — do not reference

## Testing strategy

- Unit tests: each crate has its own tests (borsh decode, IDL parsing, CAR node parsing)
- Integration test: small .car fixture file (a few hundred transactions) → full pipeline → verify decoded Parquet output
- Benchmark: `criterion` benches for CAR parsing, tx decoding, IDL decoding
- CI: GitHub Actions with `cargo test`, `cargo clippy`, `cargo deny check`

## Dependencies to add (not in old Cargo.toml)

- `memmap2` — mmap support for local CAR files
- `redb` — embedded key-value store for index (Phase 3)
- `borsh` — native borsh deserialization for IDL decoder
- `sha2` — for computing Anchor discriminators from IDL
- `criterion` — benchmarks

## Dependencies to remove

- Nothing from Node.js / npm
- Python scripts are reference only, not part of the build
