# Solana Data Extractor

A minimal, async Rust extractor for pulling Solana transactions from Old Faithful CAR archives (file or HTTP stream) and writing per-epoch Parquet outputs ready for DuckDB/Pandas. The pipeline is single-path: ingest → parse nodes → decode transactions/metadata → exact program-invocation filter → Parquet.

## Prerequisites

- Rust toolchain (Cargo).
- `aria2c` available on `PATH` for efficient CAR downloads.

## Build

```bash
cargo build --release --bin solana_extractor
```

## Usage

CLI flags (final form):

- `--latest-epoch <u64>`: Starting epoch (default `881`).
- `--num-epochs <u64>`: How many epochs back to process (default `5`).
- `--data-dir <path>`: Directory for Parquet outputs (and local CAR files when not streaming; default `./data`).
- `--program <comma-separated pubkeys>`: Target program IDs (default: pump.fun).
- `--stream-download`: Stream CARs directly over HTTPS without saving to disk.
- `--keep-car`: When downloading CARs to disk, keep them after processing (default deletes).

Behavior:

- Local mode (default): If `epoch-{n}.car` is missing, it is downloaded via `aria2c` into `--data-dir`, processed, then deleted unless `--keep-car` is set.
- Streaming mode (`--stream-download`): Streams `https://files.old-faithful.net/{epoch}/epoch-{epoch}.car` directly into the pipeline (no local CAR files).
- Transactions are decoded from bincode; metadata is decompressed/decoded when present. Metadata failures do not drop the transaction—optional fields are left empty.
- Filtering keeps only transactions that invoke any target program (top-level or inner instructions), handling v0 loaded addresses for CPI program resolution.
- Parquet is written per epoch with ZSTD compression and batched writes.

## Examples

### 1) Pump.fun analysis (default)

```bash
cargo run --release --bin solana_extractor -- \
  --program "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" \
  --num-epochs 50
```

### 2) Multi-protocol (Pump.fun + Raydium)

```bash
cargo run --release --bin solana_extractor -- \
  --program "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P,675kCCbvrYniyDAzEq89v34PqPP8j9S62279pX2t"
```

### 3) Deep history (1000 epochs)

```bash
cargo run --release --bin solana_extractor -- \
  --program "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" \
  --num-epochs 1000 \
  --latest-epoch 881 \
  --data-dir ./data
```

### 4) Native HTTP streaming (no local CAR, no curl)

Sequentially stream CARs over HTTPS and write Parquet per epoch:

```bash
cargo run --release --bin solana_extractor -- \
  --stream-download \
  --program "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" \
  --latest-epoch 894 \
  --num-epochs 10 \
  --data-dir ./data
```

## Output Schema (Parquet)

Writes `.parquet` files per epoch containing:

- Core: `slot`, `block_time`, `signatures`, `program_ids`, `fee`, `accounts`.
- Balances: `account_balance_deltas` (lamports), `token_balance_deltas` (token balances).
- Execution context: `log_messages`, `err` (transaction error, if any).
- Instructions: `instructions` (struct with `program_id`, base58 `data`, `accounts` list). Nodes include both top-level and CPI instructions.
