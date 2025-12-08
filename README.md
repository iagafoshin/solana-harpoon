# Solana Data Extractor

Solana Data Extractor is a blazingly fast, multi-threaded Rust tool for pulling specific Solana transactions out of raw ledger history (CAR archives from Old Faithful) and writing them to Parquet for analysis in DuckDB/Pandas. It replaces the older Python-based pipeline with a SIMD-accelerated byte filter (Aho-Corasick) and structured Arrow/Parquet output that includes logs and parsed instructions.

## Prerequisites

- Rust toolchain (Cargo).
- `aria2c` available on `PATH` for efficient CAR downloads.

## Build

```bash
cargo build --release --bin solana_extractor
```

## Usage

Key arguments:

- `--program`: Comma-separated list of Program IDs to filter.
- `--num-epochs`: How many epochs back to process.
- `--latest-epoch`: Starting epoch (default `881`).
- `-j, --concurrency`: Parallel downloads/workers (default `4`).
- `--data-dir`: Directory for CAR downloads and Parquet output.

The tool downloads missing CAR files via `aria2c`, filters transactions using Aho-Corasick against the target Program IDs (including CPI presence via account keys), and writes Parquet batches with detailed schema (signatures, block_time, fee, logs, program IDs, parsed instructions with base58 data and resolved accounts, token/account balance deltas, errors).

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
  --program "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P,675kCCbvrYniyDAzEq89v34PqPP8j9S62279pX2t" \
  -j 8
```

### 3) Deep history (1000 epochs, high concurrency)

```bash
cargo run --release --bin solana_extractor -- \
  --program "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" \
  --num-epochs 1000 \
  --latest-epoch 881 \
  -j 16 \
  --data-dir ./data
```

## Output Schema (Parquet)

Writes `.parquet` files per epoch containing:

- Core: `slot`, `block_time`, `signatures`, `program_ids`, `fee`, `accounts`.
- Balances: `account_balance_deltas` (lamports), `token_balance_deltas` (token balances).
- Execution context: `log_messages`, `err` (transaction error, if any).
- Instructions: `instructions` (struct with `program_id`, base58 `data`, `accounts` list). Nodes include both top-level and CPI instructions.
