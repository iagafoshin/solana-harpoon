# solana-harpoon

High-performance toolkit for extracting and decoding historical Solana data from Old Faithful CAR archives.

![Rust](https://img.shields.io/badge/rust-1.88%2B-orange)
![License](https://img.shields.io/badge/license-AGPL--3.0-blue)
[![Solana](https://img.shields.io/badge/solana-v2.2-9945FF)](https://solana.com)

## What is this

[Old Faithful](https://docs.triton.one/project-yellowstone/old-faithful-historical-solana-data) stores the complete Solana transaction history as CAR archives -- one file per epoch, 50-120 GB each. Getting program-specific data out of them currently means writing custom parsers or running full validators.

solana-harpoon does this in a single pass: download a CAR file, filter by program ID, decode instructions and events via any Anchor IDL, and write structured output as Parquet, CSV, or JSONL. Local file processing runs at 80,000+ transactions per second.

## Features

- Single-pass CAR to decoded Parquet pipeline
- Generic Anchor IDL decoder (borsh to JSON, no Node.js required)
- Three extraction modes: raw transactions, decoded events, decoded instructions
- Streaming HTTP download or local file processing (mmap)
- ZSTD-compressed Parquet output with typed Arrow schemas
- jemalloc + rayon parallelism with bounded backpressure
- Zero external runtime dependencies -- single static binary

## Quick Start

```
git clone https://github.com/lamports-dev/solana-harpoon
cd solana-harpoon
cargo build --release
```

Inspect an epoch to see what's inside:

```
./target/release/harpoon inspect --epoch 1
```

Extract all PumpFun events from epoch 700, streaming directly from Old Faithful:

```
./target/release/harpoon ingest \
  --epochs 700 \
  --program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P \
  --idl ./idls/6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P.json \
  --extract events \
  --output ./data/pumpfun \
  --stream-download
```

Process multiple epochs at once:

```
./target/release/harpoon ingest \
  --epochs 880..895 \
  --program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P \
  --output ./data/raw
```

## Commands

### ingest

Full pipeline: download CAR, parse, filter by program, optionally decode via IDL, write output.

| Flag | Description |
|---|---|
| `--epochs` | Single epoch (`700`) or range (`880..895`) |
| `--program` | Target program ID(s), comma-separated |
| `--idl` | Path to Anchor IDL JSON. Required for `--extract events` or `instructions` |
| `--extract` | Extraction mode: `raw`, `events`, or `instructions` (default: `raw`) |
| `--output` | Output directory (default: `./data`) |
| `--format` | Output format: `parquet`, `csv`, `jsonl` (default: `parquet`) |
| `--stream-download` | Stream CAR over HTTP instead of downloading first |
| `--keep-car` | Keep downloaded CAR files after processing |
| `--batch-size` | Records per Arrow batch (default: 50000) |

### decode

Offline decode: apply an IDL decoder to previously extracted raw data. Useful when you want to re-decode with a different IDL without re-downloading.

| Flag | Description |
|---|---|
| `--idl` | Path to Anchor IDL JSON |
| `--input` | Directory with raw JSONL/Parquet files |
| `--output` | Output directory for decoded files |
| `--format` | Output format: `parquet`, `csv`, `jsonl` |

### inspect

Print statistics about a CAR file: node counts by kind, slot range, transaction count.

| Flag | Description |
|---|---|
| `--car` | Path to a local CAR file |
| `--epoch` | Epoch number (downloads automatically) |

## Extraction Modes

### raw (default)

Writes full transaction records with nested fields. Each output file contains all matching transactions for an epoch.

Output schema:

```
slot, block_time, signatures, accounts, program_ids, fee,
instructions[{program_id, data, accounts}],
log_messages, err, account_balance_deltas, token_balance_deltas
```

### events

Parses `"Program data: <base64>"` entries from transaction logs, decodes them via IDL, and writes flat Parquet files partitioned by event name. Requires `--idl`.

Example: PumpFun produces `TradeEvent.parquet` with columns:

```
slot, block_time, signature, mint, sol_amount, token_amount,
is_buy, user, timestamp, virtual_sol_reserves, virtual_token_reserves
```

### instructions

Decodes instruction data via IDL and writes flat Parquet files partitioned by instruction name. Requires `--idl`.

Example: PumpFun produces `buy.parquet`, `sell.parquet`, `create.parquet`, each with the instruction's typed arguments as columns.

## Architecture

Three-stage pipeline with bounded channels for backpressure:

```
Stage 1 (I/O)          Stage 2 (CPU)            Stage 3 (I/O)
tokio / mmap            rayon thread pool         dedicated thread

 Read CAR nodes   --->   Decode tx + meta   --->   Build Arrow batches
 Parse CBOR              Filter by program         Write Parquet/CSV
                         Decode IDL (opt.)         ZSTD compress

  [bounded channel]       [bounded channel]         [bounded channel]
```

No unbounded memory growth. If the writer is slow, CPU stage blocks, which blocks the reader.

### Crates

```
harpoon-cli
├── harpoon-car       CAR format parser (async reader + mmap)
├── harpoon-solana    Solana tx/meta deserialization (bincode, protobuf)
├── harpoon-decode    Anchor IDL decoder (borsh -> JSON)
└── harpoon-export    Parquet / CSV / JSONL writers (Arrow)
```

- **harpoon-car** -- Parses Old Faithful CAR archives. Supports both `AsyncRead` (streaming HTTP) and `memmap2` (local files). Handles CBOR node deserialization with selective kind filtering.
- **harpoon-solana** -- Deserializes Solana `VersionedTransaction` and `TransactionStatusMeta`. Resolves v0 address lookup tables, matches program IDs across outer and inner (CPI) instructions, extracts balance deltas.
- **harpoon-decode** -- Loads any Anchor IDL JSON at runtime, computes discriminators via SHA-256, and decodes borsh-serialized instruction data and events into `serde_json::Value`. Handles nested structs, enums, Option, Vec, and all borsh primitive types.
- **harpoon-export** -- Constructs Arrow RecordBatches and writes ZSTD-compressed Parquet, CSV, or JSON Lines. Supports partitioned output (one file per event/instruction name).

## Using Your Own IDL

harpoon works with any Anchor program, not just PumpFun. To decode a different program:

1. Get the IDL JSON for your target program (from the program's repo, or via `anchor idl fetch <program-id>`)
2. Run ingest with `--program` and `--idl` pointing to your IDL:

```
./target/release/harpoon ingest \
  --epochs 900 \
  --program <your-program-id> \
  --idl ./idls/your_program.json \
  --extract events \
  --output ./data/your_program
```

The decoder automatically computes discriminators from the IDL and maps instruction/event data to named fields.

## Server Setup

For batch processing of many epochs, a dedicated server is recommended. See `setup.sh` for automated provisioning.

Requirements:
- Ubuntu 24.04 (or similar Linux)
- 8+ GB RAM
- `aria2` for parallel chunk downloads (16 connections)
- NVMe storage for CAR file I/O

Providers with fast network to Old Faithful endpoints (Hetzner, OVH, etc.) will see the best download speeds.

## Performance

Measured on Hetzner AX52 (AMD Ryzen 7 7700, 64 GB RAM, NVMe):

| Metric | Value |
|---|---|
| Local file ingest (mmap) | ~84,000 tx/s |
| Streaming ingest (HTTP) | ~40,000 tx/s |
| CAR download via aria2 | ~227 MB/s |
| IDL decode overhead | < 5% vs raw extraction |

The pipeline is I/O-bound when streaming. With local files, CPU (rayon) becomes the bottleneck. The early program filter skips zstd metadata decompression for non-matching transactions, which eliminates 90%+ of decompression work on typical epochs.

## License

[AGPL-3.0](LICENSE)
