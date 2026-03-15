#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import glob
import os
from pathlib import Path

import polars as pl
from tqdm import tqdm

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
BASE_COLS = ["tx_id", "slot", "block_time", "clean_instructions"]


def flatten_one(in_path: str, out_path: str) -> int:
    lf0 = pl.scan_parquet(in_path)
    schema = lf0.collect_schema()
    cols = [c for c in BASE_COLS if c in schema.names()]

    if "clean_instructions" not in cols:
        print(f"⚠️ skip (no clean_instructions): {in_path}")
        return 0

    lf = lf0.select(cols)

    lf = (
        lf.explode("clean_instructions")
        .drop_nulls("clean_instructions")
        .with_columns(
            [
                pl.col("clean_instructions").struct.field("program").alias("program"),
                pl.col("clean_instructions").struct.field("data").alias("b58"),
                pl.col("clean_instructions").struct.field("accounts").alias("accounts"),
            ]
        )
        .filter(pl.col("program") == PUMP_PROGRAM)
        .select(["tx_id", "slot", "block_time", "b58", "accounts"])
    )

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df = lf.collect(streaming=True)
    if df.height == 0:
        return 0

    df.write_parquet(out_path, compression="snappy")
    return df.height


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Flatten pump instructions from processed parquet files."
    )
    p.add_argument("--input-dir", default="data/processed")
    p.add_argument("--output-dir", default="data/flat")
    p.add_argument(
        "--force",
        action="store_true",
        help="Перезаписывать существующие *_pump_ix.parquet",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    files = sorted(glob.glob(os.path.join(args.input_dir, "*.parquet")))
    if not files:
        print(f"No files in {args.input_dir}")
        return

    total_written = 0
    done_files = 0

    for f in tqdm(files, desc="Flatten", unit="file"):
        stem = Path(f).stem
        out = os.path.join(args.output_dir, f"{stem}_pump_ix.parquet")

        if (not args.force) and os.path.exists(out):
            continue

        if args.force and os.path.exists(out):
            os.remove(out)

        try:
            n = flatten_one(f, out)
            total_written += n
            done_files += 1
        except Exception as e:
            print(f"\n❌ flatten error: {f} -> {e}")

    print(f"✅ Flat files are in: {args.output_dir}")
    print(f"✅ Processed files: {done_files}, rows written: {total_written}")


if __name__ == "__main__":
    main()
