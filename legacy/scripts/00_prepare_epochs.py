#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import gc
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

IGNORED_PROGRAMS = {
    "ComputeBudget111111111111111111111111111111",
    "11111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
    "SysvarRent111111111111111111111111111111111",
    "SysvarRecentB1ockHashes11111111111111111111",
    "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ",
}


def _to_str(value: Any) -> Any:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return str(value)
    return value


def _is_empty_seq(x: Any) -> bool:
    """
    Безопасная проверка пустоты для list/tuple/np.ndarray/pyarrow list-like.
    Избегает ошибки:
    "The truth value of an array with more than one element is ambiguous"
    """
    if x is None:
        return True
    try:
        return len(x) == 0
    except Exception:
        return False


def _normalize_accounts(accounts: Any) -> list:
    if accounts is None:
        return []
    try:
        # np.ndarray / tuple / list -> list
        return list(accounts)
    except Exception:
        return []


def clean_instructions(instructions_list: Any) -> list[dict]:
    if _is_empty_seq(instructions_list):
        return []

    cleaned: list[dict] = []
    for instr in instructions_list:
        if not isinstance(instr, dict):
            continue

        pid = _to_str(instr.get("program_id"))
        if pid in IGNORED_PROGRAMS:
            continue

        data_field = _to_str(instr.get("data"))
        cleaned.append(
            {
                "program": pid,
                "data": data_field,
                "accounts": _normalize_accounts(instr.get("accounts", [])),
            }
        )
    return cleaned


def get_tx_id_safe(signatures: Any) -> Any:
    if _is_empty_seq(signatures):
        return None
    try:
        first = signatures[0]
    except Exception:
        return None
    return _to_str(first)


def decode_err(err: Any) -> Any:
    if err is None:
        return None
    return _to_str(err)


def process_batch(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["tx_id"] = out["signatures"].apply(get_tx_id_safe)
    out["is_error"] = out["err"].notna()
    out["error_msg"] = out["err"].apply(decode_err)
    out["clean_instructions"] = out["instructions"].apply(clean_instructions)

    return out[
        [
            "slot",
            "block_time",
            "tx_id",
            "is_error",
            "error_msg",
            "clean_instructions",
        ]
    ]


def process_one_epoch(
    epoch: int,
    raw_dir: str,
    out_dir: str,
    batch_size: int,
    skip_if_exists: bool,
) -> dict:
    input_file = Path(raw_dir) / f"whales-epoch-{epoch}.parquet"
    output_file = Path(out_dir) / f"{epoch}.parquet"

    if not input_file.exists():
        print(f"⏭️  epoch={epoch}: raw не найден: {input_file}")
        return {"epoch": epoch, "status": "missing_input", "rows": 0}

    output_file.parent.mkdir(parents=True, exist_ok=True)

    if skip_if_exists and output_file.exists():
        print(f"⏭️  epoch={epoch}: output уже есть, пропускаю: {output_file}")
        return {"epoch": epoch, "status": "skipped_exists", "rows": 0}

    if output_file.exists():
        output_file.unlink()

    print(f"\n📦 epoch={epoch}")
    print(f"📂 IN : {input_file}")
    print(f"💾 OUT: {output_file}")

    columns_to_load = ["slot", "block_time", "signatures", "err", "instructions"]
    parquet_file = pq.ParquetFile(str(input_file))

    writer = None
    total_rows = 0

    try:
        for i, batch in enumerate(
            parquet_file.iter_batches(batch_size=batch_size, columns=columns_to_load),
            start=1,
        ):
            pdf = batch.to_pandas()
            if pdf.empty:
                continue

            clean_df = process_batch(pdf)
            if clean_df.empty:
                continue

            table = pa.Table.from_pandas(clean_df, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(
                    str(output_file), table.schema, compression="snappy"
                )

            writer.write_table(table)
            total_rows += len(clean_df)

            if i % 5 == 0:
                print(f"   batch {i} | total rows: {total_rows}", end="\r")

            del pdf, clean_df, table
            gc.collect()
    finally:
        if writer is not None:
            writer.close()

    if total_rows > 0:
        print(f"\n✅ epoch={epoch}: готово, строк={total_rows}")
        return {"epoch": epoch, "status": "ok", "rows": total_rows}

    print(f"\n⚠️ epoch={epoch}: пусто, ничего не записали")
    return {"epoch": epoch, "status": "empty", "rows": 0}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prepare processed parquet by epoch.")
    p.add_argument("--epoch-start", type=int, required=True)
    p.add_argument("--epoch-end", type=int, required=True)
    p.add_argument("--raw-dir", default="data/raw")
    p.add_argument("--out-dir", default="data/processed")
    p.add_argument("--batch-size", type=int, default=20_000)
    p.add_argument(
        "--force", action="store_true", help="Перезаписывать существующий output"
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.epoch_end < args.epoch_start:
        raise ValueError("--epoch-end должен быть >= --epoch-start")

    results = []
    for epoch in range(args.epoch_start, args.epoch_end + 1):
        try:
            res = process_one_epoch(
                epoch=epoch,
                raw_dir=args.raw_dir,
                out_dir=args.out_dir,
                batch_size=args.batch_size,
                skip_if_exists=not args.force,
            )
            results.append(res)
        except Exception as e:
            print(f"\n💥 epoch={epoch}: ошибка: {e}")
            results.append(
                {"epoch": epoch, "status": "error", "rows": 0, "err": str(e)}
            )

    ok = [r for r in results if r["status"] == "ok"]
    print("\n=== SUMMARY ===")
    print(f"OK epochs: {len(ok)} / {len(results)}")
    if ok:
        print(f"Total rows written: {sum(r['rows'] for r in ok)}")


if __name__ == "__main__":
    main()