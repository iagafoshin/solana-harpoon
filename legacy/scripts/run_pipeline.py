#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import subprocess
import sys
import time
from pathlib import Path


def run_cmd(cmd: list[str], step_name: str, dry_run: bool = False) -> int:
    print(f"\n{'=' * 80}")
    print(f"▶ STEP: {step_name}")
    print("CMD:", " ".join(cmd))
    print(f"{'=' * 80}\n")

    if dry_run:
        return 0

    t0 = time.time()
    proc = subprocess.run(cmd)
    dt = time.time() - t0

    if proc.returncode == 0:
        print(f"\n✅ STEP OK: {step_name} | {dt:.1f}s")
    else:
        print(f"\n❌ STEP FAIL: {step_name} | code={proc.returncode} | {dt:.1f}s")

    return proc.returncode


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run full pipeline: prepare -> flatten -> decode"
    )

    # epochs для шага prepare
    p.add_argument(
        "--from-epoch", type=int, required=True, help="start epoch (inclusive)"
    )
    p.add_argument("--to-epoch", type=int, required=True, help="end epoch (inclusive)")

    # общие
    p.add_argument("--python-bin", default=sys.executable, help="Python executable")
    p.add_argument("--scripts-dir", default="scripts", help="Directory with scripts")

    # пути данных
    p.add_argument("--raw-dir", default="data/raw")
    p.add_argument("--processed-dir", default="data/processed")
    p.add_argument("--flat-dir", default="data/flat")
    p.add_argument("--decoded-dir", default="data/decoded")

    # decode config
    p.add_argument(
        "--idl-path", default="decoder/6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P.json"
    )
    p.add_argument("--worker-path", default="decoder/decode_worker.mjs")
    p.add_argument("--workers", type=int, default=15)
    p.add_argument("--batch-items", type=int, default=5000)
    p.add_argument("--write-chunk", type=int, default=300000)
    p.add_argument("--inflight-mult", type=int, default=2)
    p.add_argument("--log-file", default="data/decoded_files.txt")
    p.add_argument("--report-file", default="data/run_report.txt")
    p.add_argument("--save-accounts-mapping", action="store_true")

    # prepare config
    p.add_argument("--prepare-batch-size", type=int, default=20000)

    # флаги поведения
    p.add_argument(
        "--force", action="store_true", help="force overwrite / ignore done logs"
    )
    p.add_argument(
        "--stop-on-error",
        action="store_true",
        default=True,
        help="stop pipeline if step failed (default: True)",
    )
    p.add_argument(
        "--no-stop-on-error",
        dest="stop_on_error",
        action="store_false",
        help="continue even if a step failed",
    )
    p.add_argument("--dry-run", action="store_true", help="print commands only")
    p.add_argument(
        "--steps",
        default="prepare,flatten,decode",
        help="comma-separated steps subset: prepare,flatten,decode",
    )

    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.to_epoch < args.from_epoch:
        raise ValueError("--to-epoch must be >= --from-epoch")

    scripts_dir = Path(args.scripts_dir)
    py = args.python_bin

    selected = {s.strip().lower() for s in args.steps.split(",") if s.strip()}
    valid = {"prepare", "flatten", "decode"}
    bad = selected - valid
    if bad:
        raise ValueError(f"Unknown steps: {bad}. Allowed: {valid}")

    plan: list[tuple[str, list[str]]] = []

    # STEP 1: prepare
    if "prepare" in selected:
        prepare_script = scripts_dir / "00_prepare_epochs.py"
        cmd = [
            py,
            str(prepare_script),
            "--epoch-start",
            str(args.from_epoch),
            "--epoch-end",
            str(args.to_epoch),
            "--raw-dir",
            args.raw_dir,
            "--out-dir",
            args.processed_dir,
            "--batch-size",
            str(args.prepare_batch_size),
        ]
        if args.force:
            cmd.append("--force")
        plan.append(("prepare", cmd))

    # STEP 2: flatten
    if "flatten" in selected:
        flatten_script = scripts_dir / "01_flatten_pump_ix.py"
        cmd = [
            py,
            str(flatten_script),
            "--input-dir",
            args.processed_dir,
            "--output-dir",
            args.flat_dir,
        ]
        if args.force:
            cmd.append("--force")
        plan.append(("flatten", cmd))

    # STEP 3: decode
    if "decode" in selected:
        decode_script = scripts_dir / "02_decode_fast.py"
        cmd = [
            py,
            str(decode_script),
            "--idl-path",
            args.idl_path,
            "--worker-path",
            args.worker_path,
            "--input-dir",
            args.flat_dir,
            "--output-dir",
            args.decoded_dir,
            "--workers",
            str(args.workers),
            "--batch-items",
            str(args.batch_items),
            "--write-chunk",
            str(args.write_chunk),
            "--inflight-mult",
            str(args.inflight_mult),
            "--log-file",
            args.log_file,
            "--report-file",
            args.report_file,
        ]
        if args.save_accounts_mapping:
            cmd.append("--save-accounts-mapping")
        if args.force:
            cmd.append("--force")
        plan.append(("decode", cmd))

    if not plan:
        print("Nothing to run. Check --steps.")
        return

    print("🚀 PIPELINE START")
    print(f"Epoch range: {args.from_epoch}..{args.to_epoch}")
    print("Steps:", " -> ".join(name for name, _ in plan))

    t0_all = time.time()
    summary = []

    for step_name, cmd in plan:
        code = run_cmd(cmd, step_name, dry_run=args.dry_run)
        summary.append((step_name, code))
        if code != 0 and args.stop_on_error:
            break

    dt_all = time.time() - t0_all

    print(f"\n{'=' * 80}")
    print("PIPELINE SUMMARY")
    for step_name, code in summary:
        status = "OK" if code == 0 else f"FAIL({code})"
        print(f"- {step_name:<10} {status}")
    print(f"Total time: {dt_all:.1f}s")
    print(f"{'=' * 80}\n")

    # финальный exit code
    for _, code in summary:
        if code != 0:
            sys.exit(code)
    sys.exit(0)


if __name__ == "__main__":
    main()
