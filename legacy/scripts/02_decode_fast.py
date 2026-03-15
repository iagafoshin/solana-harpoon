#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import datetime
import glob
import os
import subprocess
import time
from collections import Counter, defaultdict, deque
from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


class ParanoidStats:
    def __init__(self):
        self.total_processed = 0
        self.success_count = 0
        self.error_count = 0
        self.legacy_fixes = 0
        self.events = Counter()
        self.instructions = Counter()
        self.errors = Counter()
        self.unknown_discs = Counter()

    def update_local(self, local: dict):
        self.total_processed += local["processed"]
        self.success_count += local["success"]
        self.error_count += local["errors"]
        self.legacy_fixes += local["legacy_fixes"]
        self.events.update(local["events"])
        self.instructions.update(local["instructions"])
        self.errors.update(local["error_types"])
        self.unknown_discs.update(local["unknown_discs"])

    def save_report(self, report_file: str):
        Path(report_file).parent.mkdir(parents=True, exist_ok=True)
        tp = max(1, self.total_processed)

        with open(report_file, "w", encoding="utf-8") as f:
            f.write("=== PUMP.FUN DECODING REPORT ===\n")
            f.write(f"Updated: {datetime.datetime.now().isoformat()}\n")
            f.write(f"Total Items: {self.total_processed:,}\n")
            f.write(
                f"✅ Success:   {self.success_count:,} ({self.success_count / tp * 100:.2f}%)\n"
            )
            f.write(
                f"❌ Errors:    {self.error_count:,} ({self.error_count / tp * 100:.4f}%)\n"
            )
            f.write(f"🔧 Fixes:     {self.legacy_fixes:,}\n\n")

            f.write("--- EVENTS ---\n")
            for k, v in self.events.most_common():
                f.write(f"{k:<35}: {v:,}\n")

            f.write("\n--- INSTRUCTIONS ---\n")
            for k, v in self.instructions.most_common():
                f.write(f"{k:<35}: {v:,}\n")

            f.write("\n--- TOP ERRORS ---\n")
            for k, v in self.errors.most_common(15):
                f.write(f"{k:<50}: {v:,}\n")

            if self.unknown_discs:
                f.write("\n--- UNKNOWN DISCRIMINATORS ---\n")
                for k, v in self.unknown_discs.most_common(30):
                    f.write(f"Hex: {k:<20} Count: {v:,}\n")


class NodePool:
    def __init__(self, n: int, worker_path: str, idl_path: str):
        self.n = n
        self.worker_path = worker_path
        self.idl_path = idl_path
        self.procs = []
        self.rr = 0

        for _ in range(n):
            self.procs.append(self._spawn_one())

    def _spawn_one(self):
        p = subprocess.Popen(
            ["node", self.worker_path, self.idl_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        assert p.stdin and p.stdout
        return p

    def restart(self, idx: int):
        old = self.procs[idx]
        try:
            old.kill()
        except Exception:
            pass
        self.procs[idx] = self._spawn_one()

    def close(self):
        for p in self.procs:
            try:
                p.kill()
            except Exception:
                pass

    def submit_batch(self, items_bytes: bytes):
        idx = self.rr
        self.rr = (self.rr + 1) % self.n
        p = self.procs[idx]

        if p.poll() is not None:
            self.restart(idx)
            p = self.procs[idx]

        p.stdin.write(items_bytes)
        return idx

    def read_n_lines(self, idx: int, n: int):
        p = self.procs[idx]
        out = []
        for _ in range(n):
            line = p.stdout.readline()
            if not line:
                break
            out.append(line)
        return out


def load_done(log_file: str) -> set[str]:
    if not os.path.exists(log_file):
        return set()
    with open(log_file, "r", encoding="utf-8") as f:
        return set(x.strip() for x in f if x.strip())


def mark_done(log_file: str, name: str):
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(name + "\n")


def make_jsonl(items: list[dict]) -> bytes:
    return b"".join(orjson.dumps(x) + b"\n" for x in items)


def save_rows_parquet(
    output_base_dir: str,
    event_name: str,
    rows: list[dict],
    part_idx: int,
    source_stem: str,
) -> int:
    if not rows:
        return part_idx
    out_dir = Path(output_base_dir) / event_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{source_stem}_part_{part_idx:03d}.parquet"
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, out_path, compression="snappy")
    return part_idx + 1


def extract_row(decoded_msg: dict, meta: dict, save_accounts_mapping: bool):
    if not decoded_msg.get("ok"):
        return None, None, ("WorkerCrash", None)

    res = decoded_msg.get("res", {})
    if not res.get("ok"):
        disc = res.get("discHex")
        err = res.get("error", "DecodeFail")
        return None, None, (err, disc)

    event_name = res.get("name") or "Unknown"
    row = {
        "tx_id": meta["tx_id"],
        "slot": meta["slot"],
        "block_time": meta["block_time"],
        "event_type": res.get("kind"),
        **(res.get("data") or {}),
        "is_legacy_fix": bool(res.get("versionMismatchFixed")),
    }

    if save_accounts_mapping and meta.get("accounts") is not None:
        row["accounts_raw"] = meta["accounts"]

    return event_name, row, None


def iter_flat_batches(file_path: str, batch_size: int, save_accounts_mapping: bool):
    pf = pq.ParquetFile(file_path)
    cols = ["tx_id", "slot", "block_time", "b58"] + (
        ["accounts"] if save_accounts_mapping else []
    )
    for batch in pf.iter_batches(batch_size=batch_size, columns=cols):
        yield batch


def process_one_file(
    file_path: str,
    pool: NodePool,
    global_stats: ParanoidStats,
    output_base_dir: str,
    batch_items_per_worker: int,
    write_chunk: int,
    inflight_mult: int,
    save_accounts_mapping: bool,
):
    stem = Path(file_path).stem
    pf = pq.ParquetFile(file_path)
    total_rows = pf.metadata.num_rows

    buffers = defaultdict(list)
    part_counters = defaultdict(int)

    local = {
        "processed": 0,
        "success": 0,
        "errors": 0,
        "legacy_fixes": 0,
        "events": Counter(),
        "instructions": Counter(),
        "error_types": Counter(),
        "unknown_discs": Counter(),
    }

    pbar_read = tqdm(total=total_rows, desc=f"{stem} read", unit="ix", leave=False)
    pbar_dec = tqdm(total=total_rows, desc=f"{stem} decoded", unit="ix", leave=False)

    inflight = deque()

    def flush_inflight():
        while inflight:
            idx, n, metas = inflight.popleft()
            lines = pool.read_n_lines(idx, n)

            if len(lines) != n:
                missing = n - len(lines)
                p = pool.procs[idx]
                if p.poll() is not None:
                    err_tail = b""
                    try:
                        if p.stderr:
                            err_tail = p.stderr.read()[-2000:]
                    except Exception:
                        pass
                    print(
                        f"\n❌ Node worker[{idx}] died. Restart. Missing={missing}. "
                        f"stderr tail:\n{err_tail.decode('utf-8', errors='ignore')}\n"
                    )
                    pool.restart(idx)

                for _ in range(missing):
                    local["processed"] += 1
                    local["errors"] += 1
                    local["error_types"]["ShortRead"] += 1
                    pbar_dec.update(1)

            for line, meta in zip(lines, metas):
                local["processed"] += 1
                pbar_dec.update(1)

                try:
                    msg = orjson.loads(line)
                except Exception:
                    local["errors"] += 1
                    local["error_types"]["BadJSON"] += 1
                    continue

                event_name, row, errinfo = extract_row(msg, meta, save_accounts_mapping)
                if event_name and row:
                    local["success"] += 1

                    if row.get("event_type") == "instruction":
                        local["instructions"][event_name] += 1
                    else:
                        local["events"][event_name] += 1

                    if row.get("is_legacy_fix"):
                        local["legacy_fixes"] += 1

                    buffers[event_name].append(row)
                    if len(buffers[event_name]) >= write_chunk:
                        part_counters[event_name] = save_rows_parquet(
                            output_base_dir,
                            event_name,
                            buffers[event_name],
                            part_counters[event_name],
                            stem,
                        )
                        buffers[event_name].clear()
                else:
                    local["errors"] += 1
                    err_msg, disc = errinfo if errinfo else ("Unknown", None)
                    short = str(err_msg).split(":")[0][:50]
                    local["error_types"][short] += 1
                    if disc:
                        local["unknown_discs"][disc] += 1

    batch_items = []
    batch_metas = []

    t0 = time.time()
    processed_at_last = 0

    for batch in iter_flat_batches(
        file_path, batch_size=100_000, save_accounts_mapping=save_accounts_mapping
    ):
        tx_id = batch.column("tx_id").to_pylist()
        slot = batch.column("slot").to_pylist()
        bt = batch.column("block_time").to_pylist()
        b58 = batch.column("b58").to_pylist()
        accs = (
            batch.column("accounts").to_pylist()
            if save_accounts_mapping
            else [None] * len(b58)
        )

        n = len(b58)
        pbar_read.update(n)

        for i in range(n):
            payload = {"id": 0, "b58": b58[i]}
            meta = {
                "tx_id": tx_id[i],
                "slot": slot[i],
                "block_time": bt[i],
                "accounts": accs[i],
            }

            batch_items.append(payload)
            batch_metas.append(meta)

            if len(batch_items) >= batch_items_per_worker:
                blob = make_jsonl(batch_items)
                idx = pool.submit_batch(blob)
                inflight.append((idx, len(batch_items), batch_metas))
                batch_items = []
                batch_metas = []

                if len(inflight) >= pool.n * inflight_mult:
                    flush_inflight()

        if local["processed"] - processed_at_last >= 200_000:
            dt = time.time() - t0
            rate = local["processed"] / dt if dt > 0 else 0
            pbar_dec.set_postfix({"ix/s": f"{rate:,.0f}", "err": local["errors"]})
            processed_at_last = local["processed"]

    if batch_items:
        blob = make_jsonl(batch_items)
        idx = pool.submit_batch(blob)
        inflight.append((idx, len(batch_items), batch_metas))

    flush_inflight()

    for event_name, rows in buffers.items():
        if rows:
            part_counters[event_name] = save_rows_parquet(
                output_base_dir, event_name, rows, part_counters[event_name], stem
            )
            rows.clear()

    pbar_read.close()
    pbar_dec.close()

    global_stats.update_local(local)
    dt = max(1e-6, time.time() - t0)
    return local, local["processed"] / dt


def parse_args():
    p = argparse.ArgumentParser(
        description="Fast decode flattened Pump ix files via Node worker pool."
    )
    p.add_argument(
        "--idl-path", default="decoder/6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P.json"
    )
    p.add_argument("--worker-path", default="decoder/decode_worker.mjs")
    p.add_argument("--input-dir", default="data/flat")
    p.add_argument("--output-dir", default="data/decoded")
    p.add_argument("--log-file", default="data/decoded_files.txt")
    p.add_argument("--report-file", default="data/run_report.txt")

    p.add_argument("--workers", type=int, default=15)
    p.add_argument("--batch-items", type=int, default=5000)
    p.add_argument("--write-chunk", type=int, default=300_000)
    p.add_argument("--inflight-mult", type=int, default=2)

    p.add_argument("--save-accounts-mapping", action="store_true")
    p.add_argument(
        "--force",
        action="store_true",
        help="Игнорировать decoded_files.txt и декодить всё заново",
    )
    return p.parse_args()


def main():
    args = parse_args()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    files = sorted(glob.glob(os.path.join(args.input_dir, "*_pump_ix.parquet")))
    done = set() if args.force else load_done(args.log_file)
    todo = [f for f in files if Path(f).name not in done]

    print(f"🚀 FAST DECODER | Found: {len(files)} | ToDo: {len(todo)}")
    print(f"📄 Report: {args.report_file}")
    print(
        f"⚙️ Workers={args.workers} | Batch={args.batch_items} | "
        f"WriteChunk={args.write_chunk} | InflightMult={args.inflight_mult}"
    )

    if not todo:
        return

    global_stats = ParanoidStats()
    pool = NodePool(args.workers, args.worker_path, args.idl_path)

    files_processed = 0
    try:
        for f in tqdm(todo, desc="Files", unit="file"):
            try:
                local, rate = process_one_file(
                    file_path=f,
                    pool=pool,
                    global_stats=global_stats,
                    output_base_dir=args.output_dir,
                    batch_items_per_worker=args.batch_items,
                    write_chunk=args.write_chunk,
                    inflight_mult=args.inflight_mult,
                    save_accounts_mapping=args.save_accounts_mapping,
                )
                mark_done(args.log_file, Path(f).name)
                files_processed += 1

                print(
                    f"✅ {Path(f).name}: processed={local['processed']:,}, "
                    f"ok={local['success']:,}, err={local['errors']:,}, rate={rate:,.0f} ix/s"
                )

                if files_processed % 3 == 0 or local["errors"] > 100:
                    global_stats.save_report(args.report_file)

            except Exception as e:
                print(f"\n❌ ERROR {f}: {e}")
                continue

        global_stats.save_report(args.report_file)

    except KeyboardInterrupt:
        print("\n🛑 Interrupted. Saving report…")
        global_stats.save_report(args.report_file)
    finally:
        pool.close()


if __name__ == "__main__":
    main()
