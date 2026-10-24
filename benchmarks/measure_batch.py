"""How far does one fsync per batch stretch?

With sync="always" every single put pays an fsync (~250 us here).  A
batch() puts N operations into one WAL record: one CRC, one fsync.  This
sweep measures the resulting per-operation cost across batch sizes, with
and without fsync, so the point where batching stops helping is visible
rather than assumed.

    python benchmarks/measure_batch.py
    python benchmarks/measure_batch.py --ops 500 --repeats 5

Writes benchmarks/results/batch-curve-<stamp>.json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minikv import Store

SIZES = (1, 5, 10, 25, 50, 100, 250, 500)
OPS = 500
REPEATS = 5
VALUE = b"v" * 100


def measure(sync: str, ops: int, batch_size: int) -> float:
    """Return microseconds per operation."""
    root = tempfile.mkdtemp(dir=os.getcwd())
    try:
        store = Store(os.path.join(root, "db"), sync=sync, memtable_bytes=64 << 10)
        started = time.perf_counter()
        for base in range(0, ops, batch_size):
            with store.batch() as group:
                for index in range(base, min(base + batch_size, ops)):
                    group.put(f"key{index:08d}".encode(), VALUE)
        elapsed = time.perf_counter() - started
        store.close()
        return elapsed / ops * 1e6
    finally:
        shutil.rmtree(root, ignore_errors=True)


def median_of(sync: str, ops: int, size: int, repeats: int) -> tuple[float, list[float]]:
    runs = [measure(sync, ops, size) for _ in range(repeats)]
    return statistics.median(runs), runs


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--ops", type=int, default=OPS)
    parser.add_argument("--repeats", type=int, default=REPEATS)
    parser.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "results"))
    args = parser.parse_args(argv)

    rows = []
    print(f"sync=always and sync=never, {args.ops} ops, median of {args.repeats} runs")
    print(f"{'batch':>6} {'fsyncs':>7} {'always us/op':>13} {'never us/op':>12} {'speedup':>9}")
    for size in SIZES:
        batched_always, runs_always = median_of("always", args.ops, size, args.repeats)
        batched_never, runs_never = median_of("never", args.ops, size, args.repeats)
        rows.append(
            {
                "batch_size": size,
                "fsyncs_per_op": round(1 / size, 6),
                "always_us_per_op": round(batched_always, 2),
                "never_us_per_op": round(batched_never, 2),
                "always_runs": [round(x, 2) for x in runs_always],
                "never_runs": [round(x, 2) for x in runs_never],
            }
        )
        speedup = rows[0]["always_us_per_op"] / batched_always
        print(
            f"{size:>6} {max(1, args.ops // size):>7} "
            f"{batched_always:>13.2f} {batched_never:>12.2f} {speedup:>8.1f}x"
        )

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "python": sys.version.split()[0],
        "ops": args.ops,
        "repeats": args.repeats,
        "value_bytes": len(VALUE),
        "rows": rows,
    }
    os.makedirs(args.out, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%S")
    path = os.path.join(args.out, f"batch-curve-{stamp}.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
