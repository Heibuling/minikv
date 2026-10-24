"""Profile the point-read path with cProfile.

    python benchmarks/profile_reads.py            # top 20 by cumulative time
    python benchmarks/profile_reads.py --top 30

Writes a text report next to this file's results/ directory.
"""

from __future__ import annotations

import argparse
import cProfile
import io
import os
import pstats
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minikv import Store

OPS = 5000


def workload(store, keys):
    for key in keys:
        store.get(key)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--ops", type=int, default=OPS)
    args = parser.parse_args(argv)

    workdir = tempfile.mkdtemp(prefix="minikv-profile-", dir=os.getcwd())
    try:
        store = Store(os.path.join(workdir, "db"), sync="never", memtable_bytes=64 << 10)
        keys = [f"key{i:08d}".encode() for i in range(args.ops)]
        for key in keys:
            store.put(key, b"v" * 100)
        print(
            "segments:",
            store.stats()["segments"],
            "entries:",
            store.stats()["segment_entries"],
        )

        profiler = cProfile.Profile()
        profiler.enable()
        workload(store, keys)
        profiler.disable()
        store.close()

        stream = io.StringIO()
        stats = pstats.Stats(profiler, stream=stream).sort_stats("cumulative")
        stats.print_stats(args.top)
        report = stream.getvalue()
        print(report)

        results = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
        os.makedirs(results, exist_ok=True)
        path = os.path.join(results, "profile-reads.txt")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(report)
        print("wrote", path)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
