"""Profile the write path with cProfile.

    python benchmarks/profile_writes.py            # top 20 by cumulative time
    python benchmarks/profile_writes.py --ops 50000 --sync never

Writes a text report into benchmarks/results/.
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

OPS = 20000


def workload(store, keys, value):
    for key in keys:
        store.put(key, value)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--ops", type=int, default=OPS)
    parser.add_argument("--sync", default="never", choices=["always", "interval", "never"])
    parser.add_argument("--memtable-bytes", type=int, default=64 << 10)
    args = parser.parse_args(argv)

    workdir = tempfile.mkdtemp(prefix="minikv-profile-w-", dir=os.getcwd())
    try:
        store = Store(
            os.path.join(workdir, "db"),
            sync=args.sync,
            memtable_bytes=args.memtable_bytes,
        )
        keys = [f"key{i:08d}".encode() for i in range(args.ops)]
        value = b"v" * 100

        profiler = cProfile.Profile()
        profiler.enable()
        workload(store, keys, value)
        profiler.disable()
        segments = store.stats()["segments"]
        store.close()

        stream = io.StringIO()
        stats = pstats.Stats(profiler, stream=stream).sort_stats("tottime")
        stats.print_stats(args.top)
        report = stream.getvalue()
        print(report)
        print(f"segments flushed: {segments}")

        results = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
        os.makedirs(results, exist_ok=True)
        path = os.path.join(results, "profile-writes.txt")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(report)
        print("wrote", path)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
