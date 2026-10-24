"""Profile WAL replay during store open.

    python benchmarks/profile_restart.py --ops 20000 --top 18

Builds a store whose writes all live in one WAL (no flush), then profiles
the reopen.  Writes benchmarks/results/profile-restart.txt.
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
MEMTABLE = 128 << 20


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=18)
    parser.add_argument("--ops", type=int, default=OPS)
    args = parser.parse_args(argv)

    workdir = tempfile.mkdtemp(prefix="minikv-profile-r-", dir=os.getcwd())
    try:
        db = os.path.join(workdir, "db")
        store = Store(db, sync="never", memtable_bytes=MEMTABLE)
        value = b"v" * 100
        for i in range(args.ops):
            store.put(f"key{i:08d}".encode(), value)
        store.close()
        wal_bytes = sum(
            os.path.getsize(os.path.join(db, name))
            for name in os.listdir(db)
            if name.endswith(".log")
        )
        print(f"ops={args.ops} wal_bytes={wal_bytes}")

        profiler = cProfile.Profile()
        profiler.enable()
        reopened = Store(db, sync="never", memtable_bytes=MEMTABLE)
        profiler.disable()
        assert reopened.get(b"key00000000") == value
        reopened.close()

        stream = io.StringIO()
        pstats.Stats(profiler, stream=stream).sort_stats("tottime").print_stats(args.top)
        report = stream.getvalue()
        print(report)

        results = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
        os.makedirs(results, exist_ok=True)
        path = os.path.join(results, "profile-restart.txt")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(report)
        print("wrote", path)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
