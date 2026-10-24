"""Standalone, repeated measurement of the batch-vs-single fsync cost."""

from __future__ import annotations

import os
import shutil
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minikv import Store

OPS = 200
BATCH = 50
REPEATS = 7


def run(sync, grouped):
    root = tempfile.mkdtemp(dir=os.getcwd())
    try:
        store = Store(os.path.join(root, "db"), sync=sync, memtable_bytes=64 << 10)
        started = time.perf_counter()
        if grouped:
            for base in range(0, OPS, BATCH):
                with store.batch() as batch:
                    for i in range(base, min(base + BATCH, OPS)):
                        batch.put(f"key{i:08d}".encode(), b"v" * 100)
        else:
            for i in range(OPS):
                store.put(f"key{i:08d}".encode(), b"v" * 100)
        elapsed = time.perf_counter() - started
        store.close()
        return elapsed / OPS * 1e6
    finally:
        shutil.rmtree(root, ignore_errors=True)


single = [run("always", False) for _ in range(REPEATS)]
grouped = [run("always", True) for _ in range(REPEATS)]
print(f"sync=always, {OPS} ops, {REPEATS} repeats")
print(f"single put : median {statistics.median(single):7.1f} us/op")
print(f"  runs     : {[round(x, 1) for x in single]}")
print(f"batched x{BATCH}: median {statistics.median(grouped):7.1f} us/op")
print(f"  runs     : {[round(x, 1) for x in grouped]}")
print(f"speedup    : {statistics.median(single) / statistics.median(grouped):.1f}x")
