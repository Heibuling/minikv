"""Reproducible micro-benchmarks for minikv, with a SQLite baseline.

Design rules:

* fixed random seed, so every run touches the same key sequence;
* every scenario is measured three times, and the report carries the
  median plus the spread between the fastest and slowest run;
* the SQLite baseline uses the same durability setting (no fsync) and the
  same key/value shapes, on the same machine, in the same process;
* scenarios SQLite has no equivalent for (compaction) are reported as
  null rather than silently measured twice;
* results are written as JSON so a later run can be diffed.

Usage:
    python benchmarks/bench.py                 # default: 5000 ops, 3 runs
    python benchmarks/bench.py --ops 20000 --runs 3
    python benchmarks/bench.py --quick         # small smoke run
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import random
import shutil
import sqlite3
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minikv import Store

DEFAULT_OPS = 5000
DEFAULT_VALUE_SIZE = 100
DEFAULT_RUNS = 3


def key_for(i: int) -> bytes:
    return f"key{i:08d}".encode()


def value_for(size: int) -> bytes:
    return b"v" * size


def new_dir(root: str, name: str) -> str:
    path = os.path.join(root, name)
    os.makedirs(path, exist_ok=True)
    return path


def scenario_write(store_factory, ops, value, rnd, sequential):
    store = store_factory()
    try:
        keys = [key_for(i) for i in range(ops)]
        if not sequential:
            rnd.shuffle(keys)
        started = time.perf_counter()
        for key in keys:
            store.put(key, value)
        return time.perf_counter() - started
    finally:
        store.close()


def scenario_read(store_factory, ops, value, rnd, hit):
    store = store_factory()
    try:
        keys = [key_for(i) for i in range(ops)]
        for key in keys:
            store.put(key, value)
        probe = keys if hit else [key_for(i + ops) for i in range(ops)]
        if hit:
            rnd.shuffle(probe)
        started = time.perf_counter()
        for key in probe:
            store.get(key)
        return time.perf_counter() - started
    finally:
        store.close()


def scenario_scan(store_factory, ops, value, rnd):
    store = store_factory()
    try:
        for i in range(ops):
            store.put(key_for(i), value)
        started = time.perf_counter()
        rows = sum(1 for _ in store.scan(b"", None))
        elapsed = time.perf_counter() - started
        assert rows == ops, (rows, ops)
        return elapsed
    finally:
        store.close()


def scenario_restart(root, ops, value):
    """Close a store holding 'ops' WAL records, then time opening it again."""
    db = new_dir(root, "restart")
    shutil.rmtree(db, ignore_errors=True)
    os.makedirs(db)
    store = Store(db, sync="never", memtable_bytes=128 << 20)
    for i in range(ops):
        store.put(key_for(i), value)
    store.close()
    wal_bytes = sum(
        os.path.getsize(os.path.join(db, name)) for name in os.listdir(db) if name.endswith(".log")
    )
    started = time.perf_counter()
    reopened = Store(db, sync="never", memtable_bytes=128 << 20)
    elapsed = time.perf_counter() - started
    assert reopened.get(key_for(0)) == value
    reopened.close()
    return elapsed, wal_bytes


def scenario_compact(root, ops, value):
    db = new_dir(root, "compact")
    shutil.rmtree(db, ignore_errors=True)
    os.makedirs(db)
    store = Store(db, sync="never", memtable_bytes=8192)
    for i in range(ops):
        store.put(key_for(i), value)
    segments_before = store.stats()["segments"]
    started = time.perf_counter()
    store.compact()
    elapsed = time.perf_counter() - started
    segments_after = store.stats()["segments"]
    store.close()
    return elapsed, segments_before, segments_after


def scenario_sqlite_restart(root, ops, value):
    """SQLite's equivalent of a restart: reopen the file and read one row."""
    db = os.path.join(new_dir(root, "sqlite_restart"), "bench.db")
    if os.path.exists(db):
        os.unlink(db)
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB)")
    conn.executemany("INSERT INTO kv (k, v) VALUES (?, ?)", ((key_for(i), value) for i in range(ops)))
    conn.commit()
    conn.close()

    started = time.perf_counter()
    conn = sqlite3.connect(db)
    row = conn.execute("SELECT v FROM kv WHERE k = ?", (key_for(0),)).fetchone()
    elapsed = time.perf_counter() - started
    conn.close()
    assert row is not None
    return elapsed


class SQLiteAdapter:
    """Thin adapter so the same scenario code can drive SQLite."""

    def __init__(self, path, durable=False):
        self.conn = sqlite3.connect(os.path.join(path, "bench.db"))
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA synchronous=" + ("FULL" if durable else "OFF"))
        self.conn.execute("CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB)")

    def put(self, key, value):
        self.conn.execute("INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)", (key, value))

    def get(self, key):
        row = self.conn.execute("SELECT v FROM kv WHERE k = ?", (key,)).fetchone()
        return None if row is None else row[0]

    def scan(self, start, end):
        for row in self.conn.execute("SELECT k, v FROM kv ORDER BY k"):
            yield row[0], row[1]

    def close(self):
        self.conn.commit()
        self.conn.close()


def summarize(samples, **extra):
    median = statistics.median(samples)
    spread = (max(samples) - min(samples)) / median * 100 if median else 0.0
    entry = {"median_s": median, "runs_s": samples, "spread_pct": round(spread, 2)}
    entry.update(extra)
    return entry


def run_core_suite(backend, ops, value_size, runs, root):
    value = value_for(value_size)
    out = {}

    def factory(dirname):
        path = new_dir(root, dirname)
        shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path)
        return lambda: backend(path)

    for name, fn in (
        ("seq_write", lambda f, r: scenario_write(f, ops, value, r, True)),
        ("rand_write", lambda f, r: scenario_write(f, ops, value, r, False)),
        ("read_hit", lambda f, r: scenario_read(f, ops, value, r, True)),
        ("read_miss", lambda f, r: scenario_read(f, ops, value, r, False)),
        ("scan_full", lambda f, r: scenario_scan(f, ops, value, r)),
    ):
        samples = []
        for run in range(runs):
            rnd = random.Random(1234 + run)
            make = factory(f"{name}_{run}")
            samples.append(fn(make, rnd))
        out[name] = summarize(samples, unit=f"{ops} ops", per_sec=round(ops / statistics.median(samples)))
    return out


def run_store_suite(ops, value_size, runs, root):
    """Scenarios only minikv can run: restart recovery and compaction."""
    value = value_for(value_size)
    out = {}

    samples, wal_sizes = [], []
    for run in range(runs):
        elapsed, wal_bytes = scenario_restart(os.path.join(root, f"restart_{run}"), ops, value)
        samples.append(elapsed)
        wal_sizes.append(wal_bytes)
    out["restart_recovery"] = summarize(
        samples,
        unit=f"{ops} WAL records",
        wal_bytes=int(statistics.median(wal_sizes)),
    )

    samples, counts = [], []
    for run in range(runs):
        elapsed, before, after = scenario_compact(os.path.join(root, f"compact_{run}"), ops, value)
        samples.append(elapsed)
        counts.append((before, after))
    out["compaction"] = summarize(
        samples,
        unit=f"{ops} keys",
        segments_before=counts[0][0],
        segments_after=counts[0][1],
    )
    return out


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--ops", type=int, default=DEFAULT_OPS)
    parser.add_argument("--value-size", type=int, default=DEFAULT_VALUE_SIZE)
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    parser.add_argument("--quick", action="store_true", help="40 ops, 1 run: a smoke test")
    parser.add_argument("--out", type=str, default=os.path.join(os.path.dirname(__file__), "results"))
    args = parser.parse_args(argv)

    ops = 40 if args.quick else args.ops
    runs = 1 if args.quick else args.runs

    root = tempfile.mkdtemp(prefix="minikv-bench-", dir=os.getcwd())
    try:
        started = time.time()
        minikv_results = run_core_suite(
            lambda path: Store(path, sync="never", memtable_bytes=64 << 10),
            ops,
            args.value_size,
            runs,
            os.path.join(root, "minikv"),
        )
        sqlite_results = run_core_suite(
            lambda path: SQLiteAdapter(path),
            ops,
            args.value_size,
            runs,
            os.path.join(root, "sqlite"),
        )
        minikv_results.update(run_store_suite(ops, args.value_size, runs, os.path.join(root, "store")))
        sqlite_results["restart_recovery"] = summarize(
            [scenario_sqlite_restart(os.path.join(root, f"srestart_{run}"), ops, value_for(args.value_size))
             for run in range(runs)],
            unit=f"{ops} rows, reopen + one read",
        )
        payload = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "elapsed_seconds": round(time.time() - started, 1),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
            "config": {"ops": ops, "value_size": args.value_size, "runs": runs, "seed": 1234},
            "minikv": minikv_results,
            "sqlite3": sqlite_results,
        }
    finally:
        shutil.rmtree(root, ignore_errors=True)

    os.makedirs(args.out, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%S")
    path = os.path.join(args.out, f"bench-{stamp}.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)

    print(f"wrote {path}")
    print(f"{'scenario':<18}{'minikv':>12}{'sqlite3':>12}{'minikv slower':>15}")
    for name in sorted(minikv_results):
        mine = minikv_results[name]["median_s"]
        theirs = sqlite_results.get(name, {}).get("median_s")
        ratio = "n/a" if theirs is None else f"{mine / theirs:.1f}x"
        shown = 0.0 if theirs is None else theirs
        print(f"{name:<18}{mine:>11.4f}s{shown:>11.4f}s{ratio:>15}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
