"""Command line interface: get, put, del, scan, stats, compact, bench.

Keys and values are UTF-8 on the command line and bytes on disk.  Use the
Python API when you need raw bytes.

Exit codes: 0 success, 1 key not found, 2 usage error.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from .engine import Store
from .wal import SYNC_MODES


def _store(args: argparse.Namespace) -> Store:
    return Store(
        args.db,
        sync=args.sync,
        memtable_bytes=args.memtable_bytes,
    )


def cmd_put(args: argparse.Namespace) -> int:
    with _store(args) as store:
        store.put(args.key.encode("utf-8"), args.value.encode("utf-8"))
    return 0


def cmd_get(args: argparse.Namespace) -> int:
    with _store(args) as store:
        value = store.get(args.key.encode("utf-8"))
    if value is None:
        print(f"(not found) {args.key}", file=sys.stderr)
        return 1
    payload = value if args.no_newline else value + b"\n"
    sys.stdout.buffer.write(payload)
    sys.stdout.buffer.flush()
    return 0


def cmd_del(args: argparse.Namespace) -> int:
    with _store(args) as store:
        store.delete(args.key.encode("utf-8"))
    return 0


def cmd_scan(args: argparse.Namespace) -> int:
    start = args.start.encode("utf-8")
    end = args.end.encode("utf-8") if args.end is not None else None
    with _store(args) as store:
        for key, value in store.scan(start, end, limit=args.limit):
            sys.stdout.buffer.write(key + b"\t" + value + b"\n")
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    with _store(args) as store:
        stats = store.stats()
    for name, value in stats.items():
        print(f"{name}: {value}")
    return 0


def cmd_compact(args: argparse.Namespace) -> int:
    with _store(args) as store:
        result = store.compact()
    print(
        f"merged {result['segments_merged']} segment(s) into "
        f"{result['segments_after']} with {result['entries']} live entries"
    )
    return 0


def cmd_fsck(args: argparse.Namespace) -> int:
    """Read back every block of every segment and every WAL record."""
    with _store(args) as store:
        report = store.verify()
    for entry in report["segments"]:
        if entry.get("ok"):
            print(f"segment {entry['name']}: ok ({entry.get('entries', 0)} entries)")
        else:
            print(f"segment {entry['name']}: FAILED {entry.get('error')}")
    for wal_entry in report["wal_files"]:
        if wal_entry["ok"]:
            print(
                f"{wal_entry['name']}: ok "
                f"({wal_entry['records']} records, {wal_entry['bytes']} bytes)"
            )
        else:
            print(
                f"{wal_entry['name']}: {wal_entry['problem']} record "
                f"after {wal_entry['bytes']} bytes"
            )
    if report["ok"]:
        print("fsck: no problems found")
        return 0
    print(f"fsck: {len(report['errors'])} problem(s) found")
    return 1


def cmd_bench(args: argparse.Namespace) -> int:
    """A deliberately small in-process benchmark; see benchmarks/bench.py."""
    count = args.count
    payload = b"v" * args.value_size
    with Store(args.db, sync=args.sync) as store:
        started = time.perf_counter()
        for i in range(count):
            store.put(f"key{i:08d}".encode(), payload)
        write_seconds = time.perf_counter() - started

        started = time.perf_counter()
        hits = sum(1 for i in range(count) if store.get(f"key{i:08d}".encode()) is not None)
        read_seconds = time.perf_counter() - started

        started = time.perf_counter()
        scanned = sum(1 for _ in store.scan(b"key", None))
        scan_seconds = time.perf_counter() - started

    print(f"writes   {count} in {write_seconds:.3f}s  -> {count / write_seconds:,.0f} ops/s")
    print(f"reads    {hits} in {read_seconds:.3f}s  -> {hits / read_seconds:,.0f} ops/s")
    print(f"scan     {scanned} rows in {scan_seconds:.3f}s -> {scanned / scan_seconds:,.0f} rows/s")
    print(f"sync     {args.sync}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="minikv", description="Embedded LSM-style key-value store.")
    parser.add_argument("--db", default="./minikv-data", help="database directory (default: ./minikv-data)")
    parser.add_argument("--sync", default=os.environ.get("MINIKV_SYNC", "always"), choices=SYNC_MODES)
    parser.add_argument("--memtable-bytes", type=int, default=4 << 20)
    sub = parser.add_subparsers(dest="command", required=True)

    put = sub.add_parser("put", help="store a key")
    put.add_argument("key")
    put.add_argument("value")
    put.set_defaults(func=cmd_put)

    get = sub.add_parser("get", help="read a key")
    get.add_argument("key")
    get.add_argument("--no-newline", action="store_true")
    get.set_defaults(func=cmd_get)

    delete = sub.add_parser("del", help="delete a key")
    delete.add_argument("key")
    delete.set_defaults(func=cmd_del)

    scan = sub.add_parser("scan", help="scan a range")
    scan.add_argument("--start", default="")
    scan.add_argument("--end", default=None)
    scan.add_argument("--limit", type=int, default=None)
    scan.set_defaults(func=cmd_scan)

    stats = sub.add_parser("stats", help="print store statistics")
    stats.set_defaults(func=cmd_stats)

    compact = sub.add_parser("compact", help="merge all segments")
    compact.set_defaults(func=cmd_compact)

    fsck = sub.add_parser("fsck", help="verify every segment block and WAL record")
    fsck.set_defaults(func=cmd_fsck)

    bench = sub.add_parser("bench", help="quick in-process benchmark")
    bench.add_argument("--count", type=int, default=2000)
    bench.add_argument("--value-size", type=int, default=100)
    bench.set_defaults(func=cmd_bench)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
