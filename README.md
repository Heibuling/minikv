# minikv

[![ci](https://github.com/Heibuling/minikv/actions/workflows/ci.yml/badge.svg)](https://github.com/Heibuling/minikv/actions/workflows/ci.yml)

A small, auditable LSM-style key-value store written in Python with the
standard library only. Single process, no network, no background
compaction thread. It exists to be read, tested and questioned -- not to
run anything that matters.

```python
from minikv import Store

with Store("data") as store:
    store.put(b"hello", b"world")
    print(store.get(b"hello"))          # b'world'
    for key, value in store.scan(b"h", b"i", limit=10):
        print(key, value)
```

```
$ pip install -e .
$ minikv --db ./data put hello world
$ minikv --db ./data fsck          # read back every block and every WAL
```

## What it does

A write goes to a write-ahead log, then into an in-memory sorted table.
When that table reaches 4 MiB the engine freezes it, writes an immutable
segment to disk, records the live segment set in a checksummed manifest,
and only then deletes the WAL files it replaced. A read checks the memory
table, then each segment from newest to oldest, using a recorded key range,
a bloom filter and a sparse index so most files are never opened.
`compact()` merges every segment into one and drops tombstones. The disk
format is explicit little-endian byte layouts with CRC32 on every WAL
record, segment data block, segment index and bloom filter; nothing is
pickled.

## Guarantees

| Guarantee | Mechanism | Test |
| --- | --- | --- |
| `sync="always"`: an acknowledged write survives a process kill | fsync the WAL record before returning | 140 kill points, 0 losses |
| A torn WAL tail is truncated, never decoded | per-record CRC and length bounds | `test_corruption.py` |
| Damaged segment bytes raise, never return wrong data | per-block CRC checked before decoding | single-byte sweeps |
| A batch is all-or-nothing | one WAL record carries the whole batch | `test_engine.py` |
| Compaction never exposes a half-written file | write .tmp, fsync, rename, manifest | `test_compaction.py` |

## What it does not guarantee

| Non-guarantee | Why |
| --- | --- |
| `sync="never"` across a machine crash | the OS decides when to write back |
| `sync="interval"` on power loss loses at most one interval | the timer is 100 ms by default |
| Surviving damage to a *length* field mid-WAL | indistinguishable from a torn tail; the file is truncated there |
| Two processes on one directory | the second opener gets `StoreLockedError` |
| Reclaiming disk during a long run | compaction defers deletion to `close()` |

Keys are limited to 64 KiB and values to 16 MiB; exceeding either raises
`ValueError` before anything is written. Empty keys and empty values are
legal. One writer, one process: the second opener of a directory fails.

## Durability modes

| Mode | fsync | Process kill | Machine crash |
| --- | --- | --- | --- |
| `always` (default) | before every return | safe | safe |
| `interval` | every 100 ms | safe | up to 100 ms lost |
| `never` | never | safe | may lose the tail |

A Windows process cannot fsync a directory, so publishing a segment via
rename is only as durable as NTFS itself; `stats()` reports
`fsync_dir_supported: False` there.

## Benchmarks (this machine, 5 000 ops, 3 runs, median)

| Scenario | minikv | sqlite3 | minikv slower |
| --- | --- | --- | --- |
| sequential write | 0.082 s | 0.006 s | 14.9x |
| random write | 0.111 s | 0.006 s | 18.2x |
| point read, hit | 0.058 s | 0.006 s | 9.1x |
| point read, miss | 0.004 s | 0.004 s | 1.1x |
| full scan | 0.006 s | 0.002 s | 2.7x |
| restart, 5 000 WAL records | 0.008 s | 0.001 s | 10.9x |
| compaction, 76 segments | 0.026 s | not applicable | -- |

With `sync="always"`, grouping 50 writes into one `batch()` costs
7.6 us/op against 250.5 us/op for single puts: **33x**, because the batch
pays one fsync instead of fifty. SQLite is a C library with a page cache, a
B-tree and years of tuning; the gap is the interpreter, one Python object
per record and one syscall per write. BENCHMARKS.md has both cProfile runs,
the changes they led to, and what is still slow on purpose.

## When you should not use it

- You need durability, concurrency or support: use `sqlite3`, which is
  better at all of this and ships with Python.
- You need more than one writer or process, or values over 16 MiB.
- You need compaction to run in the background: it is manual.

## Design and documentation

- DESIGN.md -- why an LSM tree, fsync costs, the concurrency model and the
  trade-offs, each with measurements.
- BENCHMARKS.md -- method, numbers, the two optimization cycles.
- docs/crash-consistency.md -- invariants, fault model, injection method.
- CONTRIBUTING.md, SECURITY.md, CHANGELOG.md

## Development

```
python -m pip install pytest pytest-cov hypothesis ruff
ruff check .
pytest --cov=minikv        # coverage floor is 85% (pyproject.toml)
python benchmarks/bench.py
```

CI runs ruff, the suite and the coverage floor on Linux, macOS and Windows
for Python 3.9, 3.12 and 3.13.  MIT licensed.
