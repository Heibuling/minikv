# minikv

[![ci](https://github.com/Heibuling/minikv/actions/workflows/ci.yml/badge.svg)](https://github.com/Heibuling/minikv/actions/workflows/ci.yml)

A small, auditable LSM-style key-value store written in Python with the
standard library only. Single process, no network, no background
compaction thread. It exists to be read, tested and questioned -- not to
run anything that matters.

Three results this repository reproduces on one machine:

- **~34x from batching**: 50 writes per `batch()` under `sync="always"`
  cost 7.7 us/op against 258 us/op for single puts -- one fsync, not fifty.
- **A warm read is 2.3x from SQLite**: 0.015 s for 5 000 point reads, down
  from 0.058 s, because one 4 KiB block serves ~34 lookups.
- **Nothing acknowledged is lost**: 0 losses across 140 crash points.

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
`compact()` merges every segment into one and drops tombstones; a held
`snapshot()` keeps the segments it reads on disk, so a long scan cannot be
invalidated halfway. The disk format is explicit little-endian layouts with
CRC32 on every record, block, index and bloom filter; nothing is pickled.

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
| Reclaiming disk while a snapshot is held | the files return when the last reader lets go |

Keys are capped at 64 KiB and values at 16 MiB, rejected with `ValueError`
before anything is written; empty keys are legal, and one process only.

## Durability modes

| Mode | fsync | Process kill | Machine crash |
| --- | --- | --- | --- |
| `always` (default) | before every return | safe | safe |
| `interval` | every 100 ms | safe | up to 100 ms lost |
| `never` | never | safe | may lose the tail |

A Windows process cannot fsync a directory, so a segment rename is only as
durable as NTFS itself; `stats()` says so.

## Benchmarks (this machine, 5 000 ops, 3 runs, median)

| Scenario | minikv | sqlite3 | minikv slower |
| --- | --- | --- | --- |
| sequential write | 0.081 s | 0.006 s | 14.8x |
| point read, hit | 0.015 s | 0.007 s | 2.3x |
| point read, hot set | 0.012 s | 0.006 s | 2.1x |
| point read, miss | 0.006 s | 0.004 s | 1.5x |
| full scan | 0.006 s | 0.002 s | 2.9x |
| restart, 5 000 WAL records | 0.008 s | 0.001 s | 7.0x |
| compaction, 76 segments | 0.074 s | merge alone: 0.026 s | -- |

SQLite is a C library with a page cache and years of tuning; the gap is
the interpreter, one object per record and one syscall each.  `flush()`
checkpoints the WAL, taking restart from 0.008 s to 0.004 s.

## When you should not use it

- You need durability, concurrency or support: use `sqlite3`, which is
  better at all of this and ships with Python.
- You need more than one writer or process, or values over 16 MiB.
- You need compaction, or crash recovery, to be automatic: both are manual.

## Design and documentation

- DESIGN.md -- why an LSM tree, fsync costs, the concurrency model and the
  trade-offs, each with measurements.
- BENCHMARKS.md -- method, numbers, three optimization cycles.
- docs/crash-consistency.md, docs/format.md -- invariants and byte layouts.
- CONTRIBUTING.md, SECURITY.md, CHANGELOG.md

## Development

```
python -m pip install pytest pytest-cov hypothesis mypy ruff
ruff check . && mypy       # both strict; configured in pyproject.toml
pytest --cov=minikv        # coverage floor is 85%
```

CI runs ruff, mypy, the suite and the coverage floor on Linux, macOS and
Windows for Python 3.9, 3.12 and 3.13.  MIT licensed.
