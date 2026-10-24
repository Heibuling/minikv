# minikv

A small, auditable LSM-style key-value store written in Python with the
standard library only. Single process, no network, no background
compaction thread. It exists to be read, tested and questioned -- not to
run anything that matters.

```python
from minikv import Store

with Store("data") as store:
    store.put(b"hello", b"world")
    print(store.get(b"hello"))          # b'world'

with Store("data") as store:
    for key, value in store.scan(b"h", b"i", limit=10):
        print(key, value)
```

```
$ pip install -e .
$ minikv --db ./data put hello world
$ minikv --db ./data get hello
world
```

## What it does

A write goes to a write-ahead log, then into an in-memory sorted table.
When that table reaches 4 MiB the engine freezes it, writes an immutable
segment to disk, records the new segment set in a manifest, and only then
deletes the WAL files it replaced. A read checks the memory table, then
each segment from newest to oldest, using a per-segment bloom filter and
a sparse index so most files are never touched. `compact()` merges every
segment into one and drops tombstones.

The disk format is explicit little-endian byte layouts with CRC32 on
every WAL record, every segment data block, the segment index and the
bloom filter. Nothing is pickled.

## Guarantees

| Guarantee | Mechanism | Test |
| --- | --- | --- |
| `sync="always"`: an acknowledged write survives a process kill | fsync of the WAL record before returning | 128 deterministic kill points, 0 losses |
| A torn WAL tail is truncated, never decoded | per-record CRC and length bounds | `test_corruption.py` |
| Damaged segment bytes raise, never return wrong data | per-block CRC checked before decoding | single-byte sweeps |
| A batch is all-or-nothing | one WAL record carries the whole batch | `test_engine.py` |
| Compaction never exposes a half-written file | write .tmp, fsync, rename, manifest | `test_compaction.py` |

## What it does not guarantee

| Non-guarantee | Why |
| --- | --- |
| `sync="never"` across a machine crash | the OS decides when to write back |
| `sync="interval"` loses at most one interval on power loss | the timer is 100 ms by default |
| Surviving damage to a *length* field mid-WAL | indistinguishable from a torn tail; the file is truncated there |
| More than one process | one writer, no lock file |
| Reclaiming disk during a long run | compaction defers deletion to `close()` |
| Cheap reverse scans | the range is materialised in memory |

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
| sequential write | 0.092 s | 0.006 s | 16.9x |
| random write | 0.090 s | 0.006 s | 14.8x |
| point read, hit | 0.081 s | 0.006 s | 12.6x |
| point read, miss | 0.042 s | 0.004 s | 10.3x |
| full scan | 0.005 s | 0.002 s | 2.5x |
| restart, 5 000 WAL records | 0.007 s | 0.001 s | 7.3x |
| compaction, 72 segments | 0.034 s | not applicable | -- |

SQLite is a C library with a page cache, a B-tree and years of tuning.
The gap is the interpreter, one Python object per record and one syscall
per write. BENCHMARKS.md has the cProfile output, the change that made
point reads 6.8x faster, and what is still slow on purpose.

## When you should not use it

- You need durability, concurrency or support: use `sqlite3` from the
  standard library. It is better at all of this.
- You need more than one writer, or more than one process.
- Your values are large blobs: there is no streaming and no mmap.
- You need reverse range scans over millions of rows: they materialise.
- You need compaction to run in the background: it is manual.

## Design and documentation

- DESIGN.md -- why an LSM tree, fsync costs, the concurrency model, and
  four trade-offs with measurements.
- BENCHMARKS.md -- method, numbers, a real profile, known bottlenecks.
- docs/crash-consistency.md -- invariants, fault model, injection method.
- CHANGELOG.md

## Development

```
python -m pip install pytest ruff
ruff check .
pytest                        # 86 tests, about 30 s
python benchmarks/bench.py    # writes benchmarks/results/*.json
```

CI runs ruff and the full suite on Linux, macOS and Windows for Python
3.9, 3.12 and 3.13.

MIT licensed.
