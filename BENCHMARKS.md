# Benchmarks

Every number here was produced on the machine below, by the commands
below, and the raw JSON is committed under benchmarks/results/.

## Machine

| | |
| --- | --- |
| CPU | Intel Core Ultra 9 275HX (24 logical cores) |
| RAM | 31.4 GiB |
| Disk | UMIS RPJYJ1T24MML1AWY SSD (C:, 192 GiB free) |
| OS | Windows 11 (win32), CPython 3.12.14 |
| SQLite | as shipped with CPython 3.12, in-process |

## Method

```
python benchmarks/bench.py --ops 5000 --runs 3
python benchmarks/measure_batch.py          # the fsync-batching number
```

- 5 000 operations per scenario, 100-byte values, keys `key00000000`...
- Each scenario runs three times in a fresh directory; the table reports
  the median and the spread `(max - min) / median`.
- Random orders use a fixed seed (1234 + run index), so runs compare.
- minikv runs with `sync="never"` and a 64 KiB memtable, so the engine is
  measured rather than the disk.  SQLite runs with `journal_mode=OFF` and
  `synchronous=OFF`, the matching "no fsync" setting.
- "Restart" closes a store holding 5 000 WAL records and times the
  reopen plus one verified read.  For SQLite it is `connect()` plus one
  indexed read on the same 5 000 rows.
- "Compaction" and "batch fsync" have no SQLite equivalent and are
  reported for minikv only.

## Results

Canonical run: `benchmarks/results/bench-20261024T123521.json`.

| Scenario | minikv median | spread | sqlite3 median | spread | minikv slower |
| --- | --- | --- | --- | --- | --- |
| sequential write | 0.082 s | 3.1% | 0.006 s | 5.7% | 14.9x |
| random write | 0.111 s | 20.2% | 0.006 s | 6.0% | 18.2x |
| point read, hit | 0.058 s | 4.1% | 0.006 s | 4.9% | 9.1x |
| point read, miss | 0.004 s | 0.9% | 0.004 s | 3.4% | 1.1x |
| full scan | 0.006 s | 4.4% | 0.002 s | 2.0% | 2.7x |
| restart (5 000 WAL records) | 0.008 s | 14.1% | 0.001 s | 8.0% | 10.9x |
| compaction (76 segments) | 0.026 s | 13.2% | not applicable | -- | -- |

Throughput equivalents: ~61 000 writes/s, ~86 000 point reads/s and
~1 160 000 probes/s for a key that does not exist, single-threaded, with
no fsync.

Reproducibility: the preceding run
(`bench-20261024T123518.json`) produced 0.081 / 0.083 / 0.059 / 0.004 /
0.006 / 0.008 / 0.027 s for the same rows.  Every scenario is within 3%
except random write, which varies by up to 20% on this machine (it is the
row most sensitive to filesystem cache state), and fsync-bound timings.

### Why SQLite wins, and where it does not

SQLite is a C library: rows are compact page records, lookups go through a
B-tree with a page cache, and the Python layer does one `sqlite3_step`
per call.  minikv pays, per operation:

- a Python function call and one `bytes` object per key and value;
- `struct.pack` plus one copy of key+value to build each WAL record;
- a dict lookup, or a block decode, per read;
- one `write()` syscall per record.

The write gap (14.9x) is interpreter overhead plus one syscall per record;
the hit-read gap (9.1x) is per-record decoding in Python.

Two rows deserve a note.  **Point-read misses are at parity (1.1x)**
because a segment whose recorded key range does not contain the probe is
rejected with two byte comparisons, before any bloom filter or file
access.  **A full scan is only 2.7x slower** because both engines stream,
and minikv pays one `crc32()` per 4 KiB block rather than per record.

## Optimization 1 (v1.0): caching segment file handles

Point reads started at 0.573 s for 5 000 operations -- 76x slower than
SQLite, far worse than the write gap and not explained by the design.
cProfile over 5 000 gets found `_io.open` at 0.457 s of 0.738 s (62%):
every `get()` reopened the segment file, and a `with open(path, "rb")`
costs about 94 us on this machine, against 5 ms for all the actual block
reads put together.

Keeping one handle per segment (guarded by a per-segment lock) changed the
read row from 0.573 s to 0.081 s.  Two consequences had to be handled: on
Windows an open file cannot be deleted, so compaction stops unlinking its
inputs and retires them until `close()`; and retired files must not
reappear after a restart, so the live set moved into a checksummed
`MANIFEST`.  That manifest costs one fsync per flush, which is why the
write rows got 14% slower in the same change.

## Optimization 2 (v1.1): the write path, and segment key ranges

Two measurements drove this round.

### (a) The write profile

`python benchmarks/profile_writes.py --ops 20000` (20 000 puts, 38
flushes, `sync="never"`):

```
ncalls  tottime  cumtime  function
 20000    0.131    0.131  {method 'write' of '_io.FileIO' objects}   <-- 24%
   152    0.056    0.056  {built-in method _io.open}
    76    0.042    0.042  {built-in method nt.replace}
    76    0.034    0.034  {built-in method nt.fsync}
 20000    0.023    0.044  encoding.py:119(encode_wal_record)
 19646    0.020    0.048  segment.py:98(_Bloom.add)
157168    0.020    0.028  segment.py:92(_Bloom._positions)            <-- generator
```

The largest single item is the one syscall per record, which is
deliberate: `sync="never"` still hands each record to the kernel, because
the durability table promises that a process kill loses nothing.  The
next two items were fixable:

1. `_Bloom._positions` was a generator resumed 157 168 times for 19 646
   keys.  The hashing is now plain arithmetic inside `add` and
   `might_contain`: **0.048 s to 0.031 s** cumulative.
2. `encode_wal_record` built the record body once for the CRC and
   concatenated key+value a second time for the record itself.  It now
   builds the body once, checksums it, and prefixes the four CRC bytes:
   **0.044 s to 0.037 s** cumulative.

A third attempt was measured and reverted: rolling the CRC over the three
pieces (`crc32(head)`, `crc32(key, crc)`, `crc32(value, crc)`) tripled the
number of `zlib.crc32` calls (60 052 to 100 052) and made the encoder
slower, so the single-call version stayed.

### (b) Segment key ranges

Point-read misses were spending time checking a bloom filter for every
segment.  Each segment now records its `(min_key, max_key)` in the index
block, so a read rejects a segment with two byte comparisons, and a scan
skips non-overlapping segments without opening them at all.

### Before and after (v1.0 -> v1.1)

| Scenario | v1.0 | v1.1 | change |
| --- | --- | --- | --- |
| sequential write | 0.0920 s | 0.0816 s | 11% faster |
| point read, hit | 0.0806 s | 0.0584 s | 28% faster |
| point read, miss | 0.0421 s | 0.0043 s | ~10x |
| full scan | 0.0053 s | 0.0056 s | unchanged (noise) |
| compaction | 0.0335 s | 0.0259 s | 23% faster |
| restart | 0.0073 s | 0.0082 s | 12% slower |
| random write | 0.0902 s | 0.1109 s | within run-to-run noise |

The restart regression is the price of streaming WAL replay in 64 KiB
chunks instead of reading the file whole: recovery must no longer hold an
entire WAL in memory, which the earlier code did.  It is 0.9 ms on this
workload, and it removes an unbounded-memory failure mode.

## Batching: where one fsync per batch stops paying

With `sync="always"` every single `put` pays its own fsync (~250 us
here).  A `batch()` puts many operations into one WAL record: one CRC and
one fsync.  `python benchmarks/measure_batch.py` sweeps the batch size,
with and without fsync, so the point where batching stops helping is
visible rather than assumed.  500 operations, median of 5 runs:

| batch size | fsyncs per op | `always` us/op | `never` us/op | speedup |
| --- | --- | --- | --- | --- |
| 1 | 1 | 258.50 | 9.21 | 1.0x |
| 5 | 1/5 | 56.63 | 2.87 | 4.6x |
| 10 | 1/10 | 27.35 | 2.09 | 9.5x |
| 25 | 1/25 | 11.67 | 1.50 | 22.2x |
| 50 | 1/50 | 7.70 | 1.33 | 33.6x |
| 100 | 1/100 | 4.35 | 1.18 | 59.5x |
| 250 | 1/250 | 2.49 | 1.16 | 103.7x |
| 500 | 1/500 | 2.03 | 1.12 | 127.3x |

Two things are worth reading off this table.

**The `never` column is the hill batching climbs toward.**  Without fsync
a batched operation costs 1.1-1.3 us, and that barely moves with the batch
size: it is the cost of the WAL record, not of the fsync.  At batch 500,
`always` (2.03 us) is within 2x of `never` (1.12 us) -- the fsync has
been amortized almost entirely away.

**The returns flatten after about 100.**  Going from 50 to 500 removes
5.7 us of fsync cost per operation but only 4.2 us of total time, because
everything below ~1.1 us is the record itself.  A bigger batch also means
more memory held and a longer window of writes that a power loss can take
with it, which is the durability trade `batch()` exists to let a caller
make deliberately.  50 was the number quoted in the README because it is
where a single batch stops dominating the cost; 500 is where the curve
goes flat.

## Known bottlenecks, and what is deliberately not optimized

- **fsync latency.** In `sync="always"` a single write costs one fsync
  (~0.25 ms here).  Group commit across *concurrent* writers is not
  implemented; `batch()` covers the single-writer case, which is the one
  the API offers.
- **One syscall per record.** Buffering the WAL would remove it, but it
  would also break the documented promise that `sync="never"` survives a
  process kill, because buffered bytes never reached the kernel.
- **Per-record Python objects.** Decoding is the reason hit reads are 9x
  slower than SQLite; a C extension is out of scope by the project's
  constraints.
- **Compaction is O(live data).** It rewrites every live byte once per
  pass.  A leveled design would rewrite less.  Out of scope for v1.
- **Reverse scans decode a block forward, then walk it backwards**, which
  is correct and streaming but not the cheapest possible traversal.
- **Python startup.** Every CLI invocation pays interpreter start-up
  (~30 ms), which dwarfs a single `put`.  The CLI is for inspection; the
  API is the supported path for bulk work.

## Files

- `benchmarks/bench.py` -- the main suite.
- `benchmarks/measure_batch.py` -- the batch-size sweep (writes
  `batch-curve-<stamp>.json`).
- `benchmarks/profile_reads.py`, `benchmarks/profile_writes.py` -- cProfile runs.
- `benchmarks/results/bench-20261024T123521.json` -- canonical run (v1.1).
- `benchmarks/results/bench-20261024T123518.json` -- repeat run.
- `benchmarks/results/bench-20261024T114414.json` -- v1.0 run quoted above.
- `benchmarks/results/batch-curve-20261024T142842.json` -- the sweep above.
- `benchmarks/results/profile-reads.txt`, `profile-writes.txt` -- raw cProfile output.
