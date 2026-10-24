# Benchmarks

Every number here was produced on the machine below, by the command
below, and the raw JSON is committed under benchmarks/results/.

## Machine

| | |
| --- | --- |
| CPU | Intel Core Ultra 9 275HX (24 logical cores) |
| RAM | 31.4 GiB |
| Disk | UMIS RPJYJ1T24MML1AWY SSD (C:, 192 GiB free) |
| OS | Windows 11 (win32), CPython 3.12.14 |
| SQLite | 3.45+ as shipped with CPython 3.12 (in-process) |

## Method

```
python benchmarks/bench.py --ops 5000 --runs 3
```

- 5 000 operations per scenario, 100-byte values, keys `key00000000`...
- Each scenario runs three times in a fresh directory; the table reports
  the median and the spread `(max - min) / median`.
- Random orders use a fixed seed (1234 + run index), so runs are
  comparable.
- minikv runs with `sync="never"` and a 64 KiB memtable: the goal is to
  measure the engine, not the disk. SQLite runs with
  `journal_mode=OFF` and `synchronous=OFF`, which is the matching
  "no fsync" setting.
- "Restart" closes a store holding 5 000 WAL records and times the
  reopen (plus one verified read). For SQLite it is `connect()` plus one
  indexed read on the same 5 000 rows.
- "Compaction" has no SQLite equivalent and is reported for minikv only.

## Results

Canonical run: `benchmarks/results/bench-20261024T114907.json`.

| Scenario | minikv median | spread | sqlite3 median | spread | minikv slower |
| --- | --- | --- | --- | --- | --- |
| sequential write | 0.0920 s | 4.9% | 0.0055 s | 5.7% | 16.9x |
| random write | 0.0902 s | 3.1% | 0.0061 s | 6.2% | 14.8x |
| point read, hit | 0.0806 s | 8.9% | 0.0064 s | 9.4% | 12.6x |
| point read, miss | 0.0421 s | 1.7% | 0.0041 s | 3.1% | 10.3x |
| full scan | 0.0053 s | 4.0% | 0.0021 s | 2.4% | 2.5x |
| restart (5 000 WAL records) | 0.0073 s | 1.5% | 0.0010 s | 4.4% | 7.3x |
| compaction (72 segments) | 0.0335 s | 38% | not applicable | -- | -- |

Throughput equivalents: about 54 000 writes/s, 62 000 point reads/s and
940 000 rows/s for a full scan, single-threaded, `sync="never"`.

Reproducibility: the immediately preceding run
(`bench-20261024T114858.json`) produced 0.0929 / 0.0966 / 0.0790 /
0.0416 / 0.0056 / 0.0073 / 0.0341 s for the same rows -- every scenario
within 7% of the table above, well inside the 20% target.  Compaction is
the noisiest row (38% spread within a run); it is dominated by filesystem
metadata work on 72 small files.

### Why SQLite wins, and where it does not

SQLite is a C library: each row is a compact page record, lookups go
through a B-tree with a page cache, and the Python layer does one
`sqlite3_step` per call. minikv pays, per operation:

- a Python function call and a `bytes` object per key and value;
- `struct.pack`/concatenation to build each record (three temporary
  byte strings per WAL record);
- a `dict` lookup or a block decode per read;
- for reads, a bloom filter check per segment (9 segments here).

The 16.9x write gap is mostly interpreter overhead and one `write()`
syscall per record; the 12.6x read gap is mostly per-record decoding in
Python versus SQLite's compiled page access.

The one row where the gap narrows is a full scan (2.5x): both engines
stream records, and minikv's per-block CRC costs a single `crc32()` over
4 KiB rather than per record.

## A real profile, one change, and the result

### The problem

The first benchmark showed point reads at 0.573 s for 5 000 operations,
76x slower than SQLite -- far worse than the write gap, which the same
engine's design would predict.

### The profile

`python benchmarks/profile_reads.py` (cProfile, 5 000 gets over 9
segments):

```
ncalls  tottime  cumtime  function
     1    0.001    0.738  workload()
  5000    0.010    0.737  engine.py:260(get)
 23265    0.038    0.723  segment.py:272(get)
  4838    0.457    0.457  {built-in method _io.open}     <-- 62%
 23265    0.016    0.068  segment.py:99(might_contain)
  4838    0.058    0.058  {method '__exit__' of '_io._IOBase'}
  4838    0.005    0.053  segment.py:257(_read_block)
```

4 838 `open()` calls for 5 000 reads: every `get()` reopened the segment
file. On this machine a `with open(path, "rb")` costs about 94 us, and
the actual block read costs 5 ms in total. The bloom filter was doing its
job -- only ~1 segment per read was opened -- but the open itself was the
cost.

### The change

`SegmentReader` now keeps one file handle open (guarded by a per-segment
lock) and reuses it for `get()`. Two consequences had to be handled:

1. On Windows a file with an open handle cannot be deleted, so
   `compact()` no longer unlinks its inputs. They are retired and deleted
   in `close()`.
2. Retired files must not be resurrected by a restart, so the live set is
   now recorded in a checksummed `MANIFEST`, and anything not listed is
   deleted at open.

The manifest costs one fsync per flush, which is why the write row got
slower at the same time.

### The result

| Scenario | before | after | change |
| --- | --- | --- | --- |
| point read, hit | 0.5730 s | 0.0806 s | 7.1x faster |
| point read, miss | 0.0903 s | 0.0421 s | 2.1x faster |
| full scan | 0.0075 s | 0.0053 s | 1.4x faster |
| sequential write | 0.0804 s | 0.0920 s | 14% slower |
| random write | 0.0830 s | 0.0902 s | 9% slower |
| restart | 0.0065 s | 0.0073 s | 12% slower |

The write and restart regressions are the manifest's fsync and the extra
CRC check at open. They were accepted deliberately: the read path is the
one users hit most, and the manifest is what makes deferred deletion and
orphan cleanup safe. DESIGN.md section 7.1 and 7.2 discuss the trade.

## Known bottlenecks, and what is deliberately not optimized

- **fsync latency.** In `sync="always"` a write costs one fsync. Group
  commit (coalescing the fsyncs of concurrent writers) would help, but
  the single-writer lock means there is rarely a second writer waiting.
  Not implemented.
- **Record encoding.** Each WAL record allocates three temporary byte
  strings. A preallocated `bytearray` and `memoryview` would cut
  allocations, but no profile has shown allocation to be the bottleneck
  yet; it is on the list rather than in the code.
- **Per-segment bloom checks.** With 9 segments a miss costs 9 bloom
  checks (about 9 us in total). A fused filter or a segment key range
  would remove most of that; at this scale it does not matter.
- **Compaction is O(live data).** It rewrites everything. A leveled
  design rewrites less per pass. Out of scope for v1.0.
- **Reverse scans materialise.** A reverse scan of a large range builds a
  list; forward scans stream. Documented as a limitation rather than
  hidden.
- **Python startup.** Every CLI invocation pays interpreter start-up
  (~30 ms), which dwarfs a single `put`. The CLI is for inspection, not
  for bulk loading; the API is the supported path.

## Files

- `benchmarks/bench.py` -- the benchmark described above.
- `benchmarks/profile_reads.py` -- the cProfile run.
- `benchmarks/results/bench-20261024T114907.json` -- canonical run.
- `benchmarks/results/bench-20261024T114858.json` -- repeat run.
- `benchmarks/results/bench-20261024T114414.json` -- the pre-optimization
  run quoted in the before/after table.
- `benchmarks/results/profile-reads.txt` -- raw cProfile output.
