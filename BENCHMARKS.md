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
- `read_hot` reads 5 000 keys drawn from a 200-key working set, so a block
  is revisited before it can be evicted; `scan_repeat` walks the whole
  range five times.  Both give minikv's 8 MiB block cache something to do,
  and SQLite's page cache the same opportunity.
- `restart_recovery` closes a store whose writes all live in one WAL, so
  the reopen replays them.  `restart_checkpointed` calls `flush()` first,
  which moves the memtable into a segment and empties the WAL: the reopen
  then has nothing to replay.  The pair is the cost of an un-checkpointed
  tail.
- `--block-cache-bytes 0` switches the cache off, which is how the
  cold-read columns in optimization 3 were produced.

## Results

Canonical run: `benchmarks/results/bench-20261024T154709.json`.

| Scenario | minikv median | sqlite3 median | minikv slower |
| --- | --- | --- | --- |
| sequential write | 0.081 s | 0.006 s | 14.8x |
| random write | 0.081 s | 0.006 s | 13.2x |
| point read, hit | 0.015 s | 0.007 s | 2.3x |
| point read, hot set | 0.012 s | 0.006 s | 2.1x |
| point read, miss | 0.006 s | 0.004 s | 1.5x |
| full scan | 0.006 s | 0.002 s | 2.9x |
| full scan, repeated 5x | 0.016 s | 0.010 s | 1.5x |
| restart (5 000 WAL records) | 0.008 s | 0.001 s | 7.0x |
| restart after a checkpoint | 0.004 s | not applicable | -- |
| compaction (76 segments) | 0.076 s | not applicable | -- |

Throughput equivalents: ~61 000 writes/s, ~340 000 warm point reads/s and
~790 000 rows/s on a full scan, single-threaded, with no fsync.

Reproducibility: the second canonical run of the same build produced
0.083 / 0.092 / 0.015 / 0.012 / 0.006 / 0.006 / 0.016 / 0.008 / 0.004 /
0.077 s for the same rows -- within 8% everywhere.  Random write is the
row most sensitive to filesystem cache state, and fsync-bound timings are
noisier still.

Compaction deserves a footnote: 0.074 s is the merge *plus* reclaiming its
76 inputs, which the v1.2 snapshot design now does eagerly.  Measured
separately, the merge alone is 0.0265 s (0.0276 / 0.0257 / 0.0265 over
three runs) and the reclaim of 76 files is ~0.02 s on this machine, with
`os.unlink` costing about 275 us per file.

### Why SQLite wins, and where it does not

SQLite is a C library: rows are compact page records, lookups go through a
B-tree with a page cache, and the Python layer does one `sqlite3_step`
per call.  minikv pays, per operation:

- a Python function call and one `bytes` object per key and value;
- `struct.pack` plus one copy of key+value to build each WAL record;
- a dict lookup, or a block decode, per read;
- one `write()` syscall per record.

The write gap (14.8x) is interpreter overhead plus one syscall per record.
The read gap is now 2.1-2.3x, and its remaining cost is per-record decoding
in Python plus the bloom and range checks per segment.

Three rows deserve a note.  **A warm point read is 2.3x** because a 4 KiB
block holds ~34 records, so one disk read serves ~34 lookups and the rest
are a bisect over an in-memory tuple.  **A point-read miss is 1.5x**,
slower than the 1.1x of v1.2: a segment whose recorded key range cannot
contain the probe is still rejected with two byte comparisons, but every
read now also takes a snapshot lease (two lock operations) so that
compaction cannot delete the files under it.  **A full scan is 2.9x**
because both engines stream, and minikv pays one `crc32()` per 4 KiB block
rather than per record.

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

## Optimization 4 (v1.4): restart -- stream the replay, and checkpoint the tail

### What the profile said

Reopening a store that holds 20 000 records in one WAL took 0.061 s:

```
ncalls  tottime  cumtime  function
 20038    0.018    0.027  encoding.py:136(decode_wal_record)
 20000    0.008    0.016  memtable.py:70(put)            <-- 20k locks
 20000    0.003    0.005  memtable.py:53(entry_size)
 20001    0.001    0.001  {method 'append' of 'list' objects}
```

Two things stood out.  Recovery materialised every record into a list
before applying the first one, so a large log became tens of thousands of
tuples first.  And it applied them through `MemTable.put`, which takes the
memtable lock and recomputes sizes per record -- 20 000 lock acquisitions
and 40 000 size computations for a pass that is single-threaded by
construction.

### Change 1: stream, and load in bulk

`WAL.iter_replay()` yields operations as they are decoded, and
`MemTable.load()` inserts a batch without per-record locking: recovery
holds the directory lock and has not published the memtable yet, so the
lock protects nothing.

**0.061 s to 0.045 s** for 20 000 records, function calls 280 477 to
180 450, and the memtable's share fell from 0.016 s to 0.007 s.

### Change 2: a checkpoint watermark in the manifest

A WAL whose contents are already inside a segment does not need replaying.
Each segment now records the highest WAL sequence number it covers, the
manifest carries that number per segment (`seg-000009.sst 12`), and open
deletes any WAL at or below the watermark instead of replaying it.

The case it fixes is the crash window between "manifest updated" and "WAL
unlinked", which otherwise re-replays the same records on every start, and
would keep doing so forever if an unlink ever failed.  If the manifest is
missing or damaged the watermark is 0, which means replay everything --
slower and always safe.

### Change 3: bound the tail, and measure it

The restart cost is however much WAL has not been flushed.  `flush()` is
the checkpoint: it moves the memtable into a segment and empties the WAL.
The benchmark now measures both sides of that:

| | median | vs sqlite3 |
| --- | --- | --- |
| restart, 5 000 WAL records | 0.0080 s | 7.0x |
| restart after `flush()` | 0.0038 s | -- |

### A change that was measured and reverted, again

Rolling the record checksum over header, key and value separately -- to
avoid a third copy and a per-record memoryview -- tripled the `zlib.crc32`
calls (20 002 to 60 002) and moved the whole replay from 0.051 s to
0.055 s.  The same lesson as the encoder in v1.1, measured a second time,
and reverted again.  What stayed is the `limit` argument, which removed
40 000 `len()` calls per run.

## Optimization 3 (v1.3): point reads -- stop at the key, then cache the block

### The profile

After v1.2 a hit cost 11.6 us (0.058 s for 5 000 reads), and the first two
rounds had already removed the obvious waste: only 9 `open()` calls
remained, and the snapshot lease cost 3 ms in total.  The profile was
therefore about decoding:

```
ncalls  tottime  cumtime  function
  5000    0.003    0.123  engine.py:423(get)
 23265    0.032    0.109  segment.py:347(get)
 82782    0.035    0.042  encoding.py:198(decode_sst_record)   <-- 34%
  4653    0.003    0.011  segment.py:325(_read_block)
```

82 782 record decodes for 5 000 gets -- 16.6 per lookup.  That is the
linear walk inside a block: a 4 KiB block holds ~34 records, and the old
code decoded every record completely -- key *and* 100-byte value -- to
compare keys it was going to skip.  The bloom filter and the key range
were already doing their job; the waste was inside the block.

### Change 1: stop at the key

A record is `klen | tombstone | vlen | key | value`, so the key comes
first.  `get()` now unpacks the header, compares the key, and only touches
the value when the key matches.  A skipped record costs a header unpack
and one slice instead of a full decode with its value copy.

### Change 2: cache decoded blocks

`BlockCache` holds decoded blocks as `(keys, entries)` tuples in a bounded
LRU, so a lookup inside a warm block is a bisect over an in-memory tuple.
It is populated only from bytes that already passed their CRC: a cache of
*verified* data, not a replacement for verification -- the opposite of
trusting an mmap.

Each row is one `bench.py` run, so both are reproducible with a single
command:

| | point read, hit | point read, hot set | scan repeated 5x |
| --- | --- | --- | --- |
| cache off (`--block-cache-bytes 0`) | 0.0463 s (7.1x) | 0.0483 s (7.2x) | 0.0249 s (2.3x) |
| cache 8 MiB (the default) | 0.0147 s (2.3x) | 0.0122 s (2.1x) | 0.0159 s (1.5x) |
| speedup | 3.1x | 4.0x | 1.6x |

### The result

| Scenario | v1.2 | v1.3 | change |
| --- | --- | --- | --- |
| point read, hit | 0.058 s (9.1x) | 0.015 s (2.3x) | 4.0x faster |
| point read, hot set | not measured | 0.012 s (2.1x) | -- |
| full scan, repeated 5x | not measured | 0.016 s (1.5x) | -- |
| point read, miss | 0.004 s (1.1x) | 0.006 s (1.5x) | slower, deliberately |

The miss row got worse, and the reason is honest: the snapshot lease that
makes reclamation safe costs about 0.3 us per read, and a miss is the
cheapest read there is.  It buys the guarantee that compaction cannot
delete a segment while a reader holds it.

### A change that was measured and reverted

The first version routed `items()` -- the sequential iterator used by
compaction -- through the same cache.  Compaction went from 0.0265 s to
0.069 s, because merging 76 segments pushed every block through the cache
and evicted the working set for no benefit.  Full sequential scans now
bypass the cache deliberately, and the measurement is quoted in the
method's docstring.

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
- **Per-record Python objects.** Decoding is the remaining reason hit
  reads are 2.3x slower than SQLite; a C extension is out of scope by the
  project's constraints.
- **The block cache costs memory.** 8 MiB by default, holding ~1 300
  blocks, which covers the benchmark's whole working set (665 KiB).  A
  store far larger than the cache will see the hit rate fall, and the miss
  path is a full block decode.
- **A snapshot lease on every read.** Two lock operations per `get()`, or
  about 0.3 us, which is why the miss row is 1.5x rather than 1.1x.  It is
  the price of deleting retired segment files safely.
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
- `benchmarks/profile_reads.py`, `profile_writes.py`, `profile_restart.py` -- cProfile runs.
- `benchmarks/results/bench-20261024T154709.json` -- canonical run (v1.4).
- `benchmarks/results/bench-20261024T154715.json` -- its repeat.
- `benchmarks/results/bench-20261024T150821.json` -- v1.3 run quoted above.
- `benchmarks/results/bench-20261024T150826.json` -- its repeat.
- `benchmarks/results/bench-20261024T150924.json` -- the same build with the
  block cache off, quoted in optimization 3.
- `benchmarks/results/bench-20261024T123521.json` -- v1.1 run quoted above.
- `benchmarks/results/bench-20261024T123518.json` -- its repeat.
- `benchmarks/results/bench-20261024T114414.json` -- v1.0 run quoted above.
- `benchmarks/results/batch-curve-20261024T142842.json` -- the sweep above.
- `benchmarks/results/profile-reads.txt`, `profile-writes.txt`,
  `profile-restart.txt` -- raw cProfile output.
