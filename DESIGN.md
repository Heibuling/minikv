# Design

This document explains the choices behind minikv, including the ones that
make it slower than SQLite. Every number quoted here comes from
BENCHMARKS.md or from the measurement notes inline.

## 1. Scope

minikv is a teaching-sized storage engine: one process, one writer,
byte keys and byte values, an LSM tree with full compaction, and a
durability policy the caller selects. It is not a database: there is no
transactions layer, no query language, no secondary index, no TTL, no
replication.

The intended reader is someone who wants to see a write-ahead log, an
SSTable, a bloom filter and a compaction pass implemented end to end and
proved by tests.

## 2. Why an LSM tree and not a B-tree

A B-tree updates pages in place, which means a write touches a random
page and, for durability, must journal that page before and after. An LSM
tree turns every write into an append and pays for it later during
compaction. For a Python implementation the append-only shape is the
decisive advantage:

- the write path is a `write()` to one file plus a dict insert, with no
  page splitting, no rebalancing and no in-place mutation;
- an on-disk segment is immutable once written, so a reader can hold a
  reference to it without any locking and without any risk that the bytes
  change underneath;
- recovery is "replay the log", not "check the page checksums and repair
  the tree".

The cost is read amplification: a point lookup may have to consult the
memory table plus several segments, and the engine must periodically
rewrite data to keep that number bounded. Section 7.4 quantifies it.

A B-tree would give better point-read complexity and lower write
amplification, at the price of much more intricate crash-safe update
logic in a language without control over memory layout. That trade is
the wrong one for this project; for a real database it is often the right
one, which is why SQLite wins the benchmarks.

## 3. On-disk format

### 3.1 Write-ahead log

One WAL file per memtable generation: `wal-000001.log`, `wal-000002.log`,
and so on. A record is:

    crc32(4) | type(1) | klen(4) | vlen(4) | key | value        (little-endian)

Type 1 is put, 2 is delete, 3 is a batch whose value field contains a
sequence of `type|klen|vlen|key|value` entries. One record carries one
CRC, so replay applies a batch completely or not at all.

The CRC covers everything after the CRC field. Replay walks records until
the buffer ends or a record fails its checksum, then truncates the file
to the last verified byte and emits one warning. A crash during
`write()` can therefore only lose the record being written.

Why a separate file per generation: truncating the WAL in place after a
flush would also drop writes that arrived while the flush was running.
Rotating is a rename-free, O(1) operation, and deleting an old file is
safe once the segment that contains its contents has been made durable.

### 3.2 Segment

    [data block 0][data block 1] ... [sparse index][bloom][footer 56B]

A data block is a run of `klen(4) | tombstone(1) | vlen(4) | key | value`
records, closed at roughly 4 KiB. A block's CRC32 is not stored inline;
it lives in the sparse-index entry that points at it:

    first_key | offset(8) | length(4) | crc(4)

That lets the writer emit bytes once, without seeking back to patch a
header, and it lets the reader verify a block with one `crc32()` call
before it decodes anything.

The footer carries the index offset and length, the bloom offset and
length, the CRC of each of those two blocks, the entry count and an
eight-byte magic that encodes the format version. Opening a segment
verifies the magic, the index CRC and the bloom CRC; data blocks are
verified lazily by the read that touches them.

That split is deliberate. A full scan of every block at open would make
startup O(file size), which is exactly the cost the manifest and the
sparse index exist to avoid. The guarantee that matters -- "never hand
back bytes that failed a checksum" -- holds either way, because a block
is only decoded after its CRC passes.

### 3.3 Manifest

`MANIFEST` lists the live segment files, newest first, with a trailing
CRC32 over its own text:

    minikv-manifest-v1
    seg-000009.sst
    seg-000007.sst
    crc32 1f0c3a9d

It is written to `.tmp`, fsynced, renamed, and the directory is fsynced
where the OS supports it. Recovery uses it to decide which files are
live: anything ending in `.sst` that the manifest does not list is
deleted. Without that rule a crash between "segment renamed into place"
and "manifest updated" would leave an orphan that never goes away.

### 3.4 Recovery order

1. Read `MANIFEST` (rebuild from the directory and warn if it is
   missing or its checksum fails).
2. Delete unlisted `seg-*.sst` files and `*.tmp` leftovers.
3. Open every live segment: verify magic, index CRC, bloom CRC.
4. Replay `wal-*.log` in ascending order, truncating a damaged tail.
5. Reuse the newest WAL for new appends; older WALs are deleted after the
   next successful flush.

## 4. fsync semantics and cost

`sync="always"` fsyncs the WAL descriptor before `put()` returns. On the
benchmark machine an fsync costs roughly 0.25 ms: a single put measures
250.5 us per operation, while fifty puts sharing one fsync measure 7.6 us
each (BENCHMARKS.md). That is why the throughput benchmarks run with
`sync="never"` -- they measure the engine, not the disk.

The three modes are not cosmetic:

| Mode | syscall per write | Survives process kill | Survives power loss |
| --- | --- | --- | --- |
| always | write + fsync | yes | yes |
| interval | write | yes | up to one interval |
| never | write | yes | no promise |

A process kill is safe even in `never` mode because the bytes have left
Python and reached the kernel; only a machine crash can lose them.
The 140-point crash suite runs with `always` precisely because that is
the mode with a claim worth testing.

One acknowledged write costs one fsync. `batch()` is how a single writer
avoids that: fifty operations become one WAL record, one CRC and one
fsync, worth 33x per operation in the measurement above. Group commit
across *concurrent* writers would help a workload this API deliberately
does not offer, because there is only ever one writer.

## 5. Crash consistency model

The invariants, the fault model and the injection method live in
docs/crash-consistency.md. The one-paragraph version:

- A write is acknowledged only after its WAL record is on stable storage
  (mode-dependent) *and* in the memtable.
- A segment becomes visible only after its bytes, its rename and the
  manifest update are durable, in that order.
- A WAL file is deleted only after a segment containing its contents is
  durable and listed in the manifest.
- Deleted segment files are removed only at `close()` (or by the next
  open), never while a snapshot might still read them.

The failure that is *not* defended against is corruption of a length
field in the middle of the WAL. A damaged length and a torn append look
identical to a reader, so both stop replay at that point and truncate.
The engine still never returns wrong data; it may lose records after the
damage, which is why corruption is reported on `stats()` and in a
warning.

Every step of the flush protocol is independently injectable.  Setting
`MINIKV_FAULT` to a step name aborts the process at that exact point, and
the crash suite asserts that each acknowledged write survives, that the
store reopens, that `verify()` is clean and that writes still work
afterwards.  That is what turns the ordering above from a claim into
evidence -- see docs/crash-consistency.md.

## 6. Concurrency model

Single writer, many readers.

- Every mutation takes one `RLock`. This includes `put`, `delete`, batch
  commit and flush bookkeeping. Compaction takes it only to capture the
  segment list and to splice the result back in; the merge itself runs
  outside it, so writers are not stalled for the duration of a merge.
- Readers take no lock. They load the `_snapshot` attribute -- an
  immutable `(memtable, segments)` tuple -- and work from it.
- The memtable is only ever appended to or overwritten in place while it
  is the current one. CPython makes a single dict get atomic under the
  GIL, so a reader sees either the old or the new value for a key, never
  a mix.
- When a flush starts, the writer swaps in a fresh memtable and publishes
  a snapshot containing the frozen one. A reader that grabbed the old
  snapshot keeps iterating the old memtable and the old segment list.
- Segments are immutable, so a snapshot's segment list stays valid.  A
  segment that compaction retires stays on disk, and readable, until no
  snapshot can reach it; then its file is unlinked and its handle closed.
  Every read holds a lease for the duration of one `get()` or one scan,
  and `Store.snapshot()` hands that lease to a caller whose read spans
  many operations.

The visible consequence: read-your-writes holds within a thread because
the memtable is shared; a reader in another thread may miss a write that
happens after it took its snapshot, but it can never observe a partial
one.

Why not aim higher: a lock-free memtable or a reader-writer lock would
buy concurrent writers, which the API explicitly does not offer
(single-writer is a stated constraint), and copy-on-write snapshots per
read would cost more than the memtable it protects.

## 7. Trade-offs

### 7.1 One manifest fsync per flush: +16% write cost, crash-safe deletion

Adding the manifest made the write benchmark slower:

| | before manifest | after manifest |
| --- | --- | --- |
| sequential write, 5 000 ops | 0.0804 s | 0.0920 s |
| random write, 5 000 ops | 0.0830 s | 0.0902 s |

With a 64 KiB memtable the workload performs about eight flushes, so the
regression is roughly 1.5 ms per flush: one file write, one fsync, one
rename. In exchange, the live segment set survives a crash, orphaned
segments are reclaimed on the next open, and compaction can delete files
without guessing. Given that the alternative is "a crash silently
resurrects deleted data or leaks files forever", 16% on writes is the
right price.

### 7.2 Cached segment handles: 6.8x on point reads, deletion deferred

The first version opened the segment file on every `get()`. cProfile on
5 000 reads showed `_io.open` at 0.457 s of 0.738 s total (62%), because
opening a file on this Windows machine costs about 90 us.

Keeping one handle per segment changed the read benchmark from 0.573 s to
0.081 s for 5 000 point reads -- about 7x -- and dropped the gap to SQLite
from 76x to 12.6x.  A later change took the same row to 0.058 s by
recording each segment's key range, so a segment that cannot contain the
key is rejected before its bloom filter is consulted.

The cost is lifecycle complexity: on Windows a file with an open handle
cannot be deleted, so `compact()` cannot unlink its inputs.  They are
retired instead, and reclaimed the moment the last snapshot referencing
them is released -- which in the common case is immediately, because the
store itself is the only holder of the *current* view.  A long reader
takes a lease: implicitly around every `get()` and scan, or explicitly
with `Store.snapshot()` when the read spans many calls.

### 7.3 Sparse index and 4 KiB blocks: 0.6% index overhead

Measured on 5 000 keys of 100-byte values: 9 segments, 4 653 entries,
135 blocks, 34.5 entries per block, 569 KiB of segment bytes.

With one index entry per block, the index is about 3.2 KiB -- roughly
0.6% of the data -- against roughly 60 KiB if every key were indexed.
A lookup reads at most one 4 KiB block; the bloom filter rejects most
segments without any I/O at all.

The index header also stores the segment's (min_key, max_key).  Two byte
comparisons then reject a segment before the bloom filter runs, which is
why a point-read miss costs 0.004 s for 5 000 probes against 0.042 s
before the header was added -- and why the miss row now matches SQLite
(1.1x).

The trade is that a lookup may decode up to 34 records linearly instead
of doing one binary search over all keys. At these sizes the difference
is a few microseconds; at millions of keys per segment it would be the
wrong choice, and a denser index or a restart point array would be
needed.

### 7.4 Full compaction instead of leveled compaction

`compact()` merges *every* segment into one. That makes tombstone
dropping trivially correct -- after the merge nothing older exists for a
delete to shadow -- and keeps the read path at O(number of segments)
before compaction.

The price is write amplification and disk headroom. On the benchmark
workload 76 small segments (about 570 KiB of live data) are rewritten
into one file in 0.026 s -- every byte read once and written once per
compaction -- and, since v1.2, `compact()` also reclaims its inputs
eagerly when no snapshot holds them, which brings the whole call to about
0.074 s.  The unlink itself costs ~275 us per file here; it happens outside
the writer lock, so it delays the caller but not other writers. A leveled design rewrites less per compaction but must
track overlapping key ranges, and tombstones can only be dropped at the
bottom level.

For a 5 000-key teaching workload full compaction is simpler and fast
enough. For a store that compacts often while the working set is large,
it is the wrong design, which is why "leveled compaction" is the first
item under future work.

### 7.5 One process, enforced rather than requested

The 1.0 engine documented "do not open a directory twice" and did nothing
about it: two writers would interleave WAL appends and lose data with no
error. The store now takes an exclusive lock on a `LOCK` file (flock on
POSIX, msvcrt byte-range locking on Windows) and the second opener raises
`StoreLockedError`.

The cost is one extra file in the directory and one open plus one lock per
`Store`, not per operation. In the same pass keys and values were bounded
-- 64 KiB and 16 MiB -- so a single call cannot exhaust memory before the
format rejects it; both limits raise `ValueError` before anything is
written, and the documented boundaries are accepted.

### 7.6 A block cache, and stopping at the key

Point reads were 9.1x slower than SQLite after v1.2, and the profile put
34% of the remaining time in `decode_sst_record`: 82 782 decodes for 5 000
gets, 16.6 per lookup.  A 4 KiB block holds ~34 records, and the old code
decoded every one of them completely -- key *and* value -- to compare keys
it was about to skip.

Two changes, measured separately:

- `get()` now unpacks the record header, compares the key, and touches the
  value only on a match.  A skipped record costs a header unpack and one
  slice.  This needs no memory and no state.
- `BlockCache` keeps decoded blocks as `(keys, entries)` tuples in a
  bounded LRU (8 MiB by default), so a lookup inside a warm block is a
  bisect over an in-memory tuple.  Cache entries are built only from bytes
  that already passed their CRC: it caches *verified* data rather than
  replacing verification, which is the opposite of the mmap trade
  criticised in section 9.

Warm point reads went from 0.058 s to 0.015 s for 5 000 (2.3x SQLite), and
a hot working set to 0.012 s.  With the cache off the same build reads
0.046 s, so the cache is worth 3.1x on a full sweep and 4.0x on a hot set.

The cost is memory, and one measurement that changed the design: routing
the sequential `items()` iterator through the cache made compaction 2.5x
slower (0.0265 s to 0.069 s), because merging 76 segments pushed every
block through the cache and evicted the working set for no benefit.  Full
sequential scans now bypass it deliberately.

### 7.7 A checkpoint watermark, so replay does not repeat itself

Recovery has to replay whatever has not been flushed.  Two changes reduce
that work without weakening anything:

- A segment records the highest WAL sequence number its contents came
  from, and the manifest carries it per segment (`seg-000009.sst 12`).  A
  WAL at or below the watermark is deleted rather than replayed.  In steady
  state a WAL is unlinked the moment its segment is durable, so this only
  covers the crash window between those two steps -- but without it that
  window re-replays the same records on every start, and would keep doing
  so for as long as an unlink kept failing.
- If the manifest is missing or damaged, the watermark is 0, which means
  replay everything.  The failure mode is a slow start, never a missing
  record.

The remaining cost is inherent: a store whose memtable never reached the
threshold has its whole WAL to replay.  `flush()` is the checkpoint that
bounds it.  Streaming replay and a bulk memtable load (no per-record lock,
no materialised list of every record) took 20 000 records from 0.061 s to
0.045 s, and reopening after a checkpoint costs 0.0038 s.

## 8. Known limitations

- **Compaction rewrites everything.** It no longer holds the writer lock
  for its duration, but it still reads and writes every live byte, and the
  retired files stay on disk until `close()`.
- **No group commit for concurrent writers.** `batch()` covers the single
  writer this API offers; a multi-writer design would need more.
- **A leaked snapshot holds files open.** Reclamation depends on the last
  lease being released: a `StoreSnapshot` that is neither closed nor
  collected keeps its segments on disk and their handles open.
- **The block cache costs 8 MiB by default** and is per store.  A working
  set larger than the cache falls back to a full block decode, and the
  cache is not shared between stores.
- **A miss pays for the snapshot lease.** Two lock operations per read,
  about 0.3 us, which is why point-read misses are 1.5x rather than 1.1x.
  It buys the guarantee that compaction cannot unlink a file under a
  reader.
- **Reverse scans decode a block forward and then walk it backwards.** They
  stream and honour `limit`, but they are not the cheapest traversal.
- **Recovery replays the un-flushed tail.** Streaming removed the
  bounded-memory problem, and the checkpoint removes the redundant replay,
  but a store that never flushed still has its whole WAL to read: 5 000
  records cost 0.008 s against SQLite's 0.001 s.  Only `flush()` reduces
  that, which is why it is documented as the checkpoint.
- **MANIFEST v1 is not read.** The format moved to v2 when WAL coverage was
  added; a v1 manifest is treated as damaged, rebuilt from the directory
  and rewritten, which replays every WAL once.  Started, not silently
  wrong.
- **No checksums on the manifest's meaning**, only on its bytes: a
  manifest that is valid but lists the wrong generation cannot be
  detected. It is written only after the segments it lists are durable.
- **Format changes are not migratable.** `SST_MAGIC` moved to
  `MINIKV02` for the key-range header; `MINIKV01` files are not read.
- **Windows cannot fsync a directory**, so a rename is only as durable as
  NTFS itself. `stats()` reports `fsync_dir_supported: False` there.

## 9. Prior art and attribution

- The LSM-tree idea: P. O'Neil, E. Cheng, D. Gawlick, E. O'Neil,
  "The Log-Structured Merge-Tree (LSM-Tree)", Acta Informatica, 1996.
- The table layout (data blocks, sparse index, footer) follows the
  public LevelDB table-format document; no LevelDB source was copied.
- The write-ahead-log framing with a per-record CRC and a truncate-at-the
  -tail recovery rule is the classic design also used by the small
  MIT-licensed Python engines I read while researching this project
  (fgmacedo/soonerdb and Dragul17/py_kv). None of their code is included:
  the encoder, the segment format (blocks with per-block CRC and a
  checksummed index), the manifest and the concurrency model here are
  different, and py_kv in particular stores no CRC over segment data at
  all -- a gap that this project exists to close.
- The three durability modes are modelled on SQLite's
  `synchronous=OFF/NORMAL/FULL` semantics, which is also the baseline
  this project benchmarks against.
- "Are You Sure You Want to Use MMAP in Your Database Management System?"
  (Crotty, Leis, Pavlo, CIDR 2022) is why this engine reads blocks with
  `read()` and validates them, instead of mapping files and trusting the
  page cache.
