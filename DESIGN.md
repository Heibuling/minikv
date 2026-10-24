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
benchmark machine an fsync costs roughly 0.1-1 ms depending on the drive
and on whether the filesystem is busy, which is why the write benchmarks
run with `sync="never"`: they measure the engine, not the disk.

The three modes are not cosmetic:

| Mode | syscall per write | Survives process kill | Survives power loss |
| --- | --- | --- | --- |
| always | write + fsync | yes | yes |
| interval | write | yes | up to one interval |
| never | write | yes | no promise |

A process kill is safe even in `never` mode because the bytes have left
Python and reached the kernel; only a machine crash can lose them.
The 128-point crash suite runs with `always` precisely because that is
the mode with a claim worth testing.

One acknowledged write costs one fsync. Group commit -- batching the
fsyncs of concurrent writers -- would be the obvious next optimization;
it is listed as future work rather than implemented, because the
single-writer lock already serialises the calls.

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

## 6. Concurrency model

Single writer, many readers.

- Every mutation takes one `RLock`. This includes `put`, `delete`,
  batch commit, flush bookkeeping and compaction.
- Readers take no lock. They load the `_snapshot` attribute -- an
  immutable `(memtable, segments)` tuple -- and work from it.
- The memtable is only ever appended to or overwritten in place while it
  is the current one. CPython makes a single dict get atomic under the
  GIL, so a reader sees either the old or the new value for a key, never
  a mix.
- When a flush starts, the writer swaps in a fresh memtable and publishes
  a snapshot containing the frozen one. A reader that grabbed the old
  snapshot keeps iterating the old memtable and the old segment list.
- Segments are immutable, so a snapshot's segment list stays valid. Their
  files stay on disk until `close()` for exactly that reason.

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
0.081 s for 5 000 point reads -- about 7x -- and dropped the gap to
SQLite from 76x to 12.6x.

The cost is lifecycle complexity: on Windows a file with an open handle
cannot be deleted, so `compact()` cannot unlink its inputs. They are
retired into a list, stay readable for snapshots in flight, and are
deleted in `close()`. A long-lived process that compacts repeatedly
therefore holds one descriptor per retired segment until it closes; this
is documented as a limitation rather than fixed with reference counting,
which would require every snapshot iteration to register itself.

### 7.3 Sparse index and 4 KiB blocks: 0.6% index overhead

Measured on 5 000 keys of 100-byte values: 9 segments, 4 653 entries,
135 blocks, 34.5 entries per block, 569 KiB of segment bytes.

With one index entry per block, the index is about 3.2 KiB -- roughly
0.6% of the data -- against roughly 60 KiB if every key were indexed.
A lookup reads at most one 4 KiB block; the bloom filter rejects most
segments without any I/O at all.

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
workload 70 small segments (about 570 KiB of live data) are rewritten
into one file in 0.039 s, i.e. every byte is read once and written once
per compaction. A leveled design rewrites less per compaction but must
track overlapping key ranges, and tombstones can only be dropped at the
bottom level.

For a 5 000-key teaching workload full compaction is simpler and fast
enough. For a store that compacts often while the working set is large,
it is the wrong design, which is why "leveled compaction" is the first
item under future work.

## 8. Known limitations

- **Writers are serialised during compaction.** Compaction holds the
  writer lock for its whole duration; readers are unaffected because they
  never take it. At 570 KiB this is 40 ms; at gigabytes it would be
  unacceptable.
- **Reverse scans materialise.** `scan(reverse=True)` builds a list of
  the matching range and reverses it. Forward scans are streaming.
- **No group commit.** Each `sync="always"` write pays its own fsync.
- **One process.** There is no lock file; opening the same directory from
  two processes will corrupt it.
- **Retired segments hold descriptors** until `close()`.
- **No checksums on the manifest's meaning**, only on its bytes: a
  manifest that is valid but lists the wrong generation cannot be
  detected. It is written only after the segments it lists are durable.

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
