# Changelog

All notable changes to this project are documented here.  The format
follows Keep a Changelog.

## [1.3.0] - 2026-10-24

Additive release: point reads are 4x faster than 1.2, the test suite gained
a stateful model of the whole engine, and three measured changes are
recorded (including one that was reverted).

### Added

- **A hypothesis state machine** (`tests/test_properties.py`): random
  `put`/`delete`/`batch`/`flush`/`compact`/`reopen` plus two rules that
  inject a crash at a named flush or compaction step, all compared against
  a dict oracle after every step, with shrinking.  A guard test asserts the
  crash rules are actually selected.
- **`BlockCache`**: a bounded LRU of decoded, CRC-verified data blocks,
  shared by a store's segments, 8 MiB by default and switchable off with
  `block_cache_bytes=0` for cold-read measurement.
- Two benchmark scenarios that the cache can help: `read_hot` (a 200-key
  working set) and `scan_repeat` (the same range five times).

### Changed

- **Point reads are 4x faster**: 0.058 s to 0.015 s for 5 000 reads
  (9.1x to 2.3x slower than SQLite).  `get()` stops at the key instead of
  decoding the value of every record it walks past, and a warm block is
  answered by a bisect over a decoded tuple.  With the cache off the same
  build reads 0.046 s, so the cache is worth 3.1x on a full sweep.
- Compaction reclaims its retired inputs before returning, outside the
  writer lock: the merge is still 0.026 s, and the whole call is ~0.074 s
  because unlinking 76 files costs ~275 us each here.
- Point-read misses are 1.5x slower than SQLite instead of 1.1x, because
  every read now holds a snapshot lease (about 0.3 us).  Documented rather
  than hidden.
- 119 tests (was 117), coverage 90.2% (was 87.7%).

### Measured and rejected

- Routing the sequential `items()` iterator through the block cache made
  compaction 2.5x slower (0.0265 s to 0.069 s) by pushing every block of
  every input segment through the cache.  Reverted; the reason is recorded
  in the docstring and in BENCHMARKS.md.

## [1.2.0] - 2026-10-24

Additive release: it makes the crash-consistency claims testable at every
boundary, fixes the one structural compromise left in the compaction
design, and documents the on-disk format in one place.  Nothing was
removed.

### Added

- **`Store.snapshot()`** and reference-counted file reclamation.
  `get()` and `scan()` hold a short-lived lease, and a caller can hold one
  explicitly; a segment that compaction retires is unlinked and its handle
  closed the moment the last snapshot referencing it is released, instead
  of accumulating until `close()`.  A `StoreSnapshot` is a read-only view
  with `get`, `scan`, a context manager and `close()`.
- **`Store.flush()`**: force the current memtable to a segment on demand,
  which makes the WAL a bounded and known size before a backup.
- **Fault injection at every flush step** (`minikv/faults.py`).  Setting
  `MINIKV_FAULT` to `segment_tmp_written`, `segment_renamed`,
  `manifest_before`, `manifest_after`, `wal_before_delete`,
  `wal_after_delete`, `compaction_merged` or `compaction_manifest`
  aborts the process at that exact point.  Six new tests kill the process
  at each flush point and assert that every acknowledged write survives,
  that `verify()` is clean, and that the store still works afterwards.
- **`docs/format.md`**: every byte layout with offsets -- directory,
  WAL record, batch payload, segment record, data block, index block,
  bloom block, footer, manifest -- plus the write/flush ordering and a
  walkthrough of how a value is found.
- **`benchmarks/measure_batch.py`** now sweeps the batch size and writes a
  JSON curve.  Under `sync="always"`: 258.50 us/op at batch 1, 7.70 at
  batch 50 (33.6x), 2.03 at batch 500 (127x), against a 1.12 us/op floor
  with fsync off.

### Changed

- Restarted segments of a compaction are reclaimed as soon as they become
  unreachable, so a long-running process no longer accumulates descriptors
  and disk space between closes.
- `stats()` reports `live_snapshots` alongside `deferred_deletes`.
- 117 tests (was 108).

### Known limitations

- A `StoreSnapshot` that is never closed and never collected keeps its
  segments on disk, by design: reclamation follows the last lease.

## [1.1.0] - 2026-10-24

Correctness and measurement pass over the 1.0 engine.  No API was removed.

### Added

- **Directory lock.**  Opening a store takes an exclusive lock on a
  `LOCK` file, so a second `Store` (same process or another) raises the
  new `StoreLockedError` instead of interleaving WAL appends.  Previously
  this hazard was documented but not prevented.
- **Size limits.**  Keys are capped at 64 KiB and values at 16 MiB;
  exceeding either raises `ValueError` before anything is written.
- **`Store.verify()`** and the `minikv fsck` command: read back every
  block of every segment and every WAL record, and report all findings
  instead of raising on the first one.
- **Segment key ranges.**  Each index block records `(min_key, max_key)`,
  so a lookup rejects a segment with two byte comparisons and a scan skips
  non-overlapping segments without opening them.  (Format magic is now
  `MINIKV02`; there is no migration path for `MINIKV01` files.)
- **Streaming reverse scans.**  `scan(reverse=True)` no longer
  materialises the whole range; it decodes one block at a time and honours
  `limit` while streaming.
- **Compaction without the writer lock.**  The merge now runs outside the
  lock and only the splice at the end is serialised; segments flushed
  during a merge are kept and placed in front of the merged result.
- Hypothesis property tests for the WAL record encoding and the segment
  round-trip, and a soak test for repeated compaction.
- A coverage floor (85%) and `hypothesis` in CI.

### Changed

- **WAL replay streams in 64 KiB chunks** instead of reading the file
  whole, removing an unbounded-memory failure mode during recovery.  Cost:
  restart is ~12% slower (0.9 ms on the 5 000-record benchmark).
- Bloom filter hashing is plain arithmetic rather than a generator
  (157 168 generator resumes for 19 646 keys), and the WAL encoder builds
  its payload once instead of twice.  Sequential writes went from 0.092 s
  to 0.082 s and hit reads from 0.081 s to 0.058 s; miss reads went from
  0.042 s to 0.004 s thanks to the key ranges.
- Retired segments are deleted in `close()` and by the next `open()`,
  which now also removes any segment the manifest does not list.
- 108 tests (was 86); coverage 89% (was 81%).

### Measured and rejected

- Rolling the WAL checksum over three pieces instead of assembling the
  payload first tripled the `zlib.crc32` calls and was slower, so it was
  reverted.  Recorded in BENCHMARKS.md.

## [1.0.0] - 2026-10-24

First release.  The milestone plan in the project brief (v0.1 through
v1.0) was implemented in one pass rather than as separately tagged
releases, so there are no v0.1-v0.4 tags: an intermediate tag would have
pointed at a tree whose tests could not run.

### Added

- `Store` API: `put`, `get`, `delete`, `scan` (forward, reverse, limit),
  `batch` context manager, `compact`, `stats`, `close`, context manager.
- Write-ahead log with CRC32 per record, batch records, rotation per
  memtable generation, torn-tail detection and truncation.
- Three durability modes: `sync="always"`, `"interval"`, `"never"`.
- Immutable sorted segments: ~4 KiB data blocks, sparse index, per-block
  CRC32, per-file bloom filter, 56-byte footer with a versioned magic,
  index and bloom verified at open.
- Full k-way compaction that drops tombstones.
- `MANIFEST` listing the live segment set, written atomically and
  checksummed.
- CLI: `get`, `put`, `del`, `scan`, `stats`, `compact`, `bench`.
- 86 tests, including 140 deterministic crash points, 8 external kills,
  single-byte damage sweeps and a 10 000-operation property test.
- Benchmarks with a SQLite baseline, and a cProfile-driven optimization of
  the point-read path (0.573 s to 0.081 s for 5 000 reads).
