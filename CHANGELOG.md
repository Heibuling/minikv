# Changelog

All notable changes to this project are documented here.  The format
follows Keep a Changelog.

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
