# Changelog

All notable changes to this project are documented here.  The format
follows Keep a Changelog.

## [1.0.0] - 2026-10-24

First release.  The milestone plan in the project brief (v0.1 through
v1.0) was implemented in one pass rather than as separately tagged
releases, so there are no v0.1-v0.4 tags: an intermediate tag would have
pointed at a tree whose tests could not run.  The history below describes
what the release contains, not a series of published versions.

### Added

- `Store` API: `put`, `get`, `delete`, `scan` (forward, reverse, limit),
  `batch` context manager, `compact`, `stats`, `close`, context manager.
- Write-ahead log with CRC32 per record, batch records, rotation per
  memtable generation, torn-tail detection and truncation.
- Three durability modes: `sync="always"`, `"interval"`, `"never"`.
- Immutable sorted segments (SSTables): ~4 KiB data blocks, sparse index,
  per-block CRC32, per-file bloom filter, 56-byte footer with a versioned
  magic, index and bloom verified at open.
- Full k-way compaction that drops tombstones, with deferred file
  deletion so snapshot readers stay valid.
- `MANIFEST` listing the live segment set, written atomically and
  checksummed; unlisted segments are removed on the next open.
- CLI: `get`, `put`, `del`, `scan`, `stats`, `compact`, `bench`.
- 86 tests, including 140 deterministic crash points, 8 external kills,
  single-byte damage sweeps and a 10 000-operation property test.
- Benchmarks with a SQLite baseline, and a cProfile-driven optimization
  of the point-read path (0.573 s to 0.081 s for 5 000 reads).

### Changed

- Point reads reuse a cached file handle per segment instead of opening
  the file on every `get()`.  The cost is one manifest fsync per flush,
  which made the write benchmark about 14% slower -- a deliberate trade,
  documented in DESIGN.md section 7.1 and 7.2.

### Known limitations

- One writer, one process; no inter-process lock file.
- Reverse scans materialise their result set.
- Compaction is stop-the-world for writers and is never scheduled
  automatically.
- Retired segment files keep a descriptor open until `close()`.
