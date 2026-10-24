# Security

## What this project is

minikv is a teaching implementation of an LSM-tree storage engine.  It is
**not intended to hold data you cannot afford to lose**, and it is not
hardened against a hostile local environment.

## Threats it does consider

- **Corrupted bytes must never be returned as data.**  Every WAL record,
  every segment data block, the segment index and the bloom filter carry a
  CRC32 that is verified before decoding.  Damage raises
  `CorruptRecordError` or truncates a torn WAL tail with a warning; it
  never produces a wrong value.  `tests/test_corruption.py` sweeps
  single-bit flips to prove it.
- **Deserialization.**  The on-disk format is explicit little-endian byte
  layouts.  Nothing on disk is ever unpickled, so a malicious file cannot
  execute code during recovery.
- **Two writers on one directory.**  Opening a data directory takes an
  exclusive lock; the second opener raises `StoreLockedError` instead of
  interleaving writes.

## Threats it does not consider

- **A malicious local user.**  Segments are not encrypted, not signed and
  not access-controlled beyond normal file permissions.  Anyone who can
  write to the data directory can destroy the store or make it refuse to
  open.
- **Untrusted input sizes.**  Keys and values are bounded (64 KiB and
  16 MiB) so a single operation cannot exhaust memory, but there is no
  total-size quota, no rate limiting and no protection against a caller
  writing data faster than the disk can absorb.
- **Side channels, network exposure, multi-tenancy.**  There is no
  network surface and no isolation between callers in one process.
- **Denial of service by resource exhaustion.**  A very large number of
  small segments makes reads slower; compaction is manual.

## Reporting

Open an issue describing the failure, the exact command, and the platform.
For a corruption bug, the most useful artifact is the smallest input that
reproduces it, for example a segment file plus an off-by-one description.

Please do not report "it is slower than SQLite" as a vulnerability; it is
documented in BENCHMARKS.md with the reasons.
