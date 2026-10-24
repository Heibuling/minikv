# Crash consistency

What minikv promises when the process dies, what it promises when the disk
is damaged, how those promises are tested, and where they stop.

## 1. Fault model

Modelled:

| Fault | How it is injected | Expected outcome |
| --- | --- | --- |
| process killed after an acknowledged write | `os._exit(7)` inside the child, immediately after `put()` returns | every acknowledged write is present after reopen |
| process killed at an arbitrary moment | `Popen.kill()` (TerminateProcess / SIGKILL) at a random delay | the store reopens; surviving keys form a prefix of the write sequence |
| torn WAL append | append 1-3 junk bytes to a WAL, or truncate it mid-record | replay stops at the last verified record, the file is truncated, one `RuntimeWarning`, `stats()["corruption_events"] += 1` |
| single-byte damage in a segment data block | flip one bit | the read that touches the block raises `CorruptRecordError`; no value is returned |
| damaged segment footer / index / bloom | flip one bit in those regions | opening the store raises `CorruptRecordError` |
| truncated segment file | cut the file below the footer | opening the store raises `CorruptRecordError` |
| missing segment listed in MANIFEST | delete a listed file | opening the store raises `CorruptRecordError` |

Not modelled, and therefore not guaranteed:

- storage that silently reorders writes beyond what `fsync` promises;
- torn sectors or bit rot that happens *after* the data was verified but
  before it is read again (there is no background scrub);
- two processes opening the same directory (no lock file);
- a length field corrupted in the middle of a WAL. This is
  indistinguishable from a torn append, so it is handled as one: replay
  stops there, the tail is discarded, and a warning is emitted. Records
  after the damage are lost, but no wrong value is ever returned.

## 2. Invariants

**I1 -- Acknowledged writes survive.** With `sync="always"`, once `put()`
returns, the record is fsynced in the WAL and will be replayed after any
process kill.
Enforced in `wal.WAL._write`; tested by 140 deterministic kill points.

**I2 -- The WAL is the only source of unflushed truth.** A memtable is
never the sole copy of an acknowledged write: every mutation is appended
to the WAL first, under the same writer lock.
Enforced in `engine.Store.put/delete/_commit_batch`.

**I3 -- A segment becomes visible only when complete.** Segments are
written to `.tmp`, fsynced, renamed, and only then listed in MANIFEST.
Enforced in `segment.write_segment` and `engine.Store._drain_flushes`.

**I4 -- A WAL file is deleted only after its contents are in a durable
segment.** The order is: segment durable -> MANIFEST updated -> WAL
unlinked.
Enforced in `engine.Store._drain_flushes`; tested by the flush-crossing
kill points.

**I5 -- A segment file is deleted only when no snapshot can reference it.**
Compaction retires its inputs instead of unlinking them.  Each read holds
a lease (implicitly for one `get()` or scan, explicitly via
`Store.snapshot()`); when the last lease goes, the retired files are
unlinked and their handles closed.  Anything left over is removed by the
next `open()`, which deletes every segment the MANIFEST does not list.
Enforced in `engine.Store._acquire_snapshot/_release_snapshot/_reclaim`;
tested in `test_compaction.py`.

**I6 -- No unverified bytes are returned.** Decoded data always passed a
CRC32 first: WAL records before replay, segment blocks before decoding,
the index and bloom filter before use.
Enforced in `encoding.decode_wal_record`, `segment.SegmentReader._read_block`,
`segment.SegmentReader.__init__`.

**I7 -- A batch is all or nothing.** A batch is one WAL record with one
CRC; replay applies all of its operations or stops before it.
Enforced in `encoding.encode_batch` / `wal.WAL.replay`.

**I8 -- One writer per directory.** Opening a store takes an exclusive
lock on a `LOCK` file; a second opener raises `StoreLockedError` rather
than interleaving WAL appends with the first.  The lock is released when
the process exits, so a crash does not leave the directory unopenable.
Enforced in `engine._DirectoryLock`; tested in `test_engine.py`.

## 3. Recovery algorithm

1. Delete `*.tmp` leftovers.
2. Read MANIFEST. If it is missing, unreadable, or fails its checksum,
   warn, count a corruption event, and rebuild the live set from the
   directory.
3. Delete every `seg-*.sst` the manifest does not list.
4. Open each live segment: verify magic, footer bounds, index CRC, bloom
   CRC, then parse the sparse index.
5. Replay `wal-*.log` in ascending sequence order. Each file is read in
   64 KiB chunks and parsed in place, so recovery never holds a whole WAL
   in memory.  For each file, stop at the first torn or failing record,
   truncate to the last verified byte, and warn once.
6. Keep the highest-numbered WAL as the active append target; the rest are
   deleted after the next successful flush.
7. If the recovered memtable is already at the size threshold, flush it
   immediately.

`Store.verify()` and `minikv fsck` re-run steps 4 and 5 and decode every
record on purpose -- the read path verifies lazily, this does not.

## 4. How the tests inject faults

### 4.1 Deterministic kill points

`tests/fixtures/writer_child.py` runs in a separate interpreter, opens the
store with `sync="always"`, writes keys in order, and calls `os._exit(7)`
immediately after the `put()` at a chosen index returns.

The parent (`tests/test_crash_recovery.py`) asserts the exit code, reopens
the store, and checks that keys 0..kill_at are all present and correct.

- 120 kill points with a 4 KiB memtable (no flush involved),
- 20 kill points with a 1 KiB memtable, which forces a flush every few
  writes and therefore lands kills during and around flush bookkeeping.

### 4.2 Named flush steps

A flush walks a fixed protocol (build the temporary file, fsync it, rename
it, fsync the directory, update the manifest, fsync it, delete the WAL it
replaced).  Each step is a named fault point in `minikv/faults.py`:

    segment_tmp_written   the .tmp file exists and is durable, not renamed
    segment_renamed       the segment is in place, the manifest is not
    manifest_before       as above, immediately before the manifest write
    manifest_after        the manifest lists it, the old WAL still exists
    wal_before_delete     as above, immediately before the unlink
    wal_after_delete      the flush completed
    compaction_merged     the merged file exists, the manifest is old
    compaction_manifest   the manifest is updated, the inputs are retired

Setting `MINIKV_FAULT=<name>` makes the process call `os._exit(9)` when it
reaches that point; `MINIKV_FAULT=<name>:2` fires on the second visit, and
`MINIKV_FAULT_ACTION=raise` raises `InjectedFault` instead so a test can
stay in-process.  A name that is not in the list can never fire, so
`faults.unknown_points()` exists to catch a typo.

For each of the six flush points the suite starts a child that writes with
`sync="always"` and prints every index as soon as its `put()` returns, so
the parent knows exactly which writes were acknowledged.  It then asserts
that

- every acknowledged write is present with the right value,
- `verify()` reports no damage,
- a new write succeeds after recovery,
- and a reopen sees that new write.

In other words, the ordering claimed in docs/format.md is exercised at
every boundary rather than at one convenient moment.

### 4.3 External kills

The child writes in a loop; the parent calls `kill()` after a random delay
between 150 and 500 ms, eight times. After each kill the store is reopened
and every key that exists must be correct, and the keys after the first
gap must be absent -- writes are sequential, so a surviving set that is
not a prefix would indicate a torn record was decoded.

### 4.4 A stateful property test, crashes included

`tests/test_properties.py` runs a hypothesis `RuleBasedStateMachine` whose
rules are `put`, `delete`, `batch`, `flush`, `compact`, `reopen`,
`crash_during_flush` and `crash_during_compaction`.  The oracle is a plain
dict, compared against a full scan after *every* step, so a divergence is
reported against the rule that caused it and hypothesis shrinks the
sequence to the shortest failing one.

The crash rules arm a fault point with `MINIKV_FAULT_ACTION=raise` so the
process survives and the machine can reopen in-process; the on-disk state
is the one a real kill would leave.  A separate test asserts that the
crash rules were actually selected, so they cannot be silently skipped.

### 4.5 Damage injection

Corruption tests mutate real files in a real temporary directory; nothing
is mocked:

- append junk to a WAL, or flip a byte inside a record body;
- truncate a segment below its footer;
- overwrite the footer magic;
- flip a bit in the index or bloom region;
- sweep 60 single-bit flips across a segment, re-reading every key after
  each one, and assert that any answer returned is the correct one.

## 5. What the tests do not cover

- power loss with a lying disk controller;
- filesystem behaviour on network shares;
- two processes racing for the same directory (the lock is asserted to
  reject the second opener, not to survive a hostile one);
- dataloss from a segment file that is valid but from the wrong
  generation: the manifest checksums its bytes, not its meaning.

These are listed so that the guarantee table in README.md can be read
literally.

## 6. Reproducing

```
pytest tests/test_crash_recovery.py -q     # 140 kill points + 6 flush faults
pytest tests/test_corruption.py -q         # ~2 s
pytest tests/test_properties.py -q         # stateful machine + hypothesis
pytest -q                                  # 119 tests, ~43 s
pytest tests/test_compaction.py -q         # includes the soak and I5 tests
```

All crash tests use `sync="always"`; testing the weaker modes for
durability would not prove anything they do not promise.
