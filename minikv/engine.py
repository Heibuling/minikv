"""The public Store: lifecycle, write path, recovery, compaction.

Crash-safety order
------------------

Every acknowledged write follows this order:

1. append to the active WAL (fsync according to the sync policy),
2. apply to the in-memory memtable.

A segment flush follows this order:

3. build seg-NNNNNN.sst.tmp, fsync the file,
4. rename it to seg-NNNNNN.sst, fsync the directory (POSIX only),
5. write MANIFEST listing the new live set, fsync + rename,
6. unlink the WAL files whose contents are now inside that segment.

A crash before step 5 leaves the manifest unchanged, so the new segment is
ignored (and deleted) on the next open while the WAL still replays the
data.  A crash between 5 and 6 leaves a redundant WAL whose replay writes
the same values again (idempotent).

Compaction writes its output outside the writer lock and only takes the
lock to splice the result in, so a long merge does not stall writers.  The
input files are *not* unlinked immediately: a reader may still be iterating
a snapshot that points at them.  They are removed on close(), or by the
next open(), which deletes every segment the manifest does not list.

Concurrency
-----------

One writer lock serialises every mutation; readers never take it.  A
reader reads the 'snapshot' attribute -- an immutable (memtable, segments)
pair that the writer replaces atomically.  Segments are immutable, and the
memtable is append/overwrite-only while published, so a reader always sees
a consistent value per key.  A reader may miss a write that commits after
it took its snapshot; it never sees a torn one.

One process per directory
-------------------------

Opening a directory takes an exclusive lock on a LOCK file inside it.  A
second Store -- in this process or another -- raises StoreLockedError
instead of interleaving WAL appends with the first one.
"""

from __future__ import annotations

import contextlib
import glob
import heapq
import itertools
import os
import threading
import warnings
from collections.abc import Iterator
from types import TracebackType
from typing import NamedTuple, TypedDict

from .compaction import merge_segments
from .encoding import (
    WAL_TYPE_DELETE,
    WAL_TYPE_PUT,
    crc32,
    validate_key,
    validate_pair,
)
from .errors import ClosedStoreError, CorruptRecordError, StoreLockedError
from .faults import fault_point
from .memtable import TOMBSTONE, MemTable, Value, _Tombstone
from .segment import DEFAULT_BLOCK_BYTES, BlockCache, SegmentReader, fsync_dir, write_segment
from .wal import SYNC_MODES, WAL, IntervalSyncer, ReplayResult, warn_corruption

MANIFEST_NAME = "MANIFEST"
MANIFEST_MAGIC = "minikv-manifest-v2"
LOCK_NAME = "LOCK"


class SegmentCheck(TypedDict, total=False):
    """One line of a verify() report for a segment."""

    name: str
    ok: bool
    entries: int
    error: str


class WalCheck(TypedDict):
    """One line of a verify() report for a WAL file."""

    name: str
    ok: bool
    problem: str | None
    records: int
    bytes: int


class VerifyReport(TypedDict):
    """What Store.verify() returns: every finding, not just the first."""

    ok: bool
    segments: list[SegmentCheck]
    wal_files: list[WalCheck]
    errors: list[str]


class Snapshot(NamedTuple):
    """An immutable view handed to readers."""

    memtable: MemTable
    segments: tuple[SegmentReader, ...]


class _DirectoryLock:
    """Exclusive advisory lock on a store directory.

    POSIX uses flock; Windows uses msvcrt byte-range locking.  Both are
    advisory and both are released when the process exits, so a crashed
    process does not leave the directory permanently unopenable.
    """

    def __init__(self, directory: str) -> None:
        self.path = os.path.join(directory, LOCK_NAME)
        self._fd: int | None = None

    def acquire(self) -> None:
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            if os.fstat(fd).st_size == 0:
                os.write(fd, b"\n")
            if os.name == "nt":
                import msvcrt

                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                # typeshed's fcntl stub has no locking attributes on this
                # platform; the branch only runs on POSIX.
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[attr-defined]
        except OSError as exc:
            os.close(fd)
            raise StoreLockedError(
                f"{self.path} is already locked by another Store or process"
            ) from exc
        except ImportError:  # pragma: no cover - no locking module available
            pass
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, f"pid={os.getpid()}\n".encode())
        except OSError:  # pragma: no cover - informational only
            pass
        self._fd = fd

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_UN)  # type: ignore[attr-defined]
        except OSError:  # pragma: no cover - unlock failures are not fatal
            pass
        finally:
            os.close(fd)


def _as_bytes(value: object, what: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    raise TypeError(f"{what} must be bytes, got {type(value).__name__}")


class Store:
    """An embedded, single-process, LSM-style key-value store.

    Keys and values are bytes.  Keys are ordered by raw byte comparison,
    never by locale.  The empty key is a legal key; keys are limited to
    64 KiB and values to 16 MiB.
    """

    def __init__(
        self,
        path: str,
        *,
        sync: str = "always",
        memtable_bytes: int = 4 << 20,
        block_bytes: int = DEFAULT_BLOCK_BYTES,
        wal_sync_interval_ms: int = 100,
        block_cache_bytes: int = 8 << 20,
    ) -> None:
        if sync not in SYNC_MODES:
            raise ValueError(f"sync must be one of {SYNC_MODES}, got {sync!r}")
        if memtable_bytes < 1024:
            raise ValueError("memtable_bytes must be at least 1024")
        self.path = os.path.abspath(path)
        self.sync_mode = sync
        self.memtable_bytes = int(memtable_bytes)
        self.block_bytes = int(block_bytes)
        self.wal_sync_interval_ms = int(wal_sync_interval_ms)
        # Decoded, CRC-verified blocks.  A 4 KiB block holds ~34 records, so
        # one disk read serves ~34 point reads; BENCHMARKS.md measures it at
        # 3.2x on a full sweep and 4.0x on a hot working set.  Set 0 to
        # disable, for example when measuring cold reads.
        self.block_cache_bytes = int(block_cache_bytes)
        self._block_cache = BlockCache(self.block_cache_bytes) if self.block_cache_bytes else None

        os.makedirs(self.path, exist_ok=True)
        self._lock = threading.RLock()
        self._flush_lock = threading.Lock()
        self._compact_lock = threading.Lock()
        self._closed = False
        self._corruption_events = 0
        self._pending: list[tuple[MemTable, list[str]]] = []
        # Segments compaction replaced.  They stay readable, and on disk,
        # until no snapshot can reference them any more.
        self._retired: list[SegmentReader] = []
        self._reclaim_lock = threading.Lock()
        self._live_snapshots = 0
        self._syncer: IntervalSyncer | None = None

        self._directory_lock = _DirectoryLock(self.path)
        self._directory_lock.acquire()
        try:
            self._remove_temporary_files()
            self._segments = tuple(self._load_segments())
            self._seq = self._max_sequence()

            self._memtable = MemTable()
            # A segment records the highest WAL sequence its contents came
            # from.  Anything at or below the watermark is already on disk in
            # a segment, so replaying it would only rebuild data we already
            # have -- it is the crash window between "manifest updated" and
            # "WAL unlinked", and it grows if an unlink ever fails.
            watermark = max((reader.wal_seq for reader in self._segments), default=0)
            self.checkpoint_wal_seq = watermark
            wal_paths = []
            for candidate in sorted(glob.glob(os.path.join(self.path, "wal-*.log"))):
                if self._wal_seq(candidate) <= watermark:
                    with contextlib.suppress(OSError):
                        os.unlink(candidate)
                    continue
                wal_paths.append(candidate)
            for wal_path in wal_paths:
                self._replay_wal(wal_path)

            self._pending_wals: list[str] = wal_paths[:-1]
            if wal_paths:
                self._active_wal = WAL(wal_paths[-1], sync)
            else:
                self._seq += 1
                self._active_wal = WAL(self._wal_path(self._seq), sync)

            self._snapshot = Snapshot(self._memtable, self._segments)
            if sync == "interval":
                self._syncer = IntervalSyncer(self._sync_active_wal, self.wal_sync_interval_ms)

            if self._memtable.approx_bytes >= self.memtable_bytes:
                with self._lock:
                    self._enqueue_flush_locked()
                self._drain_flushes()
        except BaseException:
            self._directory_lock.release()
            raise

    # -- paths -----------------------------------------------------------

    def _wal_path(self, seq: int) -> str:
        return os.path.join(self.path, f"wal-{seq:06d}.log")

    def _segment_path(self, seq: int) -> str:
        return os.path.join(self.path, f"seg-{seq:06d}.sst")

    @staticmethod
    def _wal_seq(path: str) -> int:
        """The sequence number encoded in a wal-NNNNNN.log file name."""
        name = os.path.basename(path)
        digits = name[4:10]
        return int(digits) if digits.isdigit() else 0

    def _manifest_path(self) -> str:
        return os.path.join(self.path, MANIFEST_NAME)

    def _max_sequence(self) -> int:
        best = 0
        for name in os.listdir(self.path):
            if name.startswith(("seg-", "wal-")) and name[4:10].isdigit():
                best = max(best, int(name[4:10]))
        return best

    def _remove_temporary_files(self) -> None:
        for leftover in glob.glob(os.path.join(self.path, "*.tmp")):
            with contextlib.suppress(OSError):
                os.unlink(leftover)

    # -- manifest --------------------------------------------------------

    def _read_manifest(self) -> list[tuple[str, int]] | None:
        """Return (segment name, WAL coverage) newest first, or None.

        A v1 manifest (no coverage column) and a damaged one are both
        treated as "unknown": the live set is rebuilt from the directory
        with coverage 0, which means nothing is skipped and every WAL is
        replayed.  That is slower and always safe, which is the right way
        round.
        """
        try:
            with open(self._manifest_path(), encoding="utf-8") as handle:
                text = handle.read()
        except FileNotFoundError:
            return None
        lines = text.splitlines()
        if len(lines) < 2 or lines[0] != MANIFEST_MAGIC:
            self._corruption_events += 1
            warnings.warn("minikv: MANIFEST is unreadable; rebuilding from the directory",
                          RuntimeWarning, stacklevel=2)
            return None
        body = "\n".join(lines[:-1]) + "\n"
        try:
            expected = int(lines[-1].split()[1], 16)
        except (IndexError, ValueError):
            expected = -1
        if crc32(body.encode("utf-8")) != expected:
            self._corruption_events += 1
            warnings.warn("minikv: MANIFEST failed its checksum; rebuilding from the directory",
                          RuntimeWarning, stacklevel=2)
            return None
        listed: list[tuple[str, int]] = []
        for line in lines[1:-1]:
            name, _, coverage = line.partition(" ")
            try:
                wal_seq = int(coverage)
            except ValueError:
                wal_seq = 0
            listed.append((name, wal_seq))
        return listed

    def _write_manifest(self) -> None:
        body = MANIFEST_MAGIC + "\n" + "".join(
            f"{reader.name} {reader.wal_seq}\n" for reader in self._segments
        )
        payload = body + f"crc32 {crc32(body.encode('utf-8')):08x}\n"
        manifest = self._manifest_path()
        temporary = manifest + ".tmp"
        with open(temporary, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, manifest)
        fsync_dir(self.path)

    # -- recovery --------------------------------------------------------

    def _load_segments(self) -> list[SegmentReader]:
        on_disk = {
            name
            for name in os.listdir(self.path)
            if name.startswith("seg-") and name.endswith(".sst")
        }
        listed = self._read_manifest()
        if listed is None:
            entries = [(name, 0) for name in sorted(on_disk, reverse=True)]  # newest first
        else:
            missing = [name for name, _ in listed if name not in on_disk]
            if missing:
                raise CorruptRecordError(f"MANIFEST lists missing segment(s): {missing}")
            entries = listed

        for stale in on_disk - {name for name, _ in entries}:
            with contextlib.suppress(OSError):
                os.unlink(os.path.join(self.path, stale))

        readers = []
        for name, wal_seq in entries:
            reader = SegmentReader(os.path.join(self.path, name), self._block_cache)
            reader.wal_seq = wal_seq
            readers.append(reader)
        self._segments = tuple(readers)
        self._write_manifest()
        return readers

    def _replay_wal(self, path: str) -> None:
        """Apply a WAL to the memtable as its records are decoded.

        Streaming matters here: the previous version materialised every
        record into a list first, so a 20 000-record log became 20 000
        tuples before the first one was applied.
        """
        state = ReplayResult()
        self._memtable.load(self._memtable_items(WAL.iter_replay(path, state)))
        if state.problem is not None:
            self._corruption_events += 1
            WAL.truncate_to(path, state.valid_bytes)
            warn_corruption(path, state.problem, state.valid_bytes, state.file_bytes)

    @staticmethod
    def _memtable_items(
        operations: Iterator[tuple[int, bytes, bytes]],
    ) -> Iterator[tuple[bytes, Value]]:
        for rec_type, key, value in operations:
            if rec_type == WAL_TYPE_PUT:
                yield key, value
            elif rec_type == WAL_TYPE_DELETE:
                yield key, TOMBSTONE
            else:  # pragma: no cover - replay only yields known types
                raise CorruptRecordError(f"unknown WAL record type {rec_type}")

    # -- writes ----------------------------------------------------------

    def put(self, key: bytes, value: bytes) -> None:
        key = _as_bytes(key, "key")
        value = _as_bytes(value, "value")
        validate_pair(key, value)
        with self._lock:
            self._check_open()
            self._active_wal.append_put(key, value)
            self._memtable.put(key, value)
            schedule = self._maybe_enqueue_flush_locked()
        if schedule:
            self._drain_flushes()

    def delete(self, key: bytes) -> None:
        key = _as_bytes(key, "key")
        validate_key(key)
        with self._lock:
            self._check_open()
            self._active_wal.append_delete(key)
            self._memtable.delete(key)
            schedule = self._maybe_enqueue_flush_locked()
        if schedule:
            self._drain_flushes()

    def batch(self) -> _Batch:
        """Return a context manager that commits its writes atomically."""
        return _Batch(self)

    def _commit_batch(self, ops: list[tuple[int, bytes, bytes]]) -> None:
        if not ops:
            return
        with self._lock:
            self._check_open()
            self._active_wal.append_batch(ops)
            for rec_type, key, value in ops:
                if rec_type == WAL_TYPE_PUT:
                    self._memtable.put(key, value)
                else:
                    self._memtable.delete(key)
            schedule = self._maybe_enqueue_flush_locked()
        if schedule:
            self._drain_flushes()

    def _maybe_enqueue_flush_locked(self) -> bool:
        if self._memtable.approx_bytes < self.memtable_bytes:
            return False
        self._enqueue_flush_locked()
        return True

    def _enqueue_flush_locked(self) -> None:
        frozen = self._memtable
        self._memtable = MemTable()
        old_wal = self._active_wal
        self._seq += 1
        self._active_wal = WAL(self._wal_path(self._seq), self.sync_mode)
        old_wal.close()
        self._pending.append((frozen, [*self._pending_wals, old_wal.path]))
        self._pending_wals = []
        self._snapshot = Snapshot(self._memtable, self._segments)

    def _drain_flushes(self) -> None:
        if not self._flush_lock.acquire(blocking=False):
            return
        try:
            while True:
                with self._lock:
                    if not self._pending:
                        return
                    frozen, wal_paths = self._pending.pop(0)
                    self._seq += 1
                    seq = self._seq
                segment_path = self._segment_path(seq)
                try:
                    write_segment(segment_path, frozen.items_sorted(), len(frozen), self.block_bytes)
                    reader = SegmentReader(segment_path, self._block_cache)
                    reader.wal_seq = max((self._wal_seq(path) for path in wal_paths), default=0)
                except BaseException:
                    with self._lock:
                        self._pending.insert(0, (frozen, wal_paths))
                    raise
                with self._lock:
                    self._segments = (reader, *self._segments)
                    self._snapshot = Snapshot(self._memtable, self._segments)
                    fault_point("manifest_before")
                    self._write_manifest()
                    fault_point("manifest_after")
                fault_point("wal_before_delete")
                for wal_path in wal_paths:
                    with contextlib.suppress(OSError):
                        os.unlink(wal_path)
                fault_point("wal_after_delete")
        finally:
            self._flush_lock.release()

    # -- reads -----------------------------------------------------------

    def get(self, key: bytes, default: bytes | None = None) -> bytes | None:
        key = _as_bytes(key, "key")
        self._check_open()
        view = self._acquire_snapshot()
        try:
            return self._get_from(view, key, default)
        finally:
            self._release_snapshot()

    @staticmethod
    def _get_from(view: Snapshot, key: bytes, default: bytes | None) -> bytes | None:
        value = view.memtable.get(key)
        if value is not None:
            if isinstance(value, _Tombstone):
                return default
            return value
        for segment in view.segments:
            value = segment.get(key)
            if value is not None:
                if isinstance(value, _Tombstone):
                    return default
                return value
        return default

    def scan(
        self,
        start: bytes = b"",
        end: bytes | None = None,
        *,
        reverse: bool = False,
        limit: int | None = None,
    ) -> Iterator[tuple[bytes, bytes]]:
        """Yield (key, value) in key order for start <= key <= end.

        Both directions stream: only a single 4 KiB block is ever held in
        memory, and segments whose key range does not overlap the request
        are skipped without being opened.
        """
        start = _as_bytes(start, "start")
        if end is not None:
            end = _as_bytes(end, "end")
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")
        self._check_open()
        view = self._acquire_snapshot()
        try:
            return self._leased(self._scan_from(view, start, end, reverse=reverse, limit=limit))
        except BaseException:
            self._release_snapshot()
            raise

    def _scan_from(
        self,
        view: Snapshot,
        start: bytes,
        end: bytes | None,
        *,
        reverse: bool,
        limit: int | None,
    ) -> Iterator[tuple[bytes, bytes]]:
        segments = [segment for segment in view.segments if segment.overlaps(start, end)]
        if reverse:
            sources = [view.memtable.scan_backward(start, end)]
            sources.extend(segment.scan_backward(start, end) for segment in segments)
            merged = self._merge_sources_desc(sources)
        else:
            sources = [view.memtable.scan(start, end)]
            sources.extend(segment.scan(start, end) for segment in segments)
            merged = self._merge_sources(sources)
        guarded = self._guarded(merged)
        if limit is None:
            return guarded
        return itertools.islice(guarded, limit)

    def _leased(self, source: Iterator[tuple[bytes, bytes]]) -> Iterator[tuple[bytes, bytes]]:
        """Hold a snapshot lease for exactly as long as the iterator lives."""
        try:
            yield from source
        finally:
            self._release_snapshot()

    def _guarded(self, source: Iterator[tuple[bytes, bytes]]) -> Iterator[tuple[bytes, bytes]]:
        """Fail with ClosedStoreError if the store is closed mid-iteration."""
        for item in source:
            self._check_open()
            yield item

    @staticmethod
    def _merge_sources(
        sources: list[Iterator[tuple[bytes, Value]]],
    ) -> Iterator[tuple[bytes, bytes]]:
        heap: list[tuple[bytes, int, Value]] = []

        def advance(index: int) -> None:
            try:
                key, value = next(sources[index])
            except StopIteration:
                return
            heapq.heappush(heap, (key, index, value))

        for index in range(len(sources)):
            advance(index)

        while heap:
            key = heap[0][0]
            group: list[tuple[int, Value]] = []
            while heap and heap[0][0] == key:
                _, index, value = heapq.heappop(heap)
                group.append((index, value))
                advance(index)
            winner = min(group, key=lambda item: item[0])[1]
            if isinstance(winner, _Tombstone):
                continue
            yield key, winner

    @staticmethod
    def _merge_sources_desc(
        sources: list[Iterator[tuple[bytes, Value]]],
    ) -> Iterator[tuple[bytes, bytes]]:
        """Descending merge of already-descending sources (newest wins)."""
        heads: list[tuple[int, bytes, Value]] = []
        for index, source in enumerate(sources):
            try:
                key, value = next(source)
            except StopIteration:
                continue
            heads.append((index, key, value))

        while heads:
            highest = max(head[1] for head in heads)
            winner_index = -1
            winner_value: Value | None = None
            remaining: list[tuple[int, bytes, Value]] = []
            for index, key, value in heads:
                if key == highest:
                    if winner_index < 0 or index < winner_index:
                        winner_index, winner_value = index, value
                    try:
                        next_key, next_value = next(sources[index])
                        remaining.append((index, next_key, next_value))
                    except StopIteration:
                        pass
                else:
                    remaining.append((index, key, value))
            heads = remaining
            if winner_value is not None and not isinstance(winner_value, _Tombstone):
                yield highest, winner_value

    # -- maintenance -----------------------------------------------------

    def snapshot(self) -> StoreSnapshot:
        """Return a read-only view with an explicit lifetime.

        While a snapshot is held, compaction will not delete the segment
        files it references, so a long scan cannot be invalidated halfway
        through.  Release it with a context manager, close(), or by
        dropping the last reference; the retired files are then reclaimed
        immediately instead of at close().
        """
        self._check_open()
        return StoreSnapshot(self, self._acquire_snapshot())

    def _acquire_snapshot(self) -> Snapshot:
        with self._reclaim_lock:
            self._live_snapshots += 1
        return self._snapshot

    def _release_snapshot(self) -> None:
        pending: list[SegmentReader] = []
        with self._reclaim_lock:
            self._live_snapshots -= 1
            if self._live_snapshots <= 0:
                self._live_snapshots = 0
                pending, self._retired = self._retired, []
        self._reclaim(pending)

    def _drain_retired_locked(self) -> list[SegmentReader]:
        """Hand over the retired segments, if no snapshot can reach them."""
        with self._reclaim_lock:
            if self._live_snapshots > 0:
                return []
            pending, self._retired = self._retired, []
        return pending

    @staticmethod
    def _reclaim(readers: list[SegmentReader]) -> None:
        for reader in readers:
            reader.close()
            with contextlib.suppress(OSError):
                os.unlink(reader.path)

    def flush(self) -> None:
        """Force the current memtable to a segment, if it holds anything.

        Makes the WAL a bounded, known size on demand: useful before a
        backup, and in tests that want to control when a segment appears.
        """
        with self._lock:
            self._check_open()
            if not self._memtable:
                return
            self._enqueue_flush_locked()
        self._drain_flushes()

    def compact(self) -> dict[str, int]:
        """Merge every live segment into one, dropping tombstones.

        The merge itself runs outside the writer lock; only the splice at
        the end is serialised.  Segments flushed while the merge is running
        are kept and placed in front of the merged result, because they are
        newer than everything that was merged.
        """
        if not self._compact_lock.acquire(blocking=False):
            raise RuntimeError("compaction is already running")
        try:
            with self._lock:
                self._check_open()
                readers = self._segments
                if len(readers) < 2:
                    return {
                        "segments_merged": len(readers),
                        "segments_after": len(readers),
                        "entries": sum(reader.num_entries for reader in readers),
                    }
                self._seq += 1
                seq = self._seq
            output = self._segment_path(seq)
            try:
                merge_segments(readers, output, self.block_bytes)
                merged = SegmentReader(output, self._block_cache)
                merged.wal_seq = max((reader.wal_seq for reader in readers), default=0)
            except BaseException:
                with contextlib.suppress(OSError):
                    os.unlink(output)
                raise

            with self._lock:
                if self._closed:
                    merged.close()
                    with contextlib.suppress(OSError):
                        os.unlink(output)
                    raise ClosedStoreError("the store was closed during compaction")
                others = tuple(
                    segment
                    for segment in self._segments
                    if all(segment is not reader for reader in readers)
                )
                self._retired.extend(readers)
                self._segments = (*others, merged)
                self._snapshot = Snapshot(self._memtable, self._segments)
                fault_point("compaction_merged")
                self._write_manifest()
                fault_point("compaction_manifest")
                pending = self._drain_retired_locked()
                result = {
                    "segments_merged": len(readers),
                    "segments_after": len(self._segments),
                    "entries": merged.num_entries,
                }
            # Unlinking the inputs measured ~275 us per file on Windows, so it
            # happens outside the writer lock: any reader that could still
            # reference these segments already held a lease when the list was
            # drained, and a reader that arrives later sees the new snapshot.
            self._reclaim(pending)
            return result
        finally:
            self._compact_lock.release()

    def verify(self) -> VerifyReport:
        """Read back every block of every segment and every WAL.

        This is the full scrub the lazy read path deliberately avoids: it
        decodes all data and checks every CRC.  Returns a report; it does
        not raise on damage, so a caller can print all findings at once.
        """
        self._check_open()
        view = self._acquire_snapshot()
        try:
            return self._verify_view(view)
        finally:
            self._release_snapshot()

    def _verify_view(self, view: Snapshot) -> VerifyReport:
        segments_report: list[SegmentCheck] = []
        wal_report: list[WalCheck] = []
        errors: list[str] = []

        for segment in view.segments:
            try:
                entries = segment.verify()
                segments_report.append({"name": segment.name, "ok": True, "entries": entries})
            except CorruptRecordError as exc:
                segments_report.append({"name": segment.name, "ok": False, "error": str(exc)})
                errors.append(str(exc))

        for path in sorted(glob.glob(os.path.join(self.path, "wal-*.log"))):
            result = WAL.replay(path)
            ok = result.problem is None
            wal_report.append(
                {
                    "name": os.path.basename(path),
                    "ok": ok,
                    "problem": result.problem,
                    "records": len(result.entries),
                    "bytes": result.file_bytes,
                }
            )
            if not ok:
                errors.append(f"{os.path.basename(path)}: {result.problem} record")

        if self._pending:
            segments_report.append(
                {"name": "memtable (pending flush)", "ok": True, "entries": len(self._pending)}
            )
        return {
            "ok": not errors,
            "segments": segments_report,
            "wal_files": wal_report,
            "errors": errors,
        }

    def stats(self) -> dict[str, object]:
        self._check_open()
        snapshot = self._snapshot
        segment_bytes = sum(segment.size_bytes for segment in snapshot.segments)
        wal_bytes = os.path.getsize(self._active_wal.path) if os.path.exists(self._active_wal.path) else 0
        return {
            "sync": self.sync_mode,
            "memtable_bytes_limit": self.memtable_bytes,
            "memtable_bytes": snapshot.memtable.approx_bytes,
            "memtable_keys": len(snapshot.memtable),
            "segments": len(snapshot.segments),
            "segment_entries": sum(segment.num_entries for segment in snapshot.segments),
            "segment_bytes": segment_bytes,
            "wal_bytes": wal_bytes,
            "pending_flushes": len(self._pending),
            "checkpoint_wal_seq": max((reader.wal_seq for reader in snapshot.segments), default=0),
            "deferred_deletes": len(self._retired),
            "live_snapshots": self._live_snapshots,
            "block_cache_bytes": self.block_cache_bytes,
            "block_cache_used": self._block_cache.used_bytes if self._block_cache else 0,
            "corruption_events": self._corruption_events,
            "fsync_dir_supported": os.name != "nt",
        }

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._syncer is not None:
                self._syncer.stop()
                self._syncer = None
            with contextlib.suppress(OSError, ValueError):
                self._active_wal.sync()
            self._active_wal.close()

            live = list(self._segments)
            with self._reclaim_lock:
                retired, self._retired = self._retired, []
                self._live_snapshots = 0
            for reader in live:
                reader.close()
            self._reclaim(retired)
            self._directory_lock.release()

    def _sync_active_wal(self) -> None:
        with self._lock:
            if not self._closed:
                self._active_wal.sync()

    def _check_open(self) -> None:
        if self._closed:
            raise ClosedStoreError("the store is closed")

    def __enter__(self) -> Store:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class StoreSnapshot:
    """A read-only view with an explicit lifetime.

    The value of holding one is not isolation -- values are immutable, so
    any view is stable -- it is *file lifetime*: while this object is
    alive, compaction will not delete the segment files it points at, so a
    long scan cannot be invalidated halfway through.  When the last
    snapshot is released, the files compaction retired are unlinked and
    their handles closed immediately, instead of accumulating until the
    store closes.
    """

    __slots__ = ("_released", "_store", "_view")

    def __init__(self, store: Store, view: Snapshot) -> None:
        self._store = store
        self._view = view
        self._released = False

    @property
    def segments(self) -> tuple[SegmentReader, ...]:
        """The segments this snapshot can read, newest first."""
        return self._view.segments

    @property
    def released(self) -> bool:
        return self._released

    def get(self, key: bytes, default: bytes | None = None) -> bytes | None:
        self._check_usable()
        return self._store._get_from(self._view, _as_bytes(key, "key"), default)

    def scan(
        self,
        start: bytes = b"",
        end: bytes | None = None,
        *,
        reverse: bool = False,
        limit: int | None = None,
    ) -> Iterator[tuple[bytes, bytes]]:
        self._check_usable()
        start = _as_bytes(start, "start")
        if end is not None:
            end = _as_bytes(end, "end")
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")
        return self._store._scan_from(self._view, start, end, reverse=reverse, limit=limit)

    def _check_usable(self) -> None:
        if self._released:
            raise ClosedStoreError("this snapshot has been released")
        self._store._check_open()

    def close(self) -> None:
        if not self._released:
            self._released = True
            self._store._release_snapshot()

    def __enter__(self) -> StoreSnapshot:
        self._check_usable()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        with contextlib.suppress(Exception):
            self.close()


class _Batch:
    """Context manager collecting writes to commit under one WAL record."""

    def __init__(self, store: Store) -> None:
        self._store = store
        self._ops: list[tuple[int, bytes, bytes]] = []
        self._active = False

    def __enter__(self) -> _Batch:
        self._store._check_open()
        self._active = True
        return self

    def put(self, key: bytes, value: bytes) -> None:
        self._require_active()
        key_bytes = _as_bytes(key, "key")
        value_bytes = _as_bytes(value, "value")
        validate_pair(key_bytes, value_bytes)
        self._ops.append((WAL_TYPE_PUT, key_bytes, value_bytes))

    def delete(self, key: bytes) -> None:
        self._require_active()
        key_bytes = _as_bytes(key, "key")
        validate_key(key_bytes)
        self._ops.append((WAL_TYPE_DELETE, key_bytes, b""))

    def _require_active(self) -> None:
        if not self._active:
            raise RuntimeError("batch is not active; use 'with store.batch() as b'")

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._active = False
        if exc_type is None:
            self._store._commit_batch(self._ops)
        self._ops = []
