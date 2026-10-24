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

A crash before step 5 leaves the manifest unchanged, so the new segment
is ignored (and deleted) on the next open while the WAL still replays the
data.  A crash between 5 and 6 leaves a redundant WAL whose replay writes
the same values again (idempotent).

Compaction has a different rule: the merged file is written and made
durable, the manifest is updated, but the input files are *not* unlinked
immediately.  A reader may still be iterating a snapshot that points at
them.  They are removed on close(), or by the next open() (which deletes
every segment the manifest does not list).

Concurrency
-----------

One writer lock serialises every mutation; readers never take it.  A
reader reads the 'snapshot' attribute -- an immutable (memtable, segments)
pair that the writer replaces atomically.  Segments are immutable, and the
memtable is append/overwrite-only while published, so a reader always sees
a consistent value per key.  A reader may miss a write that commits after
it took its snapshot; it never sees a torn one.
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
from typing import NamedTuple

from .compaction import merge_segments
from .encoding import WAL_TYPE_DELETE, WAL_TYPE_PUT, crc32
from .errors import ClosedStoreError, CorruptRecordError
from .memtable import TOMBSTONE, MemTable
from .segment import DEFAULT_BLOCK_BYTES, SegmentReader, fsync_dir, write_segment
from .wal import SYNC_MODES, WAL, IntervalSyncer, warn_corruption

MANIFEST_NAME = "MANIFEST"
MANIFEST_MAGIC = "minikv-manifest-v1"


class Snapshot(NamedTuple):
    """An immutable view handed to readers."""

    memtable: MemTable
    segments: tuple[SegmentReader, ...]


def _as_bytes(value: object, what: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    raise TypeError(f"{what} must be bytes, got {type(value).__name__}")


class Store:
    """An embedded, single-process, LSM-style key-value store.

    Keys and values are bytes.  Keys are ordered by raw byte comparison,
    never by locale.  The empty key is a legal key.

    Only one process may open a directory at a time; that is not enforced
    with a lock file, it is a documented precondition.
    """

    def __init__(
        self,
        path: str,
        *,
        sync: str = "always",
        memtable_bytes: int = 4 << 20,
        block_bytes: int = DEFAULT_BLOCK_BYTES,
        wal_sync_interval_ms: int = 100,
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

        os.makedirs(self.path, exist_ok=True)
        self._lock = threading.RLock()
        self._flush_lock = threading.Lock()
        self._closed = False
        self._corruption_events = 0
        self._pending: list[tuple[MemTable, list[str]]] = []
        self._retired: list[SegmentReader] = []

        self._remove_temporary_files()
        self._segments: tuple[SegmentReader, ...] = tuple(self._load_segments())
        self._seq = self._max_sequence()

        self._memtable = MemTable()
        wal_paths = sorted(glob.glob(os.path.join(self.path, "wal-*.log")))
        for wal_path in wal_paths:
            self._replay_wal(wal_path)

        self._pending_wals: list[str] = wal_paths[:-1]
        if wal_paths:
            self._active_wal = WAL(wal_paths[-1], sync)
        else:
            self._seq += 1
            self._active_wal = WAL(self._wal_path(self._seq), sync)

        self._snapshot = Snapshot(self._memtable, self._segments)
        self._syncer: IntervalSyncer | None = None
        if sync == "interval":
            self._syncer = IntervalSyncer(self._sync_active_wal, self.wal_sync_interval_ms)

        if self._memtable.approx_bytes >= self.memtable_bytes:
            with self._lock:
                self._enqueue_flush_locked()
            self._drain_flushes()

    # -- paths -----------------------------------------------------------

    def _wal_path(self, seq: int) -> str:
        return os.path.join(self.path, f"wal-{seq:06d}.log")

    def _segment_path(self, seq: int) -> str:
        return os.path.join(self.path, f"seg-{seq:06d}.sst")

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

    def _read_manifest(self) -> list[str] | None:
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
        return lines[1:-1]

    def _write_manifest(self) -> None:
        names = [reader.name for reader in self._segments]
        body = MANIFEST_MAGIC + "\n" + "".join(f"{name}\n" for name in names)
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
            names = sorted(on_disk, reverse=True)  # newest first
        else:
            missing = [name for name in listed if name not in on_disk]
            if missing:
                raise CorruptRecordError(f"MANIFEST lists missing segment(s): {missing}")
            names = listed

        for stale in on_disk - set(names):
            with contextlib.suppress(OSError):
                os.unlink(os.path.join(self.path, stale))

        readers = [SegmentReader(os.path.join(self.path, name)) for name in names]
        self._segments = tuple(readers)
        self._write_manifest()
        return readers

    def _replay_wal(self, path: str) -> None:
        result = WAL.replay(path)
        if result.problem is not None:
            self._corruption_events += 1
            WAL.truncate_to(path, result.valid_bytes)
            warn_corruption(path, result.problem, result.valid_bytes, result.file_bytes)
        for rec_type, key, value in result.entries:
            if rec_type == WAL_TYPE_PUT:
                self._memtable.put(key, value)
            elif rec_type == WAL_TYPE_DELETE:
                self._memtable.delete(key)
            else:  # pragma: no cover - replay only yields known types
                raise CorruptRecordError(f"unknown WAL record type {rec_type}")

    # -- writes ----------------------------------------------------------

    def put(self, key: bytes, value: bytes) -> None:
        key = _as_bytes(key, "key")
        value = _as_bytes(value, "value")
        with self._lock:
            self._check_open()
            self._active_wal.append_put(key, value)
            self._memtable.put(key, value)
            schedule = self._maybe_enqueue_flush_locked()
        if schedule:
            self._drain_flushes()

    def delete(self, key: bytes) -> None:
        key = _as_bytes(key, "key")
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
                    reader = SegmentReader(segment_path)
                except BaseException:
                    with self._lock:
                        self._pending.insert(0, (frozen, wal_paths))
                    raise
                with self._lock:
                    self._segments = (reader, *self._segments)
                    self._snapshot = Snapshot(self._memtable, self._segments)
                    self._write_manifest()
                for wal_path in wal_paths:
                    with contextlib.suppress(OSError):
                        os.unlink(wal_path)
        finally:
            self._flush_lock.release()

    # -- reads -----------------------------------------------------------

    def get(self, key: bytes, default: bytes | None = None) -> bytes | None:
        key = _as_bytes(key, "key")
        self._check_open()
        snapshot = self._snapshot
        value = snapshot.memtable.get(key)
        if value is not None:
            return default if value is TOMBSTONE else value  # type: ignore[return-value]
        for segment in snapshot.segments:
            value = segment.get(key)
            if value is not None:
                return default if value is TOMBSTONE else value  # type: ignore[return-value]
        return default

    def scan(
        self,
        start: bytes = b"",
        end: bytes | None = None,
        *,
        reverse: bool = False,
        limit: int | None = None,
    ) -> Iterator[tuple[bytes, bytes]]:
        """Yield (key, value) in ascending key order for start <= key <= end.

        'reverse' and 'limit' apply to the merged result.  Reverse scans
        materialise the matching range first (see DESIGN.md).
        """
        start = _as_bytes(start, "start")
        if end is not None:
            end = _as_bytes(end, "end")
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")
        self._check_open()
        snapshot = self._snapshot
        sources = [snapshot.memtable.scan(start, end)]
        sources.extend(segment.scan(start, end) for segment in snapshot.segments)
        merged = self._merge_sources(sources)
        if reverse:
            rows = list(merged)
            rows.reverse()
            if limit is not None:
                rows = rows[:limit]
            return iter(rows)
        if limit is None:
            return merged
        return itertools.islice(merged, limit)

    @staticmethod
    def _merge_sources(sources: list[Iterator[tuple[bytes, object]]]) -> Iterator[tuple[bytes, bytes]]:
        heap: list[tuple[bytes, int, object]] = []

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
            group: list[tuple[int, object]] = []
            while heap and heap[0][0] == key:
                _, index, value = heapq.heappop(heap)
                group.append((index, value))
                advance(index)
            winner = min(group, key=lambda item: item[0])[1]
            if winner is TOMBSTONE:
                continue
            yield key, winner  # type: ignore[misc]

    # -- maintenance -----------------------------------------------------

    def compact(self) -> dict[str, int]:
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
            output = self._segment_path(self._seq)
            merge_segments(readers, output, self.block_bytes)
            merged = SegmentReader(output)
            self._retired.extend(readers)
            self._segments = (merged,)
            self._snapshot = Snapshot(self._memtable, self._segments)
            self._write_manifest()
            return {
                "segments_merged": len(readers),
                "segments_after": 1,
                "entries": merged.num_entries,
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
            "deferred_deletes": len(self._retired),
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

            readers = list(self._segments) + list(self._retired)
            self._retired = []
            for reader in readers:
                reader.close()
            for reader in readers[len(self._segments):]:
                with contextlib.suppress(OSError):
                    os.unlink(reader.path)

    def _sync_active_wal(self) -> None:
        with self._lock:
            if not self._closed:
                self._active_wal.sync()

    def _check_open(self) -> None:
        if self._closed:
            raise ClosedStoreError("the store is closed")

    def __enter__(self) -> Store:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
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
        self._ops.append((WAL_TYPE_PUT, _as_bytes(key, "key"), _as_bytes(value, "value")))

    def delete(self, key: bytes) -> None:
        self._require_active()
        self._ops.append((WAL_TYPE_DELETE, _as_bytes(key, "key"), b""))

    def _require_active(self) -> None:
        if not self._active:
            raise RuntimeError("batch is not active; use 'with store.batch() as b'")

    def __exit__(self, exc_type, exc, tb) -> None:
        self._active = False
        if exc_type is None:
            self._store._commit_batch(self._ops)
        self._ops = []
