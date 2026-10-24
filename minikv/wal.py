"""Write-ahead log: append, sync, replay, rotate.

One WAL file belongs to one memtable generation.  When the memtable is
flushed the engine rotates to a new file (the old one cannot be truncated
in place, because writes that arrived during the flush belong to the next
generation).  A WAL file is deleted only after the segment holding its
contents is durable.

Replay streams the file in fixed-size chunks instead of reading it whole:
a WAL is normally bounded by the memtable threshold, but a crash can leave
several of them behind, and recovery should not have to hold all of that
in memory at once.

Durability policy
-----------------

sync="always"    fsync before the call returns.  An acknowledged write
                 survives a process kill and a machine crash.
sync="interval"  an fsync runs on a background timer (default 100 ms).
                 An acknowledged write survives a process kill, and
                 survives a machine crash only if the timer fired.
sync="never"     the write reaches the OS page cache and nothing more.
                 A process kill is safe; a machine crash may lose the
                 tail.
"""

from __future__ import annotations

import os
import threading
import warnings
from collections.abc import Iterator
from dataclasses import dataclass, field

from .encoding import (
    WAL_TYPE_BATCH,
    WAL_TYPE_DELETE,
    WAL_TYPE_PUT,
    decode_batch,
    decode_wal_record,
    encode_batch,
    encode_wal_record,
)
from .errors import CorruptRecordError, IncompleteRecordError

SYNC_MODES = ("always", "interval", "never")

#: Replay reads this many bytes per iteration.  A record larger than the
#: chunk simply grows the buffer until it is complete.
DEFAULT_REPLAY_CHUNK = 64 << 10


@dataclass
class ReplayResult:
    """Outcome of replaying one WAL file."""

    entries: list[tuple[int, bytes, bytes]] = field(default_factory=list)
    valid_bytes: int = 0
    file_bytes: int = 0
    records: int = 0
    problem: str | None = None  # "torn" | "corrupt" | None


class WAL:
    """Append-only log for one memtable generation."""

    def __init__(self, path: str, sync: str = "always") -> None:
        if sync not in SYNC_MODES:
            raise ValueError(f"sync must be one of {SYNC_MODES}, got {sync!r}")
        self.path = path
        self.sync_mode = sync
        self._closed = False
        # Long-lived append handle: closed by close(), not by a with-block.
        self._fh = open(path, "ab", buffering=0)  # noqa: SIM115

    # -- writes ----------------------------------------------------------

    def append_put(self, key: bytes, value: bytes) -> None:
        self._write(encode_wal_record(WAL_TYPE_PUT, key, value))

    def append_delete(self, key: bytes) -> None:
        self._write(encode_wal_record(WAL_TYPE_DELETE, key))

    def append_batch(self, ops: list[tuple[int, bytes, bytes]]) -> None:
        payload = encode_batch(ops)
        self._write(encode_wal_record(WAL_TYPE_BATCH, b"", payload))

    def _write(self, record: bytes) -> None:
        self._fh.write(record)
        if self.sync_mode == "always":
            os.fsync(self._fh.fileno())

    def sync(self) -> None:
        """Force everything written so far to stable storage."""
        if not self._closed:
            os.fsync(self._fh.fileno())

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._fh.close()

    def size(self) -> int:
        try:
            return os.path.getsize(self.path)
        except OSError:
            return 0

    # -- replay ----------------------------------------------------------

    @staticmethod
    def iter_replay(
        path: str,
        state: ReplayResult | None = None,
        chunk_bytes: int = DEFAULT_REPLAY_CHUNK,
    ) -> Iterator[tuple[int, bytes, bytes]]:
        """Yield (type, key, value) as records are decoded.

        Batches are expanded, so a caller sees operations, not frames.
        'state' collects the outcome: 'problem' and 'valid_bytes' are only
        final once the iterator is exhausted, and recovery is the caller
        that does exhaust it.  Recovery applies records as they arrive, so
        it never materialises a whole log -- a 20 000-record WAL used to
        become 20 000 tuples before the first one was applied.
        """
        if state is None:
            state = ReplayResult()
        try:
            state.file_bytes = os.path.getsize(path)
        except OSError:
            return
        if state.file_bytes == 0:
            return

        buffer = bytearray()
        consumed = 0
        problem: str | None = None

        with open(path, "rb") as handle:
            while True:
                chunk = handle.read(chunk_bytes)
                if not chunk:
                    break
                buffer += chunk
                limit = len(buffer)
                offset = 0
                broken = False
                while True:
                    try:
                        rec_type, key, value, next_offset = decode_wal_record(buffer, offset, limit)
                    except IncompleteRecordError:
                        break
                    except CorruptRecordError:
                        broken = True
                        break
                    if rec_type == WAL_TYPE_BATCH:
                        try:
                            operations = decode_batch(value)
                        except CorruptRecordError:
                            broken = True
                            break
                        for operation in operations:
                            state.records += 1
                            yield operation
                    elif rec_type in (WAL_TYPE_PUT, WAL_TYPE_DELETE):
                        state.records += 1
                        yield rec_type, key, value
                    else:
                        broken = True
                        break
                    offset = next_offset
                del buffer[:offset]
                consumed += offset
                if broken:
                    problem = "corrupt"
                    break

        if problem is None and buffer:
            problem = "torn"
        state.problem = problem
        state.valid_bytes = consumed

    @staticmethod
    def replay(path: str, chunk_bytes: int = DEFAULT_REPLAY_CHUNK) -> ReplayResult:
        """Materialise every complete record of a WAL file.

        Kept for callers that want the whole list -- verification and tests.
        Recovery uses iter_replay() and applies records as they are decoded.
        """
        result = ReplayResult()
        for operation in WAL.iter_replay(path, result, chunk_bytes):
            result.entries.append(operation)
        return result

    @staticmethod
    def truncate_to(path: str, length: int) -> None:
        """Cut a WAL file back to the last verified byte."""
        with open(path, "r+b") as handle:
            handle.truncate(length)
            handle.flush()
            os.fsync(handle.fileno())


class IntervalSyncer:
    """Background fsync timer for sync='interval'.

    Takes a callable rather than a WAL because the store rotates its WAL
    when the memtable fills; the timer must follow the active generation.
    """

    def __init__(self, sync_fn, interval_ms: int = 100) -> None:
        self._sync = sync_fn
        self._interval = max(interval_ms, 1) / 1000.0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="minikv-walsync", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._sync()
            except Exception:  # pragma: no cover - daemon thread must not raise
                return

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5.0)


def warn_corruption(path: str, event: str, valid_bytes: int, file_bytes: int) -> None:
    """Emit the one warning the recovery contract promises."""
    warnings.warn(
        f"minikv: {event} record in {os.path.basename(path)}; "
        f"kept {valid_bytes} of {file_bytes} bytes and truncated the rest",
        RuntimeWarning,
        stacklevel=2,
    )
