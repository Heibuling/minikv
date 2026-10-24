"""The in-memory write buffer.

A memtable maps key -> value for recent writes and holds tombstones for
recent deletes.  Values are plain bytes; a module-level sentinel marks a
deleted key so the tombstone can shadow an older value that still lives
in an SSTable.

The type of a stored entry is Value -- bytes or a tombstone -- and None
means "this key is not here", which is unambiguous because neither a
value nor a tombstone is ever None.  Readers narrow with isinstance; the
engine relies on that narrowing to keep tombstones out of query results.

Ordering
--------

Point lookups hit a dict (O(1)).  Range scans need key order, which is
rebuilt lazily and cached: sorting a bounded (~4 MiB) buffer costs a few
milliseconds and only happens after a *new* key arrives -- overwriting an
existing key does not invalidate the order.

Locking
-------

Writes and cache rebuilds take a short internal mutex, so a reader can
never observe a dict that grows while it is being sorted.  Reads of
individual values are lock-free (CPython guarantees a dict get is atomic
under the GIL).  The engine's writer lock is separate and coarser.
"""

from __future__ import annotations

import bisect
import threading
from collections.abc import Iterable, Iterator
from typing import Union


class _Tombstone:
    """Sentinel for a deleted key; distinct from every bytes value."""

    _instance: _Tombstone | None = None

    def __new__(cls) -> _Tombstone:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "TOMBSTONE"


TOMBSTONE = _Tombstone()

#: What a memtable or a segment can hold for a key.  Union rather than the
#: PEP 604 spelling because this alias is evaluated at run time and the
#: package still supports Python 3.9.
Value = Union[bytes, _Tombstone]


def entry_size(key: bytes, value: Value) -> int:
    """Approximate memory cost of one entry, used for the flush threshold."""
    payload = 0 if isinstance(value, _Tombstone) else len(value)
    return len(key) + payload + 16


class MemTable:
    """Sorted-by-key in-memory buffer with tombstones."""

    def __init__(self) -> None:
        self._data: dict[bytes, Value] = {}
        self._bytes = 0
        self._keys: list[bytes] | None = None
        self._lock = threading.Lock()

    # -- writes ----------------------------------------------------------

    def put(self, key: bytes, value: bytes) -> None:
        with self._lock:
            old = self._data.get(key)
            self._data[key] = value
            if old is None:
                self._bytes += entry_size(key, value)
                self._keys = None
            else:
                self._bytes += entry_size(key, value) - entry_size(key, old)

    def delete(self, key: bytes) -> None:
        with self._lock:
            old = self._data.get(key)
            self._data[key] = TOMBSTONE
            if old is None:
                self._bytes += entry_size(key, TOMBSTONE)
                self._keys = None
            else:
                self._bytes += entry_size(key, TOMBSTONE) - entry_size(key, old)

    def load(self, items: Iterable[tuple[bytes, Value]]) -> None:
        """Insert many entries at once, for recovery only.

        Recovery is single-threaded -- the store holds the directory lock
        and has not published this memtable to a reader yet -- so the
        per-record lock and the incremental size bookkeeping are pure
        overhead.  A 20 000-record replay used to take 20 000 lock
        acquisitions and 40 000 entry_size() calls.
        """
        data = self._data
        added = 0
        for key, value in items:
            old = data.get(key)
            data[key] = value
            payload = 0 if isinstance(value, _Tombstone) else len(value)
            if old is None:
                added += len(key) + payload + 16
            else:
                old_payload = 0 if isinstance(old, _Tombstone) else len(old)
                added += payload - old_payload
        with self._lock:
            self._bytes += added
            self._keys = None

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self._bytes = 0
            self._keys = None

    # -- reads -----------------------------------------------------------

    def get(self, key: bytes) -> Value | None:
        """Return the stored value, a tombstone, or None when absent."""
        return self._data.get(key)

    def __len__(self) -> int:
        return len(self._data)

    def __bool__(self) -> bool:
        return bool(self._data)

    @property
    def approx_bytes(self) -> int:
        return self._bytes

    def sorted_keys(self) -> list[bytes]:
        """Cached ascending key list.  Treat the result as read-only."""
        keys = self._keys
        if keys is None:
            with self._lock:
                keys = self._keys
                if keys is None:
                    keys = sorted(self._data)
                    self._keys = keys
        return keys

    def items_sorted(self) -> Iterator[tuple[bytes, Value]]:
        """Yield (key, value-or-TOMBSTONE) in ascending key order."""
        keys = self.sorted_keys()
        data = self._data
        for key in keys:
            value = data.get(key)
            if value is not None:
                yield key, value

    def scan(self, start: bytes, end: bytes | None) -> Iterator[tuple[bytes, Value]]:
        """Yield entries with start <= key <= end (end=None means unbounded)."""
        keys = self.sorted_keys()
        lo = bisect.bisect_left(keys, start)
        hi = len(keys) if end is None else bisect.bisect_right(keys, end)
        data = self._data
        for index in range(lo, hi):
            key = keys[index]
            value = data.get(key)
            if value is not None:
                yield key, value

    def scan_backward(self, start: bytes, end: bytes | None) -> Iterator[tuple[bytes, Value]]:
        """Yield entries with start <= key <= end, highest key first."""
        keys = self.sorted_keys()
        lo = bisect.bisect_left(keys, start)
        hi = len(keys) if end is None else bisect.bisect_right(keys, end)
        data = self._data
        for index in range(hi - 1, lo - 1, -1):
            key = keys[index]
            value = data.get(key)
            if value is not None:
                yield key, value
