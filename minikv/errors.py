"""Exception types raised by minikv.

The public hierarchy is deliberately small: callers need to know whether
the directory is already open elsewhere, whether the store was used after
close() (a programming error), or whether bytes on disk failed
verification (a data-safety event).
"""

from __future__ import annotations


class MiniKVError(Exception):
    """Base class for every error this package raises."""


class StoreLockedError(MiniKVError):
    """Raised when another Store or process already holds the directory.

    The engine takes an exclusive lock on a LOCK file inside the data
    directory.  Two writers on one directory would interleave WAL appends
    and segment files, so the second opener fails here instead.
    """


class ClosedStoreError(MiniKVError):
    """Raised when an operation touches a Store after close().

    Also raised when a scan generator created before close() is advanced
    afterwards, so a reader never gets a low-level 'I/O operation on
    closed file' instead of a documented error.
    """


class CorruptRecordError(MiniKVError):
    """Raised when checksum, magic, or length validation fails on disk.

    The engine never returns bytes that failed a checksum: it raises this
    instead.  A damaged WAL *tail* is a special case -- it is truncated
    and reported through Store.stats()["corruption_events"] rather than
    raised, because a crash mid-append is expected to leave one.
    """


class IncompleteRecordError(MiniKVError):
    """Internal signal: fewer bytes than a record needs (a torn append).

    Not part of the public contract; the WAL converts it into a
    truncation plus a warning.
    """
