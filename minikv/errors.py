"""Exception types raised by minikv.

The public hierarchy is deliberately small: callers only need to know
whether the store was used after close() (a programming error) or
whether bytes on disk failed verification (a data-safety event).
"""

from __future__ import annotations


class MiniKVError(Exception):
    """Base class for every error this package raises."""


class ClosedStoreError(MiniKVError):
    """Raised when an operation touches a Store after close()."""


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
