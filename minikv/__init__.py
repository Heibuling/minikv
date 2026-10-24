"""miniKV -- a small, auditable LSM-style key-value store.

Public surface:

    Store               the engine
    StoreSnapshot       a read-only view that keeps its files alive
    MiniKVError         base class for every error
    StoreLockedError    the directory is already open elsewhere
    ClosedStoreError    operation on a closed store
    CorruptRecordError  bytes on disk failed verification

Everything else is an implementation detail.  Runtime dependencies: none
(the standard library only).
"""

from __future__ import annotations

from .engine import Store, StoreSnapshot
from .errors import ClosedStoreError, CorruptRecordError, MiniKVError, StoreLockedError

__all__ = [
    "ClosedStoreError",
    "CorruptRecordError",
    "MiniKVError",
    "Store",
    "StoreLockedError",
    "StoreSnapshot",
]
__version__ = "1.5.0"
