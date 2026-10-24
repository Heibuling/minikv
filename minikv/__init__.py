"""miniKV -- a small, auditable LSM-style key-value store.

Public surface:

    Store               the engine
    MiniKVError         base class for every error
    ClosedStoreError    operation on a closed store
    CorruptRecordError  bytes on disk failed verification

Everything else is an implementation detail.  Runtime dependencies: none
(the standard library only).
"""

from __future__ import annotations

from .engine import Store
from .errors import ClosedStoreError, CorruptRecordError, MiniKVError

__all__ = ["ClosedStoreError", "CorruptRecordError", "MiniKVError", "Store"]
__version__ = "1.0.0"
