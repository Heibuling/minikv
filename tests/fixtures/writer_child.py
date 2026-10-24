"""Crash-injection child process used by tests/test_crash_recovery.py.

Usage:
    writer_child.py <db> <mode> <count> <kill_at> <memtable_bytes> <sync>

mode "deterministic": write keys sequentially and call os._exit(7) right
after the put at index <kill_at> returns -- i.e. after it was acknowledged.

mode "run": write forever; the parent kills the process externally.
"""

from __future__ import annotations

import os
import sys

from minikv import Store


def key_of(index: int) -> bytes:
    return f"k{index:06d}".encode()


def value_of(index: int) -> bytes:
    return f"v{index:06d}".encode()


def main(argv):
    db, mode, count, kill_at, memtable_bytes, sync = argv
    store = Store(db, sync=sync, memtable_bytes=int(memtable_bytes))
    if mode == "deterministic":
        for index in range(int(count)):
            store.put(key_of(index), value_of(index))
            if index == int(kill_at):
                os._exit(7)
    else:
        index = 0
        while True:
            store.put(key_of(index), value_of(index))
            index += 1


if __name__ == "__main__":
    main(sys.argv[1:])
