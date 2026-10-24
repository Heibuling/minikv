"""This file proves the engine agrees with a Python dict over thousands of
random operations, across flushes, reopens and compactions."""

from __future__ import annotations

import os
import random

from minikv import Store


def reopen(db_path, store):
    store.close()
    return Store(db_path, sync="never", memtable_bytes=4096)


def test_random_operations_match_a_dict(db_path):
    rnd = random.Random(20261024)
    store = Store(db_path, sync="never", memtable_bytes=4096)
    oracle = {}

    for step in range(10000):
        roll = rnd.random()
        key = f"k{rnd.randrange(300):04d}".encode()
        if roll < 0.65:
            value = bytes(rnd.randrange(0, 60) for _ in range(rnd.randrange(0, 24)))
            store.put(key, value)
            oracle[key] = value
        elif roll < 0.85:
            store.delete(key)
            oracle.pop(key, None)
        else:
            assert store.get(key) == oracle.get(key)

        if step in (2500, 5000, 7500):
            store = reopen(db_path, store)
            for probe_key, probe_value in rnd.sample(list(oracle.items()), min(50, len(oracle))):
                assert store.get(probe_key) == probe_value

    store = reopen(db_path, store)
    for key, value in oracle.items():
        assert store.get(key) == value
    assert dict(store.scan()) == oracle
    store.compact()
    assert dict(store.scan()) == oracle
    store.close()


def test_random_scan_ranges_match_a_dict(db_path):
    random.Random(99)
    store = Store(db_path, sync="never", memtable_bytes=2048)
    oracle = {}
    for i in range(400):
        key = os.urandom(3)
        value = b"v" * (i % 7)
        store.put(key, value)
        oracle[key] = value

    for _ in range(60):
        start = os.urandom(2)
        end = os.urandom(3)
        if start > end:
            start, end = end, start
        expected = sorted((k, v) for k, v in oracle.items() if start <= k <= end)
        assert list(store.scan(start, end)) == expected
    store.close()
