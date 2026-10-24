"""This file proves the engine agrees with a Python dict over thousands of
random operations, across flushes, reopens and compactions."""

from __future__ import annotations

import os
import random
import shutil
import tempfile

from hypothesis import given, settings
from hypothesis import strategies as st

from minikv import Store
from minikv.encoding import WAL_TYPE_PUT, decode_wal_record, encode_wal_record
from minikv.segment import SegmentReader, write_segment


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


@settings(max_examples=100, deadline=None)
@given(key=st.binary(max_size=64), value=st.binary(max_size=512))
def test_wal_record_round_trips_for_arbitrary_bytes(key, value):
    payload = encode_wal_record(WAL_TYPE_PUT, key, value)
    rec_type, got_key, got_value, offset = decode_wal_record(payload, 0)
    assert (rec_type, got_key, got_value) == (WAL_TYPE_PUT, key, value)
    assert offset == len(payload)


@settings(max_examples=50, deadline=None)
@given(
    st.lists(
        st.tuples(st.binary(min_size=1, max_size=16), st.binary(max_size=64)),
        min_size=1,
        max_size=30,
        unique_by=lambda item: item[0],
    )
)
def test_segment_round_trips_for_arbitrary_sorted_items(items):
    items = sorted(items)
    directory = tempfile.mkdtemp()
    try:
        path = os.path.join(directory, "seg-000001.sst")
        write_segment(path, iter(items), len(items), block_bytes=64)
        reader = SegmentReader(path)
        assert dict(reader.items()) == dict(items)
        assert reader.verify() == len(items)
        low, high = reader.key_range
        assert (low, high) == (items[0][0], items[-1][0])
    finally:
        shutil.rmtree(directory, ignore_errors=True)
