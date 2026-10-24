"""This file proves the engine agrees with a Python dict over thousands of
random operations, across flushes, reopens and compactions."""

from __future__ import annotations

import contextlib
import os
import random
import shutil
import tempfile

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule, run_state_machine_as_test

from minikv import Store
from minikv.encoding import WAL_TYPE_PUT, decode_wal_record, encode_wal_record
from minikv.faults import InjectedFault
from minikv.faults import reset as reset_faults
from minikv.segment import SegmentReader, write_segment

FLUSH_FAULTS = (
    "segment_tmp_written",
    "segment_renamed",
    "manifest_before",
    "manifest_after",
    "wal_before_delete",
    "wal_after_delete",
)
COMPACTION_FAULTS = ("compaction_merged", "compaction_manifest")

#: Fault points the state machine actually reached, so a rule that never
#: gets selected cannot pass unnoticed.
EXERCISED_POINTS: set[str] = set()


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


class StoreStateMachine(RuleBasedStateMachine):
    """Random operations, restarts and injected crashes against a dict.

    The oracle comparison runs after every step, so a divergence is reported
    against the rule that caused it, and hypothesis shrinks the sequence to
    the shortest one that still fails.  Nothing here is mocked: each example
    builds a real store in a temporary directory, flushes real segments and
    reopens from disk.
    """

    def __init__(self):
        super().__init__()
        self._directory = tempfile.mkdtemp()
        self._store = Store(self._directory, sync="never", memtable_bytes=1024, block_cache_bytes=64 << 10)
        self.oracle: dict[bytes, bytes] = {}

    # -- lifecycle -------------------------------------------------------

    def teardown(self):
        with contextlib.suppress(Exception):
            self._store.close()
        shutil.rmtree(self._directory, ignore_errors=True)

    def _reopen(self):
        self._store.close()
        self._store = Store(self._directory, sync="never", memtable_bytes=1024, block_cache_bytes=64 << 10)
        assert dict(self._store.scan()) == self.oracle

    # -- rules -----------------------------------------------------------

    @rule(key=st.binary(min_size=1, max_size=6), value=st.binary(max_size=32))
    def put(self, key, value):
        self._store.put(key, value)
        self.oracle[key] = value

    @rule(key=st.binary(min_size=1, max_size=6))
    def delete(self, key):
        self._store.delete(key)
        self.oracle.pop(key, None)

    @rule(
        pairs=st.lists(
            st.tuples(st.binary(min_size=1, max_size=6), st.binary(max_size=32)),
            min_size=1,
            max_size=5,
        )
    )
    def batch(self, pairs):
        with self._store.batch() as group:
            for key, value in pairs:
                group.put(key, value)
                self.oracle[key] = value

    @rule()
    def flush(self):
        self._store.flush()

    @rule()
    def compact(self):
        if self._store.stats()["segments"] >= 2:
            self._store.compact()

    @rule()
    def reopen(self):
        self._reopen()

    @rule(point=st.sampled_from(FLUSH_FAULTS))
    def crash_during_flush(self, point):
        """Kill the flush protocol at a named step, then recover.

        The store writes with the fault action set to 'raise' so the process
        survives and the test can reopen in-process; the on-disk state left
        behind is the same one a real crash would leave.
        """
        self._wreck(point)

    @rule(point=st.sampled_from(COMPACTION_FAULTS))
    def crash_during_compaction(self, point):
        if self._store.stats()["segments"] >= 2:
            self._wreck(point)

    def _wreck(self, point):
        EXERCISED_POINTS.add(point)
        os.environ["MINIKV_FAULT"] = point
        os.environ["MINIKV_FAULT_ACTION"] = "raise"
        reset_faults()
        try:
            for index in range(40):
                key = f"crash-{point}-{index}".encode()
                value = f"v{index}".encode()
                try:
                    self._store.put(key, value)
                except InjectedFault:
                    # The WAL record was written before the flush it triggered.
                    self.oracle[key] = value
                    break
                self.oracle[key] = value
        finally:
            os.environ.pop("MINIKV_FAULT", None)
            os.environ.pop("MINIKV_FAULT_ACTION", None)
            reset_faults()
        self._reopen()

    # -- checks ----------------------------------------------------------

    @invariant()
    def matches_the_oracle(self):
        assert dict(self._store.scan()) == self.oracle

    @rule(start=st.binary(max_size=4), end=st.binary(max_size=4))
    def reverse_scan_matches_the_oracle(self, start, end):
        if start > end:
            start, end = end, start
        expected = [item for item in sorted(self.oracle.items()) if start <= item[0] <= end]
        assert list(self._store.scan(start, end, reverse=True)) == list(reversed(expected))


STATE_MACHINE_SETTINGS = settings(
    max_examples=40,
    stateful_step_count=20,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
StoreStateMachine.TestCase.settings = STATE_MACHINE_SETTINGS
TestStoreStateMachine = StoreStateMachine.TestCase


def test_the_state_machine_really_injects_crashes():
    """Guard: the crash rules must be selected, not silently skipped."""
    EXERCISED_POINTS.clear()
    run_state_machine_as_test(
        StoreStateMachine,
        settings=settings(
            max_examples=30,
            stateful_step_count=20,
            deadline=None,
            suppress_health_check=[HealthCheck.too_slow],
        ),
    )
    assert EXERCISED_POINTS, "hypothesis never chose a crash rule"
    assert set(FLUSH_FAULTS) | set(COMPACTION_FAULTS) >= EXERCISED_POINTS


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
