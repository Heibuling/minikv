"""This file proves that damage on disk is either repaired by truncation
(with a warning) or reported as an error -- never silently returned as
data."""

from __future__ import annotations

import os
import random
import warnings
from pathlib import Path

import pytest

from minikv import CorruptRecordError, Store
from minikv.encoding import SST_FOOTER_SIZE
from minikv.wal import WAL


def make_store_with_segment(db_path, count=60):
    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for i in range(count):
        instance.put(f"k{i:04d}".encode(), f"v{i}".encode())
    instance.close()
    segments = [n for n in os.listdir(db_path) if n.endswith(".sst")]
    assert segments
    return Path(db_path) / segments[0]


def test_torn_wal_tail_is_truncated_with_a_warning(db_path):
    instance = Store(db_path, sync="never")
    instance.put(b"a", b"1")
    instance.close()
    wal_name = next(n for n in os.listdir(db_path) if n.endswith(".log"))
    full = Path(db_path) / wal_name
    good = full.stat().st_size
    with full.open("ab") as handle:
        handle.write(b"\x99\x88\x77")

    with pytest.warns(RuntimeWarning):
        reopened = Store(db_path, sync="never")
    assert reopened.get(b"a") == b"1"
    assert reopened.stats()["corruption_events"] == 1
    assert full.stat().st_size == good
    reopened.close()


def test_corrupt_wal_record_stops_replay_with_a_warning(db_path):
    instance = Store(db_path, sync="never")
    instance.put(b"first", b"1")
    instance.put(b"second", b"2")
    instance.close()
    wal_name = next(n for n in os.listdir(db_path) if n.endswith(".log"))
    full = Path(db_path) / wal_name
    with full.open("r+b") as handle:
        handle.seek(10)
        byte = handle.read(1)
        handle.seek(10)
        handle.write(bytes([byte[0] ^ 0xFF]))

    with pytest.warns(RuntimeWarning):
        reopened = Store(db_path, sync="never")
    assert reopened.get(b"first") is None
    assert reopened.get(b"second") is None
    assert reopened.stats()["corruption_events"] == 1
    reopened.close()


def test_damaged_data_block_raises_instead_of_returning_bytes(db_path):
    segment = make_store_with_segment(db_path)
    data = bytearray(segment.read_bytes())
    data[8] ^= 0xFF
    segment.write_bytes(bytes(data))

    instance = Store(db_path, sync="never")
    with pytest.raises(CorruptRecordError):
        instance.get(b"k0000")
    instance.close()


def test_damaged_footer_is_rejected_at_open(db_path):
    segment = make_store_with_segment(db_path)
    data = bytearray(segment.read_bytes())
    data[-8:] = b"BROKEN!!"
    segment.write_bytes(bytes(data))
    with pytest.raises(CorruptRecordError):
        Store(db_path, sync="never")


def test_truncated_segment_is_rejected_at_open(db_path):
    segment = make_store_with_segment(db_path)
    data = segment.read_bytes()
    segment.write_bytes(data[: SST_FOOTER_SIZE - 4])
    with pytest.raises(CorruptRecordError):
        Store(db_path, sync="never")


def test_wal_replay_never_invents_records(tmp_path):
    path = tmp_path / "wal-000001.log"
    wal = WAL(str(path), sync="never")
    wal.append_put(b"a", b"1")
    wal.close()
    original = path.read_bytes()
    random.seed(99)
    for _ in range(40):
        data = bytearray(original)
        position = random.randrange(len(data))
        data[position] ^= 1 << random.randrange(8)
        path.write_bytes(bytes(data))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = WAL.replay(str(path))
        for _rec_type, key, value in result.entries:
            if key == b"a":
                assert value == b"1"
        path.write_bytes(original)


def test_random_segment_damage_never_changes_a_value(db_path):
    """Across many damaged copies, reads are either correct or raise."""
    segment = make_store_with_segment(db_path, count=80)
    original = segment.read_bytes()
    expected = {f"k{i:04d}".encode(): f"v{i}".encode() for i in range(80)}
    random.seed(4242)
    for _ in range(25):
        data = bytearray(original)
        position = random.randrange(len(data))
        data[position] ^= 1 << random.randrange(8)
        segment.write_bytes(bytes(data))
        try:
            instance = Store(db_path, sync="never")
        except CorruptRecordError:
            segment.write_bytes(original)
            continue
        try:
            for key, value in expected.items():
                try:
                    got = instance.get(key)
                except CorruptRecordError:
                    break
                assert got == value, f"silent corruption at byte {position}"
        finally:
            instance.close()
            segment.write_bytes(original)
