"""This file proves a segment round-trips through the documented layout,
that the sparse index and bloom filter never skip a live key, and that
damage is detected instead of returned."""

from __future__ import annotations

import os
import random

import pytest

from minikv.encoding import SST_FOOTER_SIZE
from minikv.errors import CorruptRecordError
from minikv.memtable import TOMBSTONE
from minikv.segment import SegmentReader, write_segment


def build(path, pairs, block_bytes=64):
    write_segment(str(path), iter(pairs), len(pairs), block_bytes=block_bytes)
    return SegmentReader(str(path))


def pairs_of(count, prefix=b"k"):
    return [(prefix + f"{i:04d}".encode(), f"v{i}".encode()) for i in range(count)]


def test_roundtrip_and_counts(tmp_path):
    pairs = pairs_of(50)
    reader = build(tmp_path / "seg-000001.sst", pairs)
    assert reader.num_entries == 50
    assert [k for k, _ in reader.items()] == [k for k, _ in pairs]


def test_multiple_blocks_are_written(tmp_path):
    reader = build(tmp_path / "seg-000001.sst", pairs_of(200))
    assert len(reader._index) > 1
    assert reader.verify() == 200


def test_point_lookup_hit_and_miss(tmp_path):
    reader = build(tmp_path / "seg-000001.sst", pairs_of(100))
    assert reader.get(b"k0042") == b"v42"
    assert reader.get(b"k9999") is None
    assert reader.get(b"a") is None


def test_empty_value_and_tombstone_roundtrip(tmp_path):
    reader = build(
        tmp_path / "seg-000001.sst",
        [(b"a", b""), (b"b", TOMBSTONE), (b"c", b"3")],
    )
    assert reader.get(b"a") == b""
    assert reader.get(b"b") is TOMBSTONE
    assert reader.get(b"c") == b"3"


def test_scan_stays_inside_bounds(tmp_path):
    reader = build(tmp_path / "seg-000001.sst", pairs_of(100))
    rows = list(reader.scan(b"k0010", b"k0014"))
    assert [k for k, _ in rows] == [b"k0010", b"k0011", b"k0012", b"k0013", b"k0014"]
    assert list(reader.scan(b"k0100", None)) == []


def test_empty_segment_is_valid(tmp_path):
    reader = build(tmp_path / "seg-000001.sst", [])
    assert reader.num_entries == 0
    assert reader.get(b"anything") is None
    assert list(reader.scan(b"", None)) == []
    assert reader.verify() == 0


def test_bad_magic_is_rejected(tmp_path):
    path = tmp_path / "seg-000001.sst"
    build(path, pairs_of(10))
    data = bytearray(path.read_bytes())
    data[-8:] = b"NOTMINIK"
    path.write_bytes(bytes(data))
    with pytest.raises(CorruptRecordError):
        SegmentReader(str(path))


def test_truncated_file_is_rejected(tmp_path):
    path = tmp_path / "seg-000001.sst"
    build(path, pairs_of(10))
    data = path.read_bytes()
    path.write_bytes(data[: SST_FOOTER_SIZE - 1])
    with pytest.raises(CorruptRecordError):
        SegmentReader(str(path))


def test_corrupt_index_is_rejected_on_open(tmp_path):
    path = tmp_path / "seg-000001.sst"
    reader = build(path, pairs_of(20))
    data = bytearray(path.read_bytes())
    data[reader.data_end + 2] ^= 0xFF
    path.write_bytes(bytes(data))
    with pytest.raises(CorruptRecordError):
        SegmentReader(str(path))


def test_corrupt_bloom_is_rejected_on_open(tmp_path):
    path = tmp_path / "seg-000001.sst"
    build(path, pairs_of(20))
    size = os.path.getsize(path)
    data = bytearray(path.read_bytes())
    data[size - SST_FOOTER_SIZE - 1] ^= 0xFF
    path.write_bytes(bytes(data))
    with pytest.raises(CorruptRecordError):
        SegmentReader(str(path))


def test_corrupt_data_block_raises_on_read(tmp_path):
    path = tmp_path / "seg-000001.sst"
    build(path, pairs_of(50))
    data = bytearray(path.read_bytes())
    data[10] ^= 0xFF
    path.write_bytes(bytes(data))
    reader = SegmentReader(str(path))
    with pytest.raises(CorruptRecordError):
        reader.get(b"k0000")


def test_single_byte_damage_never_yields_wrong_data(tmp_path):
    """The headline guarantee: damage is an error, never a wrong answer."""
    pairs = pairs_of(40)
    path = tmp_path / "seg-000001.sst"
    build(path, pairs)
    original = path.read_bytes()
    expected = dict(pairs)
    random.seed(1234)
    checked = 0
    for _ in range(60):
        data = bytearray(original)
        position = random.randrange(len(data))
        data[position] ^= 1 << random.randrange(8)
        path.write_bytes(bytes(data))
        try:
            reader = SegmentReader(str(path))
        except CorruptRecordError:
            checked += 1
            path.write_bytes(original)
            continue
        for key, _value in pairs:
            try:
                got = reader.get(key)
            except CorruptRecordError:
                break
            assert got == expected[key], f"silent corruption at byte {position} for {key!r}"
        checked += 1
        path.write_bytes(original)
    assert checked == 60
