"""This file proves the on-disk byte layout is exactly what the docs claim,
and that a damaged record can never decode as a valid one."""

from __future__ import annotations

import struct

import pytest

from minikv import encoding
from minikv.encoding import (
    MAX_KEY_BYTES,
    MAX_VALUE_BYTES,
    SST_FOOTER,
    SST_MAGIC,
    WAL_TYPE_DELETE,
    WAL_TYPE_PUT,
    crc32,
    decode_batch,
    decode_sst_record,
    decode_wal_record,
    encode_batch,
    encode_sst_record,
    encode_wal_record,
    validate_key,
    validate_value,
)
from minikv.errors import CorruptRecordError, IncompleteRecordError


def test_wal_layout_is_little_endian_and_ordered():
    record = encode_wal_record(WAL_TYPE_PUT, b"k", b"v")
    assert record[4] == WAL_TYPE_PUT
    assert record[5:9] == struct.pack("<I", 1)
    assert record[9:13] == struct.pack("<I", 1)
    assert record[13:] == b"kv"
    assert struct.unpack("<I", record[0:4])[0] == crc32(record[4:])


def test_wal_roundtrip_put_and_delete():
    for rec_type, key, value in (
        (WAL_TYPE_PUT, b"alpha", b"1"),
        (WAL_TYPE_PUT, b"", b""),
        (WAL_TYPE_DELETE, b"alpha", b""),
    ):
        payload = encode_wal_record(rec_type, key, value)
        got_type, got_key, got_value, offset = decode_wal_record(payload, 0)
        assert (got_type, got_key, got_value) == (rec_type, key, value)
        assert offset == len(payload)


def test_wal_crc_detects_a_single_flipped_bit():
    payload = bytearray(encode_wal_record(WAL_TYPE_PUT, b"key", b"value"))
    payload[-1] ^= 0x01
    with pytest.raises(CorruptRecordError):
        decode_wal_record(bytes(payload), 0)


def test_wal_truncated_header_is_incomplete_not_corrupt():
    payload = encode_wal_record(WAL_TYPE_PUT, b"key", b"value")
    with pytest.raises(IncompleteRecordError):
        decode_wal_record(payload[:6], 0)


def test_wal_truncated_body_is_incomplete():
    payload = encode_wal_record(WAL_TYPE_PUT, b"key", b"value")
    with pytest.raises(IncompleteRecordError):
        decode_wal_record(payload[:-2], 0)


def test_batch_roundtrip_preserves_order_and_types():
    ops = [
        (WAL_TYPE_PUT, b"a", b"1"),
        (WAL_TYPE_DELETE, b"b", b""),
        (WAL_TYPE_PUT, b"c", b""),
    ]
    assert decode_batch(encode_batch(ops)) == ops


def test_batch_truncated_payload_raises():
    payload = encode_batch([(WAL_TYPE_PUT, b"a", b"1")])
    with pytest.raises(CorruptRecordError):
        decode_batch(payload[:-1])


def test_sst_record_roundtrip_including_tombstone_and_empty_value():
    for key, value, tombstone in (
        (b"k", b"value", False),
        (b"k", b"", False),
        (b"k", b"", True),
    ):
        encoded = encode_sst_record(key, value, tombstone)
        got_key, got_value, got_tomb, offset = decode_sst_record(encoded, 0, len(encoded))
        assert (got_key, got_value, got_tomb) == (key, value, tombstone)
        assert offset == len(encoded)


def test_sst_record_truncation_raises():
    encoded = encode_sst_record(b"key", b"value", False)
    with pytest.raises(CorruptRecordError):
        decode_sst_record(encoded, 0, len(encoded) - 1)


def test_footer_is_56_bytes_and_magic_is_eight():
    assert SST_FOOTER.size == 56
    assert len(SST_MAGIC) == 8
    packed = SST_FOOTER.pack(1, 2, 3, 4, 5, 6, 7, SST_MAGIC)
    assert len(packed) == 56
    assert packed.endswith(SST_MAGIC)


def test_crc32_is_always_positive():
    assert crc32(b"") == 0
    assert crc32(b"x") > 0
    assert crc32(b"x" * 1000) <= 0xFFFFFFFF


def test_oversized_key_and_value_are_rejected():
    with pytest.raises(ValueError):
        validate_key(b"k" * (MAX_KEY_BYTES + 1))
    with pytest.raises(ValueError):
        validate_value(b"v" * (MAX_VALUE_BYTES + 1))
    validate_key(b"k" * MAX_KEY_BYTES)
    validate_value(b"v" * MAX_VALUE_BYTES)


def test_uint32_lengths_are_still_checked():
    with pytest.raises(ValueError):
        encoding._check_u32(1 << 33, "key")
