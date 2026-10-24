"""This file proves the WAL replays exactly the records that were written,
stops at the first torn or corrupt record instead of guessing, and can be
cut back to the last verified byte."""

from __future__ import annotations

import os

import pytest

from minikv.encoding import WAL_TYPE_DELETE, WAL_TYPE_PUT
from minikv.wal import WAL


def test_append_and_replay_roundtrip(tmp_path):
    path = str(tmp_path / "wal-000001.log")
    wal = WAL(path, sync="always")
    wal.append_put(b"a", b"1")
    wal.append_put(b"b", b"2")
    wal.append_delete(b"a")
    wal.close()

    result = WAL.replay(path)
    assert result.problem is None
    assert result.entries == [
        (WAL_TYPE_PUT, b"a", b"1"),
        (WAL_TYPE_PUT, b"b", b"2"),
        (WAL_TYPE_DELETE, b"a", b""),
    ]
    assert result.valid_bytes == result.file_bytes


def test_replay_of_missing_file_is_empty(tmp_path):
    result = WAL.replay(str(tmp_path / "nope.log"))
    assert result.entries == []
    assert result.file_bytes == 0


def test_torn_tail_is_reported_and_measured(tmp_path):
    path = str(tmp_path / "wal-000001.log")
    wal = WAL(path, sync="never")
    wal.append_put(b"good", b"value")
    wal.close()
    good_size = os.path.getsize(path)
    with open(path, "ab") as handle:
        handle.write(b"\x00\x01\x02")

    result = WAL.replay(path)
    assert result.problem == "torn"
    assert result.valid_bytes == good_size
    assert result.file_bytes == good_size + 3
    assert result.entries == [(WAL_TYPE_PUT, b"good", b"value")]


def test_corrupt_crc_stops_replay_at_that_record(tmp_path):
    path = str(tmp_path / "wal-000001.log")
    wal = WAL(path, sync="never")
    wal.append_put(b"first", b"1")
    first_size = os.path.getsize(path)
    wal.append_put(b"second", b"2")
    wal.close()

    # Flip a byte inside the second record's value: its lengths stay valid,
    # so the failure is unambiguously a CRC mismatch ("corrupt") rather than
    # a short tail ("torn").  A damaged *length* field cannot be told apart
    # from a torn append; docs/crash-consistency.md spells that out.
    with open(path, "r+b") as handle:
        handle.seek(first_size + 13 + len(b"second"))
        byte = handle.read(1)
        handle.seek(first_size + 13 + len(b"second"))
        handle.write(bytes([byte[0] ^ 0xFF]))

    result = WAL.replay(path)
    assert result.problem == "corrupt"
    assert result.entries == [(WAL_TYPE_PUT, b"first", b"1")]
    assert result.valid_bytes == first_size


def test_batch_record_replays_as_individual_operations(tmp_path):
    path = str(tmp_path / "wal-000001.log")
    wal = WAL(path, sync="never")
    wal.append_batch([(WAL_TYPE_PUT, b"a", b"1"), (WAL_TYPE_DELETE, b"b", b"")])
    wal.close()

    result = WAL.replay(path)
    assert result.entries == [(WAL_TYPE_PUT, b"a", b"1"), (WAL_TYPE_DELETE, b"b", b"")]


def test_truncate_to_drops_the_torn_tail(tmp_path):
    path = str(tmp_path / "wal-000001.log")
    wal = WAL(path, sync="never")
    wal.append_put(b"keep", b"me")
    wal.close()
    good_size = os.path.getsize(path)
    with open(path, "ab") as handle:
        handle.write(b"garbage")

    WAL.truncate_to(path, good_size)
    assert os.path.getsize(path) == good_size
    assert WAL.replay(path).entries == [(WAL_TYPE_PUT, b"keep", b"me")]


def test_close_is_idempotent(tmp_path):
    wal = WAL(str(tmp_path / "wal-000001.log"), sync="never")
    wal.close()
    wal.close()


def test_invalid_sync_mode_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        WAL(str(tmp_path / "wal-000001.log"), sync="sometimes")
