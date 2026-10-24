"""This file proves a full k-way merge keeps the newest value per key,
drops tombstones when nothing older survives, and leaves the store
readable after compaction."""

from __future__ import annotations

from minikv import Store
from minikv.compaction import merge_segments
from minikv.memtable import TOMBSTONE
from minikv.segment import SegmentReader, write_segment


def build_segment(path, pairs):
    write_segment(str(path), iter(pairs), len(pairs), block_bytes=128)
    return SegmentReader(str(path))


def test_newest_segment_wins(tmp_path):
    older = build_segment(tmp_path / "seg-000001.sst", [(b"a", b"old"), (b"c", b"1")])
    newer = build_segment(tmp_path / "seg-000002.sst", [(b"a", b"new"), (b"b", b"2")])
    output = str(tmp_path / "seg-000003.sst")
    merge_segments([newer, older], output)
    merged = SegmentReader(output)
    assert dict(merged.items()) == {b"a": b"new", b"b": b"2", b"c": b"1"}


def test_tombstones_are_dropped_in_a_full_merge(tmp_path):
    older = build_segment(tmp_path / "seg-000001.sst", [(b"a", b"1"), (b"b", b"2")])
    newer = build_segment(tmp_path / "seg-000002.sst", [(b"a", TOMBSTONE)])
    output = str(tmp_path / "seg-000003.sst")
    count = merge_segments([newer, older], output)
    merged = SegmentReader(output)
    assert count == 1
    assert dict(merged.items()) == {b"b": b"2"}


def test_merge_handles_identical_keys_everywhere(tmp_path):
    one = build_segment(tmp_path / "seg-000001.sst", [(b"k", b"1")])
    two = build_segment(tmp_path / "seg-000002.sst", [(b"k", b"2")])
    three = build_segment(tmp_path / "seg-000003.sst", [(b"k", b"3")])
    output = str(tmp_path / "seg-000004.sst")
    merge_segments([three, two, one], output)
    assert dict(SegmentReader(output).items()) == {b"k": b"3"}


def test_store_compact_reduces_segments_and_keeps_data(db_path):
    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for i in range(300):
        instance.put(f"k{i:04d}".encode(), b"v" * 10)
    before = instance.stats()["segments"]
    assert before >= 2
    result = instance.compact()
    assert result["segments_after"] == 1
    assert instance.stats()["segments"] == 1
    assert [i for i in range(300) if instance.get(f"k{i:04d}".encode()) != b"v" * 10] == []
    instance.close()


def test_store_compact_removes_tombstones_from_disk(db_path):
    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for i in range(200):
        instance.put(f"k{i:04d}".encode(), b"v" * 10)
    for i in range(0, 200, 2):
        instance.delete(f"k{i:04d}".encode())
    # Force the tombstones out of the memtable and into a segment, otherwise
    # compaction has nothing to drop: it only merges segments.
    instance.put(b"filler", b"x" * 4096)
    assert instance.stats()["segments"] >= 2
    instance.compact()
    assert instance.stats()["segment_entries"] == 101  # 100 live keys + filler
    assert instance.get(b"k0000") is None
    instance.close()


def test_compaction_then_reopen_is_consistent(db_path):
    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for i in range(120):
        instance.put(f"k{i:04d}".encode(), str(i).encode())
    instance.delete(b"k0007")
    instance.compact()
    instance.close()

    reopened = Store(db_path, sync="never")
    assert reopened.get(b"k0007") is None
    assert reopened.get(b"k0119") == b"119"
    reopened.close()


def test_compact_on_single_segment_is_a_noop(db_path):
    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for i in range(80):
        instance.put(f"k{i}".encode(), b"v")
    instance.compact()
    result = instance.compact()
    assert result["segments_merged"] == 1
    instance.close()


def test_merge_of_empty_inputs_is_valid(tmp_path):
    left = build_segment(tmp_path / "seg-000001.sst", [])
    right = build_segment(tmp_path / "seg-000002.sst", [])
    output = str(tmp_path / "seg-000003.sst")
    assert merge_segments([right, left], output) == 0
    assert SegmentReader(output).num_entries == 0
