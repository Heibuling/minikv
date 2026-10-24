"""This file proves a full k-way merge keeps the newest value per key,
drops tombstones when nothing older survives, and leaves the store
readable after compaction."""

from __future__ import annotations

import os

import pytest

from minikv import ClosedStoreError, Store
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


def test_scan_started_before_compaction_keeps_reading(db_path):
    """Invariant I5: a snapshot taken before compaction stays readable.

    Compaction retires its input files instead of unlinking them, precisely
    so an iterator that is already walking them cannot fail.
    """
    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for i in range(400):
        instance.put(f"k{i:04d}".encode(), b"v")
    expected = [f"k{i:04d}".encode() for i in range(400)]

    rows = instance.scan(b"", None)
    seen = [next(rows)[0] for _ in range(5)]

    result = instance.compact()
    assert result["segments_after"] == 1
    assert instance.stats()["deferred_deletes"] >= 1

    seen.extend(key for key, _ in rows)
    assert seen == expected
    # Exhausting the scan releases its lease, so the retired files go.
    assert instance.stats()["deferred_deletes"] == 0
    instance.close()


def test_a_held_snapshot_defers_file_deletion(db_path):
    """A live snapshot must keep the segment files compaction retires."""
    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for i in range(200):
        instance.put(f"k{i:04d}".encode(), b"v")
    before = {name for name in os.listdir(db_path) if name.endswith(".sst")}
    assert len(before) >= 2

    with instance.snapshot() as view:
        assert instance.stats()["live_snapshots"] == 1
        instance.compact()
        during = {name for name in os.listdir(db_path) if name.endswith(".sst")}
        assert before <= during, "a held snapshot must keep the inputs on disk"
        assert instance.stats()["deferred_deletes"] >= 1
        assert view.get(b"k0000") == b"v"

    # Releasing the last snapshot reclaims immediately, not at close().
    after = [name for name in os.listdir(db_path) if name.endswith(".sst")]
    assert len(after) == 1, f"retired files should be gone, found {after}"
    assert instance.stats()["deferred_deletes"] == 0
    instance.close()


def test_explicit_snapshot_survives_compaction(db_path):
    """A snapshot guarantees file lifetime, not isolation.

    Its segment list is fixed, so compaction cannot take its files away.
    The memtable is shared and still being written, which is why a
    snapshot may see a write that happened after it was taken -- the
    documented consequence of one writer, many readers (DESIGN.md section
    6).  What it must never do is lose the data it was created over.
    """
    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for i in range(300):
        instance.put(f"k{i:04d}".encode(), b"old")

    with instance.snapshot() as view:
        segments_before = len(view.segments)
        instance.compact()
        assert instance.stats()["deferred_deletes"] >= 1
        assert len(view.segments) == segments_before, "the view must not change"
        assert view.get(b"k0299") == b"old"
        assert [key for key, _ in view.scan(b"k0000", b"k0004")] == [
            b"k0000",
            b"k0001",
            b"k0002",
            b"k0003",
            b"k0004",
        ]

    # Releasing it lets the retired files go, without closing the store.
    assert instance.stats()["deferred_deletes"] == 0
    assert instance.get(b"k0299") == b"old"
    instance.close()


def test_released_snapshot_rejects_further_reads(db_path):
    instance = Store(db_path, sync="never")
    instance.put(b"a", b"1")
    view = instance.snapshot()
    view.close()
    assert view.released is True
    view.close()  # idempotent
    with pytest.raises(ClosedStoreError):
        view.get(b"a")
    with pytest.raises(ClosedStoreError):
        view.scan()
    instance.close()


def test_flush_forces_a_segment(db_path):
    instance = Store(db_path, sync="never", memtable_bytes=1 << 20)
    instance.put(b"a", b"1")
    assert instance.stats()["segments"] == 0
    instance.flush()
    assert instance.stats()["segments"] == 1
    instance.flush()  # empty memtable: a no-op
    assert instance.stats()["segments"] == 1
    assert instance.get(b"a") == b"1"
    instance.close()


def test_segments_flushed_during_compaction_are_kept(db_path, monkeypatch):
    """Compaction merges without the writer lock, so a flush can land mid-merge."""
    import minikv.engine as engine_module

    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for i in range(200):
        instance.put(f"k{i:04d}".encode(), b"v")
    assert instance.stats()["segments"] >= 2

    real_merge = engine_module.merge_segments

    def merge_then_flush(readers, output, block_bytes):
        # a writer arrives while the merge is running
        instance.put(b"during-merge", b"kept")
        return real_merge(readers, output, block_bytes)

    monkeypatch.setattr(engine_module, "merge_segments", merge_then_flush)
    result = instance.compact()
    monkeypatch.undo()

    assert result["segments_after"] >= 1
    assert instance.get(b"during-merge") == b"kept"
    assert instance.get(b"k0000") == b"v"
    assert instance.get(b"k0199") == b"v"
    instance.close()


def test_repeated_compaction_does_not_leak_files(db_path):
    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for cycle in range(4):
        for i in range(150):
            instance.put(f"c{cycle}-k{i:04d}".encode(), b"v" * 20)
        instance.compact()
    instance.close()

    remaining = [name for name in os.listdir(db_path) if name.endswith(".sst")]
    assert len(remaining) == 1, f"expected one segment, found {remaining}"

    reopened = Store(db_path, sync="never")
    assert reopened.stats()["segments"] == 1
    assert reopened.get(b"c3-k0000") == b"v" * 20
    assert reopened.get(b"c0-k0149") == b"v" * 20
    reopened.close()
