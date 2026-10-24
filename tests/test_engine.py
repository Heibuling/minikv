"""This file proves the public API behaves as documented: bytes in, bytes
out, explicit tombstones, atomic batches, and a store that survives a
close/reopen cycle."""

from __future__ import annotations

import os

import pytest

from minikv import ClosedStoreError, Store


def test_put_get_delete(store):
    store.put(b"a", b"1")
    assert store.get(b"a") == b"1"
    store.delete(b"a")
    assert store.get(b"a") is None


def test_get_default(store):
    assert store.get(b"missing") is None
    assert store.get(b"missing", b"fallback") == b"fallback"


def test_empty_key_and_empty_value(store):
    store.put(b"", b"")
    assert store.get(b"") == b""
    assert list(store.scan()) == [(b"", b"")]


def test_bytes_like_inputs_are_accepted(store):
    store.put(bytearray(b"mk"), memoryview(b"mv"))
    assert store.get(b"mk") == b"mv"


def test_str_keys_are_rejected(store):
    with pytest.raises(TypeError):
        store.put("text", b"value")  # type: ignore[arg-type]


def test_closed_store_rejects_operations(db_path):
    instance = Store(db_path, sync="never")
    instance.close()
    instance.close()  # idempotent
    with pytest.raises(ClosedStoreError):
        instance.put(b"a", b"1")
    with pytest.raises(ClosedStoreError):
        instance.get(b"a")


def test_context_manager_closes(db_path):
    with Store(db_path, sync="never") as instance:
        instance.put(b"a", b"1")
    with pytest.raises(ClosedStoreError):
        instance.get(b"a")


def test_scan_forward_reverse_and_limit(store):
    for i in range(10):
        store.put(f"k{i}".encode(), str(i).encode())
    assert [k for k, _ in store.scan(b"k2", b"k5")] == [b"k2", b"k3", b"k4", b"k5"]
    assert [k for k, _ in store.scan(b"k2", b"k5", reverse=True)] == [b"k5", b"k4", b"k3", b"k2"]
    assert len(list(store.scan(b"k", None, limit=3))) == 3
    assert [k for k, _ in store.scan(b"k", None, reverse=True, limit=2)] == [b"k9", b"k8"]
    assert list(store.scan(b"zzz")) == []


def test_scan_hides_tombstones(store):
    store.put(b"a", b"1")
    store.put(b"b", b"2")
    store.delete(b"a")
    assert [k for k, _ in store.scan()] == [b"b"]


def test_batch_commits_atomically(store):
    with store.batch() as batch:
        batch.put(b"x", b"1")
        batch.put(b"y", b"2")
        batch.delete(b"x")
    assert store.get(b"x") is None
    assert store.get(b"y") == b"2"


def test_batch_discards_on_exception(store):
    with pytest.raises(RuntimeError), store.batch() as batch:
        batch.put(b"x", b"1")
        raise RuntimeError("abort")
    assert store.get(b"x") is None


def test_delete_of_absent_key_is_harmless(store):
    store.delete(b"never-existed")
    assert store.get(b"never-existed") is None
    assert list(store.scan()) == []


def test_large_value_roundtrip(store):
    payload = os.urandom(256 * 1024)
    store.put(b"big", payload)
    assert store.get(b"big") == payload


def test_flush_creates_segments_and_reopen_restores(db_path):
    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for i in range(200):
        instance.put(f"k{i:04d}".encode(), b"v" * 20)
    assert instance.stats()["segments"] > 0
    instance.close()

    reopened = Store(db_path, sync="never", memtable_bytes=1024)
    missing = [i for i in range(200) if reopened.get(f"k{i:04d}".encode()) != b"v" * 20]
    assert missing == []
    reopened.close()


def test_reopen_after_delete_keeps_the_delete(db_path):
    instance = Store(db_path, sync="never", memtable_bytes=1024)
    for i in range(100):
        instance.put(f"k{i:04d}".encode(), b"v")
    instance.delete(b"k0000")
    instance.close()

    reopened = Store(db_path, sync="never")
    assert reopened.get(b"k0000") is None
    reopened.close()


def test_stats_reports_documented_fields(store):
    store.put(b"a", b"1")
    stats = store.stats()
    for field in (
        "sync", "memtable_bytes", "memtable_keys", "segments",
        "segment_entries", "segment_bytes", "wal_bytes", "corruption_events",
    ):
        assert field in stats


def test_compact_is_safe_when_nothing_to_merge(store):
    store.put(b"a", b"1")
    result = store.compact()
    assert result["segments_merged"] == store.stats()["segments"]
    assert store.get(b"a") == b"1"


def test_invalid_configuration_is_rejected(db_path):
    with pytest.raises(ValueError):
        Store(db_path, sync="often")
    with pytest.raises(ValueError):
        Store(db_path, memtable_bytes=10)
