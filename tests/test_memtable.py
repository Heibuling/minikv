"""This file proves the memtable keeps byte order, distinguishes an empty
value from an absent key, and represents deletes as tombstones rather
than dropping them."""

from __future__ import annotations

from minikv.memtable import TOMBSTONE, MemTable


def test_put_get_overwrite():
    table = MemTable()
    table.put(b"a", b"1")
    table.put(b"a", b"2")
    assert table.get(b"a") == b"2"
    assert len(table) == 1


def test_empty_value_is_not_absence():
    table = MemTable()
    table.put(b"a", b"")
    assert table.get(b"a") == b""
    assert table.get(b"missing") is None


def test_delete_leaves_a_tombstone():
    table = MemTable()
    table.put(b"a", b"1")
    table.delete(b"a")
    assert table.get(b"a") is TOMBSTONE
    assert table.get(b"never") is None


def test_sorted_keys_are_byte_ordered():
    table = MemTable()
    for key in (b"b", b"a", b"\xc3\xa9", b"A", b""):
        table.put(key, b"v")
    assert table.sorted_keys() == [b"", b"A", b"a", b"b", b"\xc3\xa9"]


def test_scan_respects_bounds():
    table = MemTable()
    for i in range(10):
        table.put(f"k{i}".encode(), b"v")
    assert [k for k, _ in table.scan(b"k3", b"k6")] == [b"k3", b"k4", b"k5", b"k6"]
    assert [k for k, _ in table.scan(b"k8", None)] == [b"k8", b"k9"]
    assert list(table.scan(b"z", None)) == []


def test_byte_accounting_tracks_overwrites_and_deletes():
    table = MemTable()
    table.put(b"a", b"12345")
    after_put = table.approx_bytes
    table.put(b"a", b"1")
    assert table.approx_bytes < after_put
    table.delete(b"a")
    assert table.approx_bytes > 0


def test_items_sorted_includes_tombstones():
    table = MemTable()
    table.put(b"a", b"1")
    table.delete(b"b")
    assert list(table.items_sorted()) == [(b"a", b"1"), (b"b", TOMBSTONE)]


def test_bulk_load_matches_repeated_puts():
    items = [(b"a", b"1"), (b"b", b"2"), (b"a", b"3"), (b"c", TOMBSTONE)]

    one = MemTable()
    for key, value in items:
        if value is TOMBSTONE:
            one.delete(key)
        else:
            one.put(key, value)

    other = MemTable()
    other.load(iter(items))

    assert list(one.items_sorted()) == list(other.items_sorted())
    assert one.approx_bytes == other.approx_bytes
    assert one.get(b"a") == b"3"
    assert one.get(b"c") is TOMBSTONE


def test_clear_resets_everything():
    table = MemTable()
    table.put(b"a", b"1")
    table.clear()
    assert len(table) == 0
    assert table.approx_bytes == 0
    assert not table
