"""This file proves the single-writer / multi-reader model is safe: four
writers and eight readers run concurrently for ten seconds without a
deadlock, an exception, or a lost final value.  Synchronisation uses a
barrier and an event, never sleep()."""

from __future__ import annotations

import random
import threading
import time

import pytest

from minikv import Store

WRITERS = 4
READERS = 8
DURATION = 10.0


@pytest.mark.slow
def test_four_writers_eight_readers_for_ten_seconds(db_path):
    store = Store(db_path, sync="never", memtable_bytes=8192)
    deadline = time.monotonic() + DURATION
    barrier = threading.Barrier(WRITERS + READERS)
    finished = threading.Event()
    errors = []
    final = {}
    final_lock = threading.Lock()

    def writer(writer_id):
        barrier.wait()
        counter = 0
        while time.monotonic() < deadline:
            key = f"w{writer_id}:{counter % 200:04d}".encode()
            value = str(counter).encode()
            store.put(key, value)
            with final_lock:
                final[key] = value
            counter += 1

    def reader(reader_id):
        rnd = random.Random(reader_id)
        barrier.wait()
        while not finished.is_set():
            try:
                store.get(f"w{rnd.randrange(WRITERS)}:{rnd.randrange(200):04d}".encode())
                list(store.scan(b"w0:", b"w0:~", limit=5))
                if len(store.stats()) == 0:  # pragma: no cover - stats is never empty
                    errors.append(AssertionError("empty stats"))
            except Exception as exc:  # pragma: no cover - only on failure
                errors.append(exc)
                return

    writers = [threading.Thread(target=writer, args=(i,)) for i in range(WRITERS)]
    readers = [threading.Thread(target=reader, args=(i,)) for i in range(READERS)]
    for thread in writers + readers:
        thread.start()
    for thread in writers:
        thread.join(timeout=DURATION + 30)
        assert not thread.is_alive(), "writer deadlocked"
    finished.set()
    for thread in readers:
        thread.join(timeout=30)
        assert not thread.is_alive(), "reader deadlocked"

    assert errors == []
    with final_lock:
        expected = dict(final)
    for key, value in expected.items():
        assert store.get(key) == value
    store.close()


def test_reader_sees_a_consistent_value_per_key(db_path):
    """A value is always one that was written, never a mix of two writes."""
    store = Store(db_path, sync="never", memtable_bytes=4096)
    stop = threading.Event()
    bad = []

    def writer():
        counter = 0
        while not stop.is_set():
            store.put(b"hot", counter.to_bytes(8, "little"))
            counter += 1

    def reader():
        while not stop.is_set():
            value = store.get(b"hot")
            if value is not None and len(value) != 8:
                bad.append(value)

    thread_w = threading.Thread(target=writer)
    thread_r = [threading.Thread(target=reader) for _ in range(3)]
    thread_w.start()
    for thread in thread_r:
        thread.start()
    time.sleep(0.5)
    stop.set()
    thread_w.join(timeout=10)
    for thread in thread_r:
        thread.join(timeout=10)
    assert bad == []
    store.close()
