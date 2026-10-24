"""This file proves the headline durability claim: with sync="always", a
write that returned successfully is still there after the process is
killed.  140 deterministic kill points, plus 8 external kills."""

from __future__ import annotations

import os
import random
import subprocess
import sys
import time
import warnings

import pytest

from minikv import Store

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHILD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "writer_child.py")


def spawn(db, mode, count, kill_at, memtable_bytes=4096, sync="always"):
    env = dict(os.environ)
    env["PYTHONPATH"] = PROJECT_ROOT
    return subprocess.Popen(
        [sys.executable, CHILD, str(db), mode, str(count), str(kill_at), str(memtable_bytes), sync],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=env,
    )


def key_of(index):
    return f"k{index:06d}".encode()


def value_of(index):
    return f"v{index:06d}".encode()


@pytest.mark.slow
def test_120_kill_points_preserve_every_acknowledged_write(tmp_path):
    random.seed(20261024)
    kill_points = list(range(60)) * 2
    random.shuffle(kill_points)
    assert len(kill_points) == 120

    for trial, kill_at in enumerate(kill_points):
        db = tmp_path / f"db{trial:03d}"
        process = spawn(db, "deterministic", 80, kill_at)
        _, stderr = process.communicate(timeout=60)
        assert process.returncode == 7, stderr.decode("utf-8", "replace")[-800:]

        store = Store(str(db), sync="always", memtable_bytes=4096)
        missing = [i for i in range(kill_at + 1) if store.get(key_of(i)) != value_of(i)]
        store.close()
        assert missing == [], f"kill point {kill_at} lost {missing[:5]}"


@pytest.mark.slow
def test_kill_points_that_cross_a_memtable_flush(tmp_path):
    """memtable_bytes=1024 forces flushes every few writes."""
    random.seed(7)
    for trial in range(20):
        kill_at = random.randrange(0, 40)
        db = tmp_path / f"flush{trial:02d}"
        process = spawn(db, "deterministic", 80, kill_at, memtable_bytes=1024)
        _, stderr = process.communicate(timeout=60)
        assert process.returncode == 7, stderr.decode("utf-8", "replace")[-800:]

        store = Store(str(db), sync="always", memtable_bytes=1024)
        missing = [i for i in range(kill_at + 1) if store.get(key_of(i)) != value_of(i)]
        store.close()
        assert missing == [], f"kill point {kill_at} lost {missing[:5]}"


@pytest.mark.slow
def test_external_kill_leaves_a_prefix_and_no_corruption(tmp_path):
    random.seed(11)
    for trial in range(8):
        db = tmp_path / f"ext{trial:02d}"
        process = spawn(db, "run", 0, 0, memtable_bytes=1024)
        time.sleep(random.uniform(0.15, 0.5))
        process.kill()
        process.wait(timeout=30)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            store = Store(str(db), sync="always", memtable_bytes=1024)
        present = []
        index = 0
        while store.get(key_of(index)) is not None:
            assert store.get(key_of(index)) == value_of(index)
            present.append(index)
            index += 1
            if index > 200000:  # pragma: no cover - guard against a runaway loop
                break
        # Everything after the first gap must be absent: writes were sequential.
        gaps = [i for i in range(index, index + 50) if store.get(key_of(i)) is not None]
        store.close()
        assert gaps == [], f"keys after the gap: {gaps[:5]}"
