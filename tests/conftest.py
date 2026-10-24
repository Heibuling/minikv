"""Shared fixtures.

This module exists so every test file can open a real store in a real
temporary directory; nothing here mocks the filesystem.
"""

from __future__ import annotations

import os
import sys

import pytest

from minikv import Store

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "db")


@pytest.fixture()
def store(db_path):
    instance = Store(db_path, sync="always", memtable_bytes=64 << 10)
    yield instance
    instance.close()


@pytest.fixture()
def small_store(db_path):
    """A store that flushes every few kilobytes, to exercise segments."""
    instance = Store(db_path, sync="never", memtable_bytes=2048)
    yield instance
    instance.close()


def run_child(tmp_path, script, *args, **kwargs):
    """Run a fixture script in a fresh interpreter and return the result."""
    import subprocess

    env = dict(os.environ)
    env["PYTHONPATH"] = PROJECT_ROOT
    env.update(kwargs.pop("env", {}))
    return subprocess.run(
        [sys.executable, os.path.join(FIXTURES, script), *[str(a) for a in args]],
        capture_output=True,
        env=env,
        timeout=120,
        **kwargs,
    )
