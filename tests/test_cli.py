"""This file proves the command line interface maps onto the API and
reports a missing key with a non-zero exit code."""

from __future__ import annotations

import os
import subprocess
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def cli(db, *args, **kwargs):
    env = dict(os.environ)
    env["PYTHONPATH"] = PROJECT_ROOT
    return subprocess.run(
        [sys.executable, "-m", "minikv", "--db", str(db), *args],
        capture_output=True,
        env=env,
        timeout=120,
        **kwargs,
    )


def test_put_then_get_roundtrip(tmp_path):
    db = tmp_path / "db"
    assert cli(db, "put", "hello", "world").returncode == 0
    result = cli(db, "get", "hello")
    assert result.returncode == 0
    assert result.stdout == b"world\n"


def test_get_missing_key_exits_1(tmp_path):
    result = cli(tmp_path / "db", "get", "absent")
    assert result.returncode == 1
    assert b"not found" in result.stderr


def test_del_removes_the_key(tmp_path):
    db = tmp_path / "db"
    cli(db, "put", "k", "v")
    assert cli(db, "del", "k").returncode == 0
    assert cli(db, "get", "k").returncode == 1


def test_scan_stats_and_compact(tmp_path):
    db = tmp_path / "db"
    for i in range(5):
        cli(db, "put", f"k{i}", f"v{i}")
    scanned = cli(db, "scan", "--start", "k1", "--end", "k3")
    assert scanned.returncode == 0
    assert [line.split(b"\t")[0] for line in scanned.stdout.splitlines()] == [b"k1", b"k2", b"k3"]

    stats = cli(db, "stats")
    assert b"memtable_keys" in stats.stdout

    compact = cli(db, "compact")
    assert compact.returncode == 0
    assert b"merged" in compact.stdout


def test_bench_runs(tmp_path):
    result = cli(tmp_path / "db", "bench", "--count", "20", "--value-size", "8")
    assert result.returncode == 0
    assert b"writes" in result.stdout


def test_bad_usage_exits_2(tmp_path):
    result = cli(tmp_path / "db", "put", "only-a-key")
    assert result.returncode == 2
