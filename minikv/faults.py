"""Deterministic fault injection for crash-consistency tests.

The engine calls fault_point(name) at the moments where a crash would
leave a different on-disk state -- between fsyncing a segment's temporary
file and renaming it, between updating the manifest and deleting the WAL
it replaced, and so on.  Nothing happens unless the environment names that
point, so the cost in normal operation is one os.environ lookup on a path
that only runs during a flush.

    MINIKV_FAULT          comma separated points.  'name' fires on the
                          first call, 'name:N' on the Nth.  Example:
                          'segment_renamed,manifest_after:2'
    MINIKV_FAULT_ACTION   'exit' (default) calls os._exit(9); 'raise'
                          raises InjectedFault instead, so a test can
                          inspect the state in-process.

Point names are validated against a fixed set, because a test that
misspelled one would silently never fire.
"""

from __future__ import annotations

import os
import threading

FAULT_ENV = "MINIKV_FAULT"
ACTION_ENV = "MINIKV_FAULT_ACTION"
EXIT_CODE = 9

#: Every injectable moment, in the order a flush reaches them.
POINTS = (
    "segment_tmp_written",
    "segment_renamed",
    "manifest_before",
    "manifest_after",
    "wal_before_delete",
    "wal_after_delete",
    "compaction_merged",
    "compaction_manifest",
)

_counts: dict[str, int] = {}
_lock = threading.Lock()


class InjectedFault(Exception):
    """Raised at a fault point when the action is 'raise'."""


def unknown_points(spec: str) -> list[str]:
    """Names in 'spec' that are not injectable points (empty when valid)."""
    names = []
    for part in spec.split(","):
        name = part.strip().partition(":")[0]
        if name and name not in POINTS:
            names.append(name)
    return names


def _specs() -> dict[str, int]:
    raw = os.environ.get(FAULT_ENV, "").strip()
    if not raw:
        return {}
    specs: dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        name, _, nth = part.partition(":")
        try:
            specs[name] = int(nth) if nth else 1
        except ValueError:
            continue
    return specs


def fault_point(name: str) -> None:
    """Abort or raise here, if this point is armed."""
    specs = _specs()
    if not specs:
        return
    nth = specs.get(name)
    if nth is None:
        return
    with _lock:
        seen = _counts.get(name, 0) + 1
        _counts[name] = seen
    if seen != nth:
        return
    if os.environ.get(ACTION_ENV, "exit") == "raise":
        raise InjectedFault(f"fault injected at {name}")
    os._exit(EXIT_CODE)


def reset() -> None:
    """Forget call counts (tests that arm faults in-process use this)."""
    with _lock:
        _counts.clear()
