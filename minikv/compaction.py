"""k-way merge of segments into a single new segment.

The store always compacts *every* live segment, so a surviving tombstone
has nothing left to shadow and can be dropped.  (Leveled compaction,
where a tombstone must be kept because older levels still exist, is
explicitly out of scope -- see DESIGN.md.)

Inputs are ordered newest-first: on a key collision the entry with the
lowest index wins.

This module only *writes* the merged file.  Retiring the inputs is the
store's job: it must keep their file handles readable for snapshots that
are still being iterated, and it removes the files on close() or on the
next open().
"""

from __future__ import annotations

import heapq
from collections.abc import Iterator, Sequence

from .memtable import Value, _Tombstone
from .segment import SegmentReader, write_segment


def merge_segments(
    readers: Sequence[SegmentReader],
    output_path: str,
    block_bytes: int = 4096,
) -> int:
    """Merge 'readers' (newest first) into 'output_path'; returns entry count.

    Takes a Sequence so the caller can hand over an immutable tuple -- the
    store's live segment list -- without copying it.
    """
    expected = sum(reader.num_entries for reader in readers)
    return write_segment(output_path, _merged(readers), expected, block_bytes)


def _merged(readers: Sequence[SegmentReader]) -> Iterator[tuple[bytes, Value]]:
    sources = [reader.items() for reader in readers]
    heap: list[tuple[bytes, int, Value]] = []

    def advance(index: int) -> None:
        try:
            key, value = next(sources[index])
        except StopIteration:
            return
        heapq.heappush(heap, (key, index, value))

    for index in range(len(sources)):
        advance(index)

    while heap:
        key = heap[0][0]
        group: list[tuple[int, Value]] = []
        while heap and heap[0][0] == key:
            _, index, value = heapq.heappop(heap)
            group.append((index, value))
            advance(index)
        winner = min(group, key=lambda item: item[0])[1]
        if isinstance(winner, _Tombstone):
            continue
        yield key, winner
