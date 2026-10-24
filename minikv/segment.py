"""Immutable sorted segments (the SSTables of this engine).

File layout
-----------

    [data block 0][data block 1] ... [sparse index][bloom][footer]

A data block is a concatenation of records, around 4 KiB each.  The
block's CRC lives in the index entry rather than inline, so the writer can
emit a block without patching bytes afterwards.

The sparse index starts with the segment's key range and then holds one
entry per block -- (first key, offset, length, crc).  The range lets a
read skip a whole segment with two comparisons; the entries let it jump to
the block that could contain a key and verify that block before decoding.

The index and the bloom filter are small and verified eagerly when the
file is opened; data blocks are verified lazily by the read that touches
them.  That trade-off is documented in DESIGN.md.
"""

from __future__ import annotations

import bisect
import contextlib
import os
import struct
import threading
from collections import OrderedDict
from collections.abc import Iterator
from typing import BinaryIO

from .encoding import (
    BLOOM_HEAD,
    SST_FOOTER,
    SST_FOOTER_SIZE,
    SST_INDEX_HEAD,
    SST_INDEX_HEADER,
    SST_INDEX_TAIL,
    SST_MAGIC,
    SST_RECORD_HEAD,
    crc32,
    decode_sst_record,
    encode_sst_record,
)
from .errors import ClosedStoreError, CorruptRecordError
from .faults import fault_point
from .memtable import TOMBSTONE, Value, _Tombstone

DEFAULT_BLOCK_BYTES = 4096
DEFAULT_BITS_PER_KEY = 10

IndexEntry = tuple[bytes, int, int, int]  # first_key, offset, length, crc

#: A decoded data block: its keys, and its (key, value) entries.  Cached so a
#: second lookup in the same block is a bisect rather than a decode.
DecodedBlock = tuple[tuple[bytes, ...], tuple[tuple[bytes, Value], ...]]
CacheEntry = tuple[tuple[bytes, ...], tuple[tuple[bytes, Value], ...], int]
CacheKey = tuple[str, int]


def fsync_dir(path: str) -> bool:
    """Flush a directory entry to stable storage; False when impossible.

    POSIX needs this so a rename survives a machine crash.  Windows has no
    directory handle to fsync, so the rename is only as durable as NTFS's
    own journal -- a documented degradation, not something this code can
    paper over.
    """
    if os.name == "nt":
        return False
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return False
    try:
        os.fsync(fd)
        return True
    except OSError:
        return False
    finally:
        os.close(fd)


class BlockCache:
    """Bounded LRU over decoded, CRC-verified data blocks.

    Blocks are immutable, so an entry never needs invalidating: a segment
    that compaction retires simply ages out.  Only bytes that already
    passed their CRC are ever decoded into the cache, so this is a cache of
    *verified* data, not a substitute for verification -- unlike an mmap
    that trusts the page cache.

    A cached block is stored as a tuple of keys plus a tuple of entries, so
    a point read inside it is a bisect instead of a linear decode.
    """

    def __init__(self, max_bytes: int = 0) -> None:
        self.max_bytes = max(0, int(max_bytes))
        self._lock = threading.Lock()
        self._entries: OrderedDict[CacheKey, CacheEntry] = OrderedDict()
        self._used = 0

    @property
    def enabled(self) -> bool:
        return self.max_bytes > 0

    def get(self, cache_key: CacheKey) -> CacheEntry | None:
        if not self.enabled:
            return None
        with self._lock:
            entry = self._entries.get(cache_key)
            if entry is not None:
                self._entries.move_to_end(cache_key)
            return entry

    def put(
        self,
        cache_key: CacheKey,
        keys: tuple[bytes, ...],
        entries: tuple[tuple[bytes, Value], ...],
        size: int,
    ) -> None:
        if not self.enabled:
            return
        with self._lock:
            if cache_key in self._entries:
                self._entries.move_to_end(cache_key)
                return
            self._entries[cache_key] = (keys, entries, size)
            self._used += size
            while self._used > self.max_bytes and len(self._entries) > 1:
                _, (_, _, evicted) = self._entries.popitem(last=False)
                self._used -= evicted

    @property
    def used_bytes(self) -> int:
        with self._lock:
            return self._used

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


class _Bloom:
    """Fixed-size bloom filter, double hashing over CRC32."""

    __slots__ = ("bit_size", "bits", "num_hashes")

    def __init__(self, bit_size: int, num_hashes: int, bits: bytearray | None = None) -> None:
        self.bit_size = bit_size
        self.num_hashes = num_hashes
        self.bits = bytearray((bit_size + 7) // 8) if bits is None else bits

    @classmethod
    def for_capacity(cls, expected: int, bits_per_key: int = DEFAULT_BITS_PER_KEY) -> _Bloom:
        bits = max(expected, 1) * bits_per_key
        bits = max(64, (bits + 7) // 8 * 8)
        hashes = max(1, min(30, round(bits_per_key * 0.6931)))
        return cls(bits, hashes)

    def _hash_pair(self, key: bytes) -> tuple[int, int]:
        """Two independent probes; the rest follow from double hashing.

        Kept as plain arithmetic rather than a generator: profiling the
        flush path showed ~157k generator resumes for 20k keys, which was
        a visible share of the write cost.
        """
        return crc32(key), crc32(key, 0x9E3779B1) | 1

    def add(self, key: bytes) -> None:
        bits = self.bits
        size = self.bit_size
        hash1, hash2 = self._hash_pair(key)
        probe = hash1
        for _ in range(self.num_hashes):
            position = probe % size
            bits[position >> 3] |= 1 << (position & 7)
            probe += hash2

    def might_contain(self, key: bytes) -> bool:
        bits = self.bits
        size = self.bit_size
        hash1, hash2 = self._hash_pair(key)
        probe = hash1
        for _ in range(self.num_hashes):
            position = probe % size
            if not bits[position >> 3] & 1 << (position & 7):
                return False
            probe += hash2
        return True

    def to_bytes(self) -> bytes:
        return BLOOM_HEAD.pack(self.bit_size, self.num_hashes, len(self.bits)) + bytes(self.bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> _Bloom:
        if len(data) < BLOOM_HEAD.size:
            raise CorruptRecordError("truncated bloom filter")
        bit_size, num_hashes, nbytes = BLOOM_HEAD.unpack_from(data, 0)
        if BLOOM_HEAD.size + nbytes != len(data):
            raise CorruptRecordError("bloom filter length mismatch")
        return cls(bit_size, num_hashes, bytearray(data[BLOOM_HEAD.size:]))


def write_segment(
    path: str,
    items: Iterator[tuple[bytes, Value]],
    expected_entries: int,
    block_bytes: int = DEFAULT_BLOCK_BYTES,
    bits_per_key: int = DEFAULT_BITS_PER_KEY,
) -> int:
    """Write 'items' (ascending key order) to 'path'.  Returns entry count.

    The file is built as 'path.tmp', fsynced, then renamed into place, so a
    crash leaves either the old state or a complete new segment -- never a
    half-written .sst that a future open would pick up.
    """
    tmp_path = path + ".tmp"
    bloom = _Bloom.for_capacity(max(expected_entries, 1), bits_per_key)
    index: list[IndexEntry] = []
    buffer = bytearray()
    block_first: bytes | None = None
    block_offset = 0
    count = 0
    first_key: bytes | None = None
    last_key: bytes | None = None

    def flush_buffer() -> None:
        nonlocal buffer, block_first, block_offset
        if not buffer:
            return
        assert block_first is not None
        handle.write(buffer)
        index.append((block_first, block_offset, len(buffer), crc32(buffer)))
        block_offset += len(buffer)
        buffer = bytearray()
        block_first = None

    try:
        with open(tmp_path, "wb") as handle:
            for key, value in items:
                if block_first is None:
                    block_first = key
                if first_key is None:
                    first_key = key
                last_key = key
                # Branched rather than assigned to a flag: isinstance() is what
                # narrows Value to bytes, and a bool would not.
                if isinstance(value, _Tombstone):
                    buffer += encode_sst_record(key, b"", True)
                else:
                    buffer += encode_sst_record(key, value, False)

                bloom.add(key)
                count += 1
                if len(buffer) >= block_bytes:
                    flush_buffer()
            flush_buffer()

            index_offset = block_offset
            index_bytes = bytearray()
            min_key = first_key if first_key is not None else b""
            max_key = last_key if last_key is not None else b""
            index_bytes += SST_INDEX_HEADER.pack(len(min_key), len(max_key))
            index_bytes += min_key
            index_bytes += max_key
            for entry_key, offset, length, crc in index:
                index_bytes += SST_INDEX_HEAD.pack(len(entry_key))
                index_bytes += entry_key
                index_bytes += SST_INDEX_TAIL.pack(offset, length, crc)
            handle.write(index_bytes)

            bloom_offset = index_offset + len(index_bytes)
            bloom_bytes = bloom.to_bytes()
            handle.write(bloom_bytes)

            handle.write(
                SST_FOOTER.pack(
                    index_offset,
                    len(index_bytes),
                    bloom_offset,
                    len(bloom_bytes),
                    crc32(index_bytes),
                    crc32(bloom_bytes),
                    count,
                    SST_MAGIC,
                )
            )
            handle.flush()
            os.fsync(handle.fileno())

        fault_point("segment_tmp_written")
        os.replace(tmp_path, path)
        fsync_dir(os.path.dirname(os.path.abspath(path)))
        fault_point("segment_renamed")
        return count
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise


class SegmentReader:
    """Read-only view of one segment file.

    In the unlikely event the store is closed between a caller's check and
    this object's next read, the read raises ClosedStoreError rather than
    leaking a low-level 'I/O operation on closed file'.
    """

    def __init__(self, path: str, cache: BlockCache | None = None) -> None:
        self.path = path
        self.name = os.path.basename(path)
        self._cache = cache
        size = os.path.getsize(path)
        if size < SST_FOOTER_SIZE:
            raise CorruptRecordError(f"{self.name}: file shorter than a footer")
        with open(path, "rb") as handle:
            handle.seek(size - SST_FOOTER_SIZE)
            footer = handle.read(SST_FOOTER_SIZE)
            try:
                (index_offset, index_len, bloom_offset, bloom_len, index_crc,
                 bloom_crc, num_entries, magic) = SST_FOOTER.unpack(footer)
            except struct.error as exc:  # pragma: no cover - size checked above
                raise CorruptRecordError(f"{self.name}: unreadable footer") from exc
            if magic != SST_MAGIC:
                raise CorruptRecordError(f"{self.name}: bad magic {magic!r}")
            if not (0 <= index_offset <= bloom_offset <= size - SST_FOOTER_SIZE):
                raise CorruptRecordError(f"{self.name}: footer offsets out of range")
            if index_offset + index_len != bloom_offset or bloom_offset + bloom_len != size - SST_FOOTER_SIZE:
                raise CorruptRecordError(f"{self.name}: inconsistent footer lengths")

            handle.seek(index_offset)
            index_bytes = handle.read(index_len)
            if len(index_bytes) != index_len or crc32(index_bytes) != index_crc:
                raise CorruptRecordError(f"{self.name}: index failed CRC")

            handle.seek(bloom_offset)
            bloom_bytes = handle.read(bloom_len)
            if len(bloom_bytes) != bloom_len or crc32(bloom_bytes) != bloom_crc:
                raise CorruptRecordError(f"{self.name}: bloom filter failed CRC")

        self.num_entries = num_entries
        self.data_end = index_offset
        self.size_bytes = size
        self._min_key, self._max_key, self._index = self._parse_index(index_bytes)
        self._keys = [entry[0] for entry in self._index]
        self._bloom = _Bloom.from_bytes(bloom_bytes)
        # One cached handle serves the point-read path: opening a file per
        # get() costs ~90 us on Windows and dominated every read benchmark.
        #: Highest WAL sequence number whose contents this segment holds.
        #: Set by the store when the segment is created or loaded; 0 means
        #: "unknown", which makes recovery replay everything (always safe).
        self.wal_seq = 0
        self._handle: BinaryIO | None = None
        self._io_lock = threading.Lock()
        self._closed = False

    def _parse_index(self, data: bytes) -> tuple[bytes, bytes, list[IndexEntry]]:
        if len(data) < SST_INDEX_HEADER.size:
            raise CorruptRecordError(f"{self.name}: truncated index header")
        min_len, max_len = SST_INDEX_HEADER.unpack_from(data, 0)
        cursor = SST_INDEX_HEADER.size
        if cursor + min_len + max_len > len(data):
            raise CorruptRecordError(f"{self.name}: truncated key range")
        min_key = data[cursor : cursor + min_len]
        cursor += min_len
        max_key = data[cursor : cursor + max_len]
        cursor += max_len

        entries: list[IndexEntry] = []
        while cursor < len(data):
            if cursor + SST_INDEX_HEAD.size > len(data):
                raise CorruptRecordError(f"{self.name}: truncated index")
            (klen,) = SST_INDEX_HEAD.unpack_from(data, cursor)
            start = cursor + SST_INDEX_HEAD.size
            end = start + klen + SST_INDEX_TAIL.size
            if end > len(data):
                raise CorruptRecordError(f"{self.name}: truncated index")
            entry_key = data[start : start + klen]
            block_offset, length, crc = SST_INDEX_TAIL.unpack_from(data, start + klen)
            entries.append((entry_key, block_offset, length, crc))
            cursor = end
        return min_key, max_key, entries

    # -- range -----------------------------------------------------------

    @property
    def key_range(self) -> tuple[bytes, bytes]:
        """(minimum key, maximum key) stored in this segment."""
        return self._min_key, self._max_key

    def overlaps(self, start: bytes, end: bytes | None) -> bool:
        """True when the segment can hold a key in [start, end]."""
        if self.num_entries == 0:
            return False
        if end is not None and self._min_key > end:
            return False
        return not self._max_key < start

    # -- block access ----------------------------------------------------

    def _read_block(self, handle: BinaryIO, position: int) -> bytes:
        _, offset, length, crc = self._index[position]
        handle.seek(offset)
        data = handle.read(length)
        if len(data) != length:
            raise CorruptRecordError(f"{self.name}: block {position} is short")
        if crc32(data) != crc:
            raise CorruptRecordError(f"{self.name}: block {position} failed CRC")
        return data

    def _block_for(self, key: bytes) -> int:
        return max(bisect.bisect_right(self._keys, key) - 1, 0)

    def _cached_handle(self) -> BinaryIO:
        if self._closed:
            raise ClosedStoreError(f"segment {self.name} was closed with its store")
        if self._handle is None:
            self._handle = open(self.path, "rb")  # noqa: SIM115 - closed by close()
        return self._handle

    def _block_entries(self, position: int) -> DecodedBlock:
        """Decoded, verified entries of one block, through the cache."""
        cache_key = (self.path, position)
        if self._cache is not None:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached[0], cached[1]

        keys, entries = self._block_entries_uncached(position)
        if self._cache is not None:
            self._cache.put(cache_key, keys, entries, self._block_size(entries))
        return keys, entries

    @staticmethod
    def _decode_block(data: bytes) -> DecodedBlock:
        """Decode every record of one verified block."""
        decoded: list[tuple[bytes, Value]] = []
        offset = 0
        end = len(data)
        while offset < end:
            record_key, record_value, tombstone, offset = decode_sst_record(data, offset, end)
            if tombstone:
                decoded.append((record_key, TOMBSTONE))
            else:
                decoded.append((record_key, record_value))
        entries: tuple[tuple[bytes, Value], ...] = tuple(decoded)
        return tuple(key for key, _ in entries), entries

    def _block_entries_uncached(self, position: int) -> DecodedBlock:
        """Read and verify a block without consulting the cache.

        Anything that must inspect the bytes on disk -- verify(), fsck --
        goes through here, so a warm cache can never hide damage.
        """
        with self._io_lock:
            data = self._read_block(self._cached_handle(), position)
        return self._decode_block(data)

    # -- public reads ----------------------------------------------------

    def get(self, key: bytes) -> Value | None:
        """Return bytes, TOMBSTONE, or None (definitely absent)."""
        if not self._index or not self.overlaps(key, key):
            return None
        if not self._bloom.might_contain(key):
            return None
        position = self._block_for(key)

        if self._cache is not None:
            cached = self._cache.get((self.path, position))
            if cached is not None:
                keys, entries = cached[0], cached[1]
                found = bisect.bisect_left(keys, key)
                if found < len(keys) and keys[found] == key:
                    return entries[found][1]
                return None

        with self._io_lock:
            data = self._read_block(self._cached_handle(), position)

        if self._cache is not None:
            keys, entries = self._decode_block(data)
            self._cache.put((self.path, position), keys, entries, self._block_size(entries))
            found = bisect.bisect_left(keys, key)
            if found < len(keys) and keys[found] == key:
                return entries[found][1]
            return None

        # No cache: stop as soon as the key is decided.  The record layout
        # puts the key before the value precisely so a miss inside the block
        # never has to copy the values it walks past.
        head = SST_RECORD_HEAD
        offset = 0
        end = len(data)
        while offset < end:
            klen, tombstone, vlen = head.unpack_from(data, offset)
            key_start = offset + head.size
            record_key = data[key_start : key_start + klen]
            if record_key == key:
                if tombstone:
                    return TOMBSTONE
                value_start = key_start + klen
                return data[value_start : value_start + vlen]
            if record_key > key:
                return None
            offset = key_start + klen + vlen
        return None

    @staticmethod
    def _block_size(entries: tuple[tuple[bytes, Value], ...]) -> int:
        return sum(
            len(key) + (0 if isinstance(value, _Tombstone) else len(value)) + 32
            for key, value in entries
        )

    def scan(self, start: bytes, end: bytes | None) -> Iterator[tuple[bytes, Value]]:
        """Yield start <= key <= end in ascending order."""
        if not self.overlaps(start, end):
            return
        position = self._block_for(start)
        while position < len(self._index):
            if end is not None and self._keys[position] > end:
                return
            _keys, entries = self._block_entries(position)
            for key, value in entries:
                if end is not None and key > end:
                    return
                if key >= start:
                    yield key, value
            position += 1

    def scan_backward(self, start: bytes, end: bytes | None) -> Iterator[tuple[bytes, Value]]:
        """Yield start <= key <= end in descending order.

        A block is decoded forward into a small list (bounded by the block
        size), then walked backwards: variable-length records cannot be
        read in reverse without knowing where they start.
        """
        if not self.overlaps(start, end):
            return
        position = len(self._index) - 1 if end is None else self._block_for(end)
        while position >= 0:
            if self._keys[position] > (end if end is not None else self._max_key):
                position -= 1
                continue
            _, entries = self._block_entries(position)
            for key, value in reversed(entries):
                if end is not None and key > end:
                    continue
                if key < start:
                    return
                yield key, value
            position -= 1

    def items(self) -> Iterator[tuple[bytes, Value]]:
        """Yield every entry in ascending key order.

        Deliberately bypasses the block cache: this is a full sequential
        pass (compaction, byte-level verification), where caching every
        block would evict the working set to no benefit.  Routing it through
        the cache measured 2.5x slower on compaction.
        """
        for position in range(len(self._index)):
            _, entries = self._block_entries_uncached(position)
            yield from entries

    def close(self) -> None:
        """Release the cached read handle; the file itself is untouched."""
        with self._io_lock:
            handle, self._handle = self._handle, None
            self._closed = True
        if handle is not None:
            handle.close()

    def verify(self) -> int:
        """Verify every block and every record; returns the entry count.

        Used by tests, by Store.verify() and by the fsck CLI command.
        """
        count = 0
        previous: bytes | None = None
        first: bytes | None = None
        for position in range(len(self._index)):
            _, entries = self._block_entries_uncached(position)
            for key, _ in entries:
                if previous is not None and key <= previous:
                    raise CorruptRecordError(f"{self.name}: keys out of order")
                if first is None:
                    first = key
                previous = key
                count += 1
        if count != self.num_entries:
            raise CorruptRecordError(
                f"{self.name}: footer says {self.num_entries} entries, found {count}"
            )
        if count and (first != self._min_key or previous != self._max_key):
            raise CorruptRecordError(f"{self.name}: index key range does not match the data")
        return count
