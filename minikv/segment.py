"""Immutable sorted segments (the SSTables of this engine).

File layout
-----------

    [data block 0][data block 1] ... [sparse index][bloom][footer]

A data block is a concatenation of records, around 4 KiB each.  The
block's CRC lives in the index entry rather than inline, so the writer
can emit a block without patching bytes afterwards.

The sparse index holds one entry per block -- (first key, offset, length,
crc) -- which is enough to jump to the block that could contain a key and
to verify it before decoding.  The index and the bloom filter are small
and verified eagerly when the file is opened; data blocks are verified
lazily by the read that touches them.  That trade-off is documented in
DESIGN.md.
"""

from __future__ import annotations

import bisect
import contextlib
import os
import struct
import threading
from collections.abc import Iterator

from .encoding import (
    BLOOM_HEAD,
    SST_FOOTER,
    SST_FOOTER_SIZE,
    SST_INDEX_HEAD,
    SST_INDEX_TAIL,
    SST_MAGIC,
    crc32,
    decode_sst_record,
    encode_sst_record,
)
from .errors import CorruptRecordError
from .memtable import TOMBSTONE

DEFAULT_BLOCK_BYTES = 4096
DEFAULT_BITS_PER_KEY = 10

IndexEntry = tuple[bytes, int, int, int]  # first_key, offset, length, crc


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

    def _positions(self, key: bytes) -> Iterator[int]:
        h1 = crc32(key)
        h2 = crc32(key, 0x9E3779B1) | 1
        for i in range(self.num_hashes):
            yield (h1 + i * h2) % self.bit_size

    def add(self, key: bytes) -> None:
        bits = self.bits
        for pos in self._positions(key):
            bits[pos >> 3] |= 1 << (pos & 7)

    def might_contain(self, key: bytes) -> bool:
        bits = self.bits
        return all(bits[pos >> 3] & 1 << (pos & 7) for pos in self._positions(key))

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
    items: Iterator[tuple[bytes, object]],
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

    try:
        with open(tmp_path, "wb") as handle:
            for key, value in items:
                tombstone = value is TOMBSTONE
                if block_first is None:
                    block_first = key
                buffer += encode_sst_record(key, b"" if tombstone else value, tombstone)  # type: ignore[arg-type]
                bloom.add(key)
                count += 1
                if len(buffer) >= block_bytes:
                    assert block_first is not None
                    handle.write(buffer)
                    index.append((block_first, block_offset, len(buffer), crc32(bytes(buffer))))
                    block_offset += len(buffer)
                    buffer.clear()
                    block_first = None

            if buffer:
                assert block_first is not None
                handle.write(buffer)
                index.append((block_first, block_offset, len(buffer), crc32(bytes(buffer))))
                block_offset += len(buffer)
                buffer.clear()

            index_offset = block_offset
            index_bytes = bytearray()
            for first_key, offset, length, crc in index:
                index_bytes += SST_INDEX_HEAD.pack(len(first_key))
                index_bytes += first_key
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
                    crc32(bytes(index_bytes)),
                    crc32(bloom_bytes),
                    count,
                    SST_MAGIC,
                )
            )
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(tmp_path, path)
        fsync_dir(os.path.dirname(os.path.abspath(path)))
        return count
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise


class SegmentReader:
    """Read-only view of one segment file."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.name = os.path.basename(path)
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
        self._index = self._parse_index(index_bytes)
        self._keys = [entry[0] for entry in self._index]
        self._bloom = _Bloom.from_bytes(bloom_bytes)
        # One cached handle serves the point-read path: opening a file per
        # get() costs ~90 us on Windows and dominated every read benchmark.
        self._handle = None
        self._io_lock = threading.Lock()

    def _parse_index(self, data: bytes) -> list[IndexEntry]:
        entries: list[IndexEntry] = []
        offset = 0
        while offset < len(data):
            if offset + SST_INDEX_HEAD.size > len(data):
                raise CorruptRecordError(f"{self.name}: truncated index")
            (klen,) = SST_INDEX_HEAD.unpack_from(data, offset)
            start = offset + SST_INDEX_HEAD.size
            end = start + klen + SST_INDEX_TAIL.size
            if end > len(data):
                raise CorruptRecordError(f"{self.name}: truncated index")
            key = data[start : start + klen]
            block_offset, length, crc = SST_INDEX_TAIL.unpack_from(data, start + klen)
            entries.append((key, block_offset, length, crc))
            offset = end
        return entries

    # -- block access ----------------------------------------------------

    def _read_block(self, handle, position: int) -> bytes:
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

    # -- public reads ----------------------------------------------------

    def get(self, key: bytes) -> object:
        """Return bytes, TOMBSTONE, or None (definitely absent)."""
        if not self._index or not self._bloom.might_contain(key):
            return None
        position = self._block_for(key)
        with self._io_lock:
            if self._handle is None:
                self._handle = open(self.path, "rb")  # noqa: SIM115 - closed by close()
            data = self._read_block(self._handle, position)
        offset = 0
        while offset < len(data):
            rec_key, value, tombstone, offset = decode_sst_record(data, offset, len(data))
            if rec_key == key:
                return TOMBSTONE if tombstone else value
            if rec_key > key:
                return None
        return None

    def scan(self, start: bytes, end: bytes | None) -> Iterator[tuple[bytes, object]]:
        """Yield start <= key <= end in ascending order."""
        if not self._index:
            return
        position = self._block_for(start)
        with open(self.path, "rb") as handle:
            while position < len(self._index):
                if end is not None and self._keys[position] > end:
                    return
                data = self._read_block(handle, position)
                offset = 0
                while offset < len(data):
                    key, value, tombstone, offset = decode_sst_record(data, offset, len(data))
                    if end is not None and key > end:
                        return
                    if key >= start:
                        yield key, TOMBSTONE if tombstone else value
                position += 1

    def items(self) -> Iterator[tuple[bytes, object]]:
        """Yield every entry in ascending key order."""
        with open(self.path, "rb") as handle:
            for position in range(len(self._index)):
                data = self._read_block(handle, position)
                offset = 0
                while offset < len(data):
                    key, value, tombstone, offset = decode_sst_record(data, offset, len(data))
                    yield key, TOMBSTONE if tombstone else value

    def close(self) -> None:
        """Release the cached read handle; the file itself is untouched."""
        with self._io_lock:
            handle, self._handle = self._handle, None
        if handle is not None:
            handle.close()

    def verify(self) -> int:
        """Verify every block and every record; returns the entry count.

        Used by tests and by anyone who wants a full scrub instead of the
        lazy per-read verification the engine performs.
        """
        count = 0
        previous: bytes | None = None
        for key, _ in self.items():
            if previous is not None and key <= previous:
                raise CorruptRecordError(f"{self.name}: keys out of order")
            previous = key
            count += 1
        if count != self.num_entries:
            raise CorruptRecordError(
                f"{self.name}: footer says {self.num_entries} entries, found {count}"
            )
        return count
