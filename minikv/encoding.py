"""Byte layouts shared by the write-ahead log and the SSTable format.

Every integer is little-endian, every length is an unsigned 32-bit count,
and every structure that can be damaged on disk carries a CRC32 over
exactly the bytes it protects.  Nothing is pickled: a truncated or
flipped byte must produce a checksum failure, never a plausible-looking
record.

WAL record
----------

    +-----------+--------+---------+---------+-----+---------+
    | crc32 (4) | type(1)| klen(4) | vlen(4) | key | value   |
    +-----------+--------+---------+---------+-----+---------+

'type' is 1 (put), 2 (delete) or 3 (batch).  The CRC covers
type|klen|vlen|key|value -- everything after the CRC field itself -- so a
torn append cannot masquerade as a record with a believable length.

A batch record carries a sequence of:

    type(1) | klen(4) | vlen(4) | key | value

inside its 'value' field.  One record, one CRC: replay applies every
operation in the batch or none of them.

SSTable data record (the unit stored inside a data block)
---------------------------------------------------------

    klen(4) | tombstone(1) | vlen(4) | key | value

SSTable index block
-------------------

    min_klen(4) | max_klen(4) | min_key | max_key | entry...

The two leading key lengths give the block the key range it covers, which
lets a read skip a whole segment without touching its data.  The header is
inside the block that the index CRC covers.

SSTable footer (the last 56 bytes of the file)
----------------------------------------------

    index_offset(8) index_len(8) bloom_offset(8) bloom_len(8)
    index_crc(4) bloom_crc(4) num_entries(8) magic(8)
"""

from __future__ import annotations

import struct
import zlib
from collections.abc import Iterable

from .errors import CorruptRecordError, IncompleteRecordError

UINT32_MAX = 0xFFFFFFFF
UINT64_MAX = 0xFFFFFFFFFFFFFFFF

#: Largest key the engine accepts (64 KiB).  Keys live in the memtable and
#: in every sparse index entry that references them, so they are bounded.
MAX_KEY_BYTES = 64 << 10

#: Largest value the engine accepts (16 MiB).  One value is one record and
#: must fit in memory alongside its key; bigger payloads belong in files.
MAX_VALUE_BYTES = 16 << 20

# --- WAL -----------------------------------------------------------------

WAL_HEADER = struct.Struct("<IBII")          # crc, type, klen, vlen (decode)
WAL_CRC = struct.Struct("<I")                # crc on its own (encode)
WAL_BODY_HEAD = struct.Struct("<BII")        # type, klen, vlen
BATCH_OP = struct.Struct("<BII")             # type, klen, vlen

WAL_TYPE_PUT = 1
WAL_TYPE_DELETE = 2
WAL_TYPE_BATCH = 3

# --- SSTable -------------------------------------------------------------

SST_RECORD_HEAD = struct.Struct("<IBI")      # klen, tombstone, vlen
SST_INDEX_HEAD = struct.Struct("<I")         # key length
SST_INDEX_TAIL = struct.Struct("<QII")       # offset, length, crc
SST_INDEX_HEADER = struct.Struct("<II")      # min key length, max key length
SST_FOOTER = struct.Struct("<QQQQIIQ8s")     # see module docstring
SST_MAGIC = b"MINIKV02"
SST_FOOTER_SIZE = SST_FOOTER.size            # 56

BLOOM_HEAD = struct.Struct("<III")           # bit_size, num_hashes, nbytes


def crc32(data, seed: int = 0) -> int:
    """CRC32 as a positive 32-bit integer (zlib returns a signed value)."""
    return zlib.crc32(data, seed) & UINT32_MAX


def _check_u32(n: int, what: str) -> None:
    if not 0 <= n <= UINT32_MAX:
        raise ValueError(f"{what} length {n} does not fit in a uint32")


def validate_key(key: bytes) -> None:
    """Reject keys the on-disk format cannot represent."""
    if len(key) > MAX_KEY_BYTES:
        raise ValueError(f"key is {len(key)} bytes; the limit is {MAX_KEY_BYTES}")


def validate_value(value: bytes) -> None:
    """Reject values the on-disk format cannot represent."""
    if len(value) > MAX_VALUE_BYTES:
        raise ValueError(f"value is {len(value)} bytes; the limit is {MAX_VALUE_BYTES}")


def validate_pair(key: bytes, value: bytes) -> None:
    validate_key(key)
    validate_value(value)
    _check_u32(len(key), "key")
    _check_u32(len(value), "value")


def encode_wal_record(rec_type: int, key: bytes = b"", value: bytes = b"") -> bytes:
    """Serialize one WAL record, CRC included."""
    _check_u32(len(key), "key")
    _check_u32(len(value), "value")
    klen = len(key)
    vlen = len(value)
    # A record is exactly crc(4) plus the body, so the body can be built
    # once, checksummed, and prefixed: key and value are copied once, and
    # zlib.crc32 is called once per record.  (The obvious version copied
    # key+value a second time to build the record; rolling the CRC over
    # three pieces instead was measured to be slower, because crc32 is
    # only cheap when it is called once.)
    body = WAL_BODY_HEAD.pack(rec_type, klen, vlen) + key + value
    return WAL_CRC.pack(crc32(body)) + body


def decode_wal_record(buf, offset: int, limit: int | None = None) -> tuple[int, bytes, bytes, int]:
    """Decode the record at 'offset'.

    Returns (type, key, value, next_offset).  Raises IncompleteRecordError
    when the buffer ends mid-record (a torn tail) and CorruptRecordError
    when the CRC does not match.  'buf' may be bytes, bytearray or a
    memoryview, so a replay loop can parse a reused buffer in place.
    'limit' is len(buf) when the caller already knows it; replay hands it in
    because it decodes tens of thousands of records per chunk.

    The checksum is taken over one contiguous slice for the same reason the
    encoder builds one payload: rolling it over header, key and value
    separately triples the zlib calls and measured slower, twice (v1.1 and
    v1.4).  See BENCHMARKS.md.
    """
    if limit is None:
        limit = len(buf)
    if offset + WAL_HEADER.size > limit:
        raise IncompleteRecordError("torn WAL header")
    crc, rec_type, klen, vlen = WAL_HEADER.unpack_from(buf, offset)
    start = offset + WAL_HEADER.size
    end = start + klen + vlen
    if end > limit:
        raise IncompleteRecordError("torn WAL body")
    key = bytes(buf[start : start + klen])
    value = bytes(buf[start + klen : end])
    if crc32(memoryview(buf)[offset + 4 : end]) != crc:
        raise CorruptRecordError(f"WAL record at offset {offset} failed CRC")
    return rec_type, key, value, end


def encode_batch(ops: Iterable[tuple[int, bytes, bytes]]) -> bytes:
    """Serialize (type, key, value) operations into a batch payload."""
    out = bytearray()
    for rec_type, key, value in ops:
        _check_u32(len(key), "key")
        _check_u32(len(value), "value")
        out += BATCH_OP.pack(rec_type, len(key), len(value))
        out += key
        out += value
    return bytes(out)


def decode_batch(payload: bytes) -> list[tuple[int, bytes, bytes]]:
    """Inverse of encode_batch."""
    ops: list[tuple[int, bytes, bytes]] = []
    offset = 0
    while offset < len(payload):
        if offset + BATCH_OP.size > len(payload):
            raise CorruptRecordError("truncated batch payload")
        rec_type, klen, vlen = BATCH_OP.unpack_from(payload, offset)
        start = offset + BATCH_OP.size
        end = start + klen + vlen
        if end > len(payload):
            raise CorruptRecordError("truncated batch payload")
        payload_key = payload[start : start + klen]
        payload_value = payload[start + klen : end]
        ops.append((rec_type, payload_key, payload_value))
        offset = end
    return ops


def encode_sst_record(key: bytes, value: bytes, tombstone: bool) -> bytes:
    """Serialize one key inside an SSTable data block."""
    _check_u32(len(key), "key")
    _check_u32(len(value), "value")
    head = SST_RECORD_HEAD.pack(len(key), 1 if tombstone else 0, len(value))
    return head + key + value


def decode_sst_record(buf, offset: int, limit: int) -> tuple[bytes, bytes, bool, int]:
    """Decode the SSTable record at 'offset'; 'limit' bounds the block."""
    if offset + SST_RECORD_HEAD.size > limit:
        raise CorruptRecordError("truncated SSTable record header")
    klen, tomb, vlen = SST_RECORD_HEAD.unpack_from(buf, offset)
    start = offset + SST_RECORD_HEAD.size
    end = start + klen + vlen
    if end > limit:
        raise CorruptRecordError("truncated SSTable record body")
    key = bytes(buf[start : start + klen])
    value = bytes(buf[start + klen : end])
    return key, value, bool(tomb), end
