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

SSTable footer (the last 56 bytes of the file)
----------------------------------------------

    index_offset(8) index_len(8) bloom_offset(8) bloom_len(8)
    index_crc(4) bloom_crc(4) num_entries(8) magic(8)

Data blocks do not carry their CRC inline; each sparse-index entry
stores (first_key, offset, length, crc) for the block it points at, so a
damaged block is detected by the read that touches it.
"""

from __future__ import annotations

import struct
import zlib
from collections.abc import Iterable

from .errors import CorruptRecordError, IncompleteRecordError

UINT32_MAX = 0xFFFFFFFF
UINT64_MAX = 0xFFFFFFFFFFFFFFFF

# --- WAL -----------------------------------------------------------------

WAL_HEADER = struct.Struct("<IBII")          # crc, type, klen, vlen
WAL_BODY_HEAD = struct.Struct("<BII")        # type, klen, vlen
BATCH_OP = struct.Struct("<BII")             # type, klen, vlen

WAL_TYPE_PUT = 1
WAL_TYPE_DELETE = 2
WAL_TYPE_BATCH = 3

# --- SSTable -------------------------------------------------------------

SST_RECORD_HEAD = struct.Struct("<IBI")      # klen, tombstone, vlen
SST_INDEX_HEAD = struct.Struct("<I")         # key length
SST_INDEX_TAIL = struct.Struct("<QII")       # offset, length, crc
SST_FOOTER = struct.Struct("<QQQQIIQ8s")     # see module docstring
SST_MAGIC = b"MINIKV01"
SST_FOOTER_SIZE = SST_FOOTER.size            # 56

BLOOM_HEAD = struct.Struct("<III")           # bit_size, num_hashes, nbytes


def crc32(data: bytes, seed: int = 0) -> int:
    """CRC32 as a positive 32-bit integer (zlib returns a signed value)."""
    return zlib.crc32(data, seed) & UINT32_MAX


def _check_len(n: int, what: str) -> None:
    if not 0 <= n <= UINT32_MAX:
        raise ValueError(f"{what} length {n} does not fit in a uint32")


def encode_wal_record(rec_type: int, key: bytes = b"", value: bytes = b"") -> bytes:
    """Serialize one WAL record, CRC included."""
    _check_len(len(key), "key")
    _check_len(len(value), "value")
    body = WAL_BODY_HEAD.pack(rec_type, len(key), len(value)) + key + value
    return WAL_HEADER.pack(crc32(body), rec_type, len(key), len(value)) + key + value


def decode_wal_record(buf: bytes, offset: int) -> tuple[int, bytes, bytes, int]:
    """Decode the record at 'offset'.

    Returns (type, key, value, next_offset).  Raises IncompleteRecordError
    when the buffer ends mid-record (a torn tail) and CorruptRecordError
    when the CRC does not match.
    """
    if offset + WAL_HEADER.size > len(buf):
        raise IncompleteRecordError("torn WAL header")
    crc, rec_type, klen, vlen = WAL_HEADER.unpack_from(buf, offset)
    start = offset + WAL_HEADER.size
    end = start + klen + vlen
    if end > len(buf):
        raise IncompleteRecordError("torn WAL body")
    key = buf[start : start + klen]
    value = buf[start + klen : end]
    body = buf[offset + 4 : end]
    if crc32(body) != crc:
        raise CorruptRecordError(f"WAL record at offset {offset} failed CRC")
    return rec_type, key, value, end


def encode_batch(ops: Iterable[tuple[int, bytes, bytes]]) -> bytes:
    """Serialize (type, key, value) operations into a batch payload."""
    out = bytearray()
    for rec_type, key, value in ops:
        _check_len(len(key), "key")
        _check_len(len(value), "value")
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
        key = payload[start : start + klen]
        value = payload[start + klen : end]
        ops.append((rec_type, key, value))
        offset = end
    return ops


def encode_sst_record(key: bytes, value: bytes, tombstone: bool) -> bytes:
    """Serialize one key inside an SSTable data block."""
    _check_len(len(key), "key")
    _check_len(len(value), "value")
    head = SST_RECORD_HEAD.pack(len(key), 1 if tombstone else 0, len(value))
    return head + key + value


def decode_sst_record(buf: bytes, offset: int, limit: int) -> tuple[bytes, bytes, bool, int]:
    """Decode the SSTable record at 'offset'; 'limit' bounds the block."""
    if offset + SST_RECORD_HEAD.size > limit:
        raise CorruptRecordError("truncated SSTable record header")
    klen, tomb, vlen = SST_RECORD_HEAD.unpack_from(buf, offset)
    start = offset + SST_RECORD_HEAD.size
    end = start + klen + vlen
    if end > limit:
        raise CorruptRecordError("truncated SSTable record body")
    key = buf[start : start + klen]
    value = buf[start + klen : end]
    return key, value, bool(tomb), end
