# On-disk format

Every integer is little-endian.  Every structure that can be damaged
carries a CRC32 over exactly the bytes it protects.  Nothing on disk is
ever unpickled.

## Directory

    data/
      LOCK                 exclusive lock, holds "pid=<n>" for reference
      MANIFEST             live segment set, newest first
      seg-000007.sst       immutable segment
      seg-000009.sst
      wal-000012.log       write-ahead log for one memtable generation
      *.tmp                transient; deleted on open

Names encode a sequence number, so "newest" is the highest number.
Segments are listed in the manifest; a segment that is present but not
listed is deleted when the store opens (that is what makes a crash
between rename and manifest update harmless).

## WAL record

    offset  size  field
    0       4     crc32 of bytes 4..end
    4       1     type: 1 = put, 2 = delete, 3 = batch
    5       4     key length
    9       4     value length
    13      k     key
    13+k    v     value

Total length is 13 + k + v.  A record longer than the remaining buffer is
a torn append: replay stops and the file is truncated there.

## Batch payload (the value field of a type-3 record)

A sequence of operations, each:

    offset  size  field
    0       1     type: 1 = put, 2 = delete
    1       4     key length
    5       4     value length
    9       k     key
    9+k     v     value

The outer record carries one CRC over all of them, so replay applies the
whole batch or none of it.

## Segment

    [data block][data block] ... [index block][bloom block][footer 56B]

### Data record (inside a block)

    offset  size  field
    0       4     key length
    4       1     tombstone flag (1 = deleted)
    5       4     value length
    9       k     key
    9+k     v     value

A tombstone stores an empty value: it means "this key was deleted" and
shadows any older value in an older segment.

### Data block

Blocks are closed at roughly 4 KiB (`Store(block_bytes=...)`).  A block
does not carry its CRC inline; the CRC is in the index entry that points
at it, so the writer never has to seek back to patch a header.

### Index block

    offset  size  field
    0       4     minimum key length
    4       4     maximum key length
    8       a     minimum key
    8+a     b     maximum key
    8+a+b   ...   entries, each of:

        offset  size  field
        0       4     key length (the block's first key)
        4       k     key
        4+k     8     block offset from the start of the file
        12+k    4     block length
        16+k    4     crc32 of the block's bytes

One entry per data block, so the index is roughly 1/35th of the key count
at the default block size.  The minimum and maximum key let a reader
reject the whole segment with two byte comparisons, before the bloom
filter is consulted.  The index block's CRC is stored in the footer.

### Bloom filter block

    offset  size  field
    0       4     bit count (m)
    4       4     number of hash functions (k)
    8       4     number of bytes that follow
    12      n     the bit array

Built over every key in the segment, with a 1% target false-positive rate
and double hashing over CRC32.  A bloom hit is only a hint: the index and
the block are still consulted.

### Footer (the last 56 bytes)

    offset  size  field
    0       8     index block offset
    8       8     index block length
    16      8     bloom block offset
    24      8     bloom block length
    32      4     crc32 of the index block
    36      4     crc32 of the bloom block
    40      8     number of records
    48      8     magic, "MINIKV02"

Opening a segment verifies the magic, the footer bounds, the index CRC and
the bloom CRC.  Data blocks are verified by the read that touches them;
`Store.verify()` and `minikv fsck` verify all of them.

## Manifest

    minikv-manifest-v2
    seg-000009.sst 12
    seg-000007.sst 8
    crc32 1f0c3a9d

The CRC covers every line before the `crc32` line, including its
terminator.  Lines after the magic name the live segments, newest first,
followed by the highest WAL sequence number each one covers -- the
checkpoint watermark.  Written as `MANIFEST.tmp`, fsynced, renamed, then
the directory is fsynced where the platform supports it.

A manifest that is missing, malformed or fails its CRC is not fatal: the
store warns, rebuilds the live set from the directory and rewrites it.  A
rebuilt live set has watermark 0, so nothing is skipped and every WAL is
replayed -- slower, and always safe.  A `minikv-manifest-v1` file (no
coverage column) is treated the same way.

## Order of operations

A write:

1. append the WAL record and fsync it, if the sync mode says so;
2. apply it to the memtable.

A flush:

1. write `seg-NNNNNN.sst.tmp` and fsync it;
2. rename it to `seg-NNNNNN.sst`, then fsync the directory (POSIX);
3. write the manifest listing the new live set and the highest WAL
   sequence it covers, fsync, rename, fsync dir;
4. unlink the WAL files whose contents are now inside that segment.

On the next open, any WAL at or below the recorded watermark is deleted
without being read: its records are already in a segment.  That is the
crash window between steps 3 and 4, and it is why a start after a crash
does not replay records that were already durable.

A crash between any two steps leaves either the previous state or the new
one.  Each of those steps is a named fault point in `minikv/faults.py`,
and `tests/test_crash_recovery.py` kills the process at every one of them
(see docs/crash-consistency.md).

## Finding a value

1. The memtable answers from a dict, if it has the key.
2. Otherwise each live segment is considered newest first: if the key is
   outside the segment's recorded range, skip it.
3. If the bloom filter says "definitely not here", skip it.
4. Binary search the index for the block that could contain the key,
   verify that block's CRC, then decode its records in order.

A tombstone found on the way stops the search: the key is deleted, and an
older segment must not resurrect it.
