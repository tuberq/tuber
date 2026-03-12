# WAL Binary Format Specification

Version 3 — Tuber Write-Ahead Log

## Overview

When persistence is enabled (`-b <dir>`), tuber writes all job mutations to a
segmented, append-only write-ahead log (WAL). On restart, the WAL is replayed
to restore server state.

Files are named `binlog.NNNNNN` (zero-padded sequence number) and stored in the
WAL directory alongside a `lock` file used for exclusive access via `flock(2)`.

```
wal-dir/
  lock              # flock — prevents concurrent access
  binlog.000001     # oldest segment
  binlog.000002
  binlog.000003     # current writable segment
```

Each file begins with a 12-byte header, followed by a sequence of records.
All multi-byte integers are **little-endian**.

## File Header

```
 0                   1                   2
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  'T'  'W'  'A'  'L'  |     version (u32)     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|      flags (u32)      |
+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Offset | Size | Field     | Value                                      |
|--------|------|-----------|--------------------------------------------|
| 0      | 4    | magic     | `TWAL` (`0x5457414C`)                      |
| 4      | 4    | version   | `3` (u32 LE)                               |
| 8      | 4    | flags     | `0` — reserved for future use (see below)  |

**Total: 12 bytes**

The `flags` field is reserved for forward compatibility. Currently written as
zero and ignored on read. Future versions may use individual bits to signal
file-level features (e.g. compressed bodies) without bumping the version number.

## Record Types

Two record types follow the header. Both share a common envelope:

```
+-----------+------------------+------------------+---------+----------+
| type (u8) |   job_id (u64)   | payload_len (u32)|  payload | crc (u32)|
+-----------+------------------+------------------+---------+----------+
     1              8                  4            variable      4
```

The CRC32 (using the crc32c polynomial via `crc32fast`) covers everything from
the type byte through the end of the payload — the CRC itself is excluded.

### 0x01 — FullJob

Written on `put`. Contains the complete job state.

```
+------+----------+-------------+-----------------------------------+-----+
| 0x01 | job_id   | payload_len |            payload                | crc |
|  1B  |   8B     |     4B      |          variable                 | 4B  |
+------+----------+-------------+-----------------------------------+-----+
```

**Payload layout** (field order matches `serialize_full_job`):

```
+------------------------------------------------------------------+
| priority (u32)                                                    |  4
+------------------------------------------------------------------+
| delay_nanos (u64)                                                 |  8
+------------------------------------------------------------------+
| ttr_nanos (u64)                                                   |  8
+------------------------------------------------------------------+
| created_at_epoch_secs (u64)                                       |  8
+------------------------------------------------------------------+
| state (u8)                                                        |  1
+------------------------------------------------------------------+
| reserve_ct (u32)                                                  |  4
+------------------------------------------------------------------+
| timeout_ct (u32)                                                  |  4
+------------------------------------------------------------------+
| release_ct (u32)                                                  |  4
+------------------------------------------------------------------+
| bury_ct (u32)                                                     |  4
+------------------------------------------------------------------+
| kick_ct (u32)                                                     |  4
+------------------------------------------------------------------+
| tube_name_len (u16) | tube_name (bytes)                          |  2+N
+------------------------------------------------------------------+
| idempotency_key          (option_string)                          |  2+N
+------------------------------------------------------------------+
| idempotency_ttl (u32)                                             |  4
+------------------------------------------------------------------+
| group                    (option_string)                          |  2+N
+------------------------------------------------------------------+
| after_group              (option_string)                          |  2+N
+------------------------------------------------------------------+
| concurrency_key          (option_string)                          |  2+N
+------------------------------------------------------------------+
| concurrency_limit (u32)                                           |  4
+------------------------------------------------------------------+
| body_len (u32)      | body (bytes)                               |  4+N
+------------------------------------------------------------------+
```

### 0x02 — StateChange

Written on `delete`, `bury`, `release`, `kick`. Fixed-size payload (21 bytes).

```
+------+----------+-------------+---------------------+-----+
| 0x02 | job_id   | payload_len |       payload       | crc |
|  1B  |   8B     |  4B (= 21) |        21B          | 4B  |
+------+----------+-------------+---------------------+-----+
                                 |                     |
                                 v                     |
                   +-------------+---+--------+--------+
                   | state | pri | delay_ns  | expiry  |
                   |  1B   | 4B  |    8B     |   8B    |
                   +-------+-----+-----------+---------+
```

| Offset* | Size | Field              | Notes                                   |
|---------|------|--------------------|-----------------------------------------|
| 0       | 1    | state              | New state, or `0xFF` for deleted         |
| 1       | 4    | new_priority       | Updated priority (u32)                  |
| 5       | 8    | new_delay_nanos    | Updated delay in nanoseconds (u64)      |
| 13      | 8    | expiry_epoch_secs  | Idempotency tombstone expiry (u64), 0 = none |

*Offsets relative to payload start.

**Total record size: 38 bytes** (1 + 8 + 4 + 21 + 4)

## Encoding Details

### State Encoding

| Value  | State      |
|--------|------------|
| `0x00` | Ready      |
| `0x01` | Reserved   |
| `0x02` | Delayed    |
| `0x03` | Buried     |
| `0xFF` | Deleted    |

### option_string

Variable-length nullable string, used for extension fields.

```
+----------------+-----------------+
| len (u16 LE)   | bytes (UTF-8)   |
+----------------+-----------------+
```

- `len = 0` → None (no bytes follow)
- `len > 0` → Some(string), `len` bytes of UTF-8 follow

### Idempotency Tombstones

When a job with an `idp:` key and TTL is deleted, the StateChange record's
`expiry_epoch_secs` is set to the UNIX timestamp when the tombstone expires.
On replay, tombstones with `expiry_epoch_secs > now` are restored to prevent
re-insertion of recently completed jobs.

## Replay Semantics

On startup, files are read in sequence order. For each record:

- **FullJob**: Insert or replace in the job map. Track WAL position for GC.
- **StateChange (delete)**: Remove from job map. If `expiry_epoch_secs > now`,
  extract the idempotency key and restore the tombstone.
- **StateChange (other)**: Update the job's state, priority, and delay.

State adjustments during replay:
- **Reserved → Ready** — reservations don't survive restarts
- **Delayed** — `deadline_at` reconstructed as `Instant::now() + delay`
- `reserver_id` always set to `None`

Corrupt or truncated records terminate processing of that file (with a warning).
Valid records before the corruption point are preserved. The file is truncated
at the corrupt offset to prevent repeated warnings on future restarts.

## File Management

### Segmentation and Rotation

Files rotate when the current segment reaches `max_file_size` (default 10 MB).
The current file is fsynced before a new segment is created.

StateChange records are allowed to slightly exceed the size limit — this avoids
per-file balancing complexity while keeping segments roughly bounded.

### Garbage Collection

Each file tracks a reference count (`refs`) — the number of live jobs whose
most recent WAL record is in that file. On each server tick, head files with
`refs == 0` are deleted. The current writable file is never removed.

### Space Reservation

A global `reserved_bytes` counter ensures disk space for future deletes:

- On `put`: reserve `STATE_CHANGE_RECORD_SIZE` (38 bytes) for the eventual delete
- On delete/state change: release the reservation
- `reserve_put()` checks: current free + one new file >= reserved + needed
- If insufficient space: return `OUT_OF_MEMORY` (server can still process deletes)

### Directory Locking

The WAL directory is exclusively locked via `flock(2)` on a `lock` file.
This prevents multiple tuber instances from writing to the same WAL.

## Version History

| Version | Header Size | Changes                                            |
|---------|-------------|----------------------------------------------------|
| 1       | 8 bytes     | Initial format: magic + version                   |
| 2       | 12 bytes    | Added `flags` (u32) field for forward compatibility. Added `expiry_epoch_secs` to StateChange for idempotency tombstones. Added `concurrency_limit` and `idempotency_ttl` to FullJob payload. |
| 3       | 12 bytes    | Reordered FullJob payload: body moved to end, idempotency_ttl grouped with idempotency_key, concurrency_limit grouped with concurrency_key. |
