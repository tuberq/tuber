# Batch command limits

`reserve-batch` and `delete-batch` are bounded by `MAX_RESERVE_BATCH` and
`MAX_DELETE_BATCH` (both currently 1000, in `src/protocol.rs`). The two limits
guard different failure modes and don't have to move together.

## Why limits exist

The engine is single-threaded: `handle_command` (src/server.rs) runs to
completion before any other client's command is processed. Both batch
operations are tight loops with no yield points, so a large batch is a
head-of-line stall for every other connection.

## `reserve-batch` — bounded by response size

The handler builds `Response::ReservedBatch(Vec<(u64, Vec<u8>)>)` fully in
memory before sending (src/server.rs cmd_reserve_batch). With the default
64 KB max job size, worst-case response = `count × 64 KB`:

| count  | worst-case response |
| ------ | ------------------- |
| 1 000  | 64 MB               |
| 10 000 | 640 MB              |

The engine isn't blocked on the socket write (reply goes via channel), but
the spike is real if bodies are large. Raising this limit is risky in
proportion to your max job size.

## `delete-batch` — bounded by engine-loop time

Per-op work is cheap: state mutation, idempotency/group bookkeeping, one WAL
append, no body. The dominant cost is WAL fsync policy:

- **Interval-based fsync** (default): 10 000 deletes appends 10 000 records
  to a buffered writer, fsynced on the configured cadence. Sub-second.
- **Per-write fsync (`-f 0`)**: every delete fsyncs. 10 000 fsyncs serialized
  inside one command can be seconds on commodity disk. **Measure before
  raising the cap if you run with `-f 0`.**

There's also a side effect on the read path: `MAX_LINE_LEN` is derived from
`MAX_DELETE_BATCH` (src/server.rs):

```
MAX_LINE_LEN = 13 + MAX_DELETE_BATCH × (MAX_U64_DIGITS + 1) + 2
```

Raising `MAX_DELETE_BATCH` from 1 000 → 10 000 pushes the per-line read cap
from ~21 KB to ~210 KB on **every** connection, paid by every client that
sends any command, not just batch users.

## Recommended split: keep `reserve-batch` at 1 000, raise `delete-batch`

A worker that reserves in 1 K chunks and acks the whole window in one
`delete-batch` is a good fit for the protocol — it amortizes the
round-trip without forcing the server to hold a giant response in memory.

Pros:

- Caps `reserve-batch` response at ~64 MB worst-case.
- Lets a worker accumulate 10× reserve-batch windows (10 000 jobs) and ack
  them in a single round-trip.
- `delete-batch` per-op work is cheap and predictable.

Cons:

- Per-connection line buffer grows to ~210 KB regardless of whether the
  client uses batch ops.
- With `-f 0`, one 10 K delete-batch can stall the engine for the duration
  of 10 000 fsyncs.

## Before raising either limit

1. Confirm your fsync policy. With per-write fsync, neither limit should be
   raised without benchmarking on the target disk.
2. Confirm your max job size. Workloads with 1 GB job-size limits should
   keep `reserve-batch` lower than the default, not higher.
3. Decide whether the line-buffer growth is acceptable for your connection
   count.
