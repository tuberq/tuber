# Batch command limits

`reserve-batch` and `delete-batch` are bounded by `MAX_RESERVE_BATCH` and
`MAX_DELETE_BATCH` in `src/protocol.rs` — the constants there are
authoritative; the numbers below assume their value at the time of writing,
1000. The two limits guard different failure modes and don't have to move
together.

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

There's also a side effect on the read path: `MAX_LINE_LEN` — the per-line
read ceiling — is derived from `MAX_DELETE_BATCH` (see the comment on the
constant in `src/server.rs` for the exact formula). Raising the cap 10×
raises that ceiling from ~21 KB to ~210 KB. The line buffer grows on
demand, so ordinary clients pay nothing — this is the worst case a single
connection *can* force, not a per-connection cost.

## Recommended split: keep `reserve-batch` at 1 000, raise `delete-batch`

A worker that reserves in 1 K chunks and acks the whole accumulated window
in one `delete-batch` is a good fit for the protocol — it amortizes the
round-trip without forcing the server to hold a giant response in memory.
The costs of raising `delete-batch` are exactly the two above: a larger
worst-case line a client can force, and — under `-f 0` only — a longer
fsync stall per command.

## Before raising either limit

1. Confirm your fsync policy. With per-write fsync, neither limit should be
   raised without benchmarking on the target disk.
2. Confirm your max job size. Workloads with 1 GB job-size limits should
   keep `reserve-batch` lower than the default, not higher.
