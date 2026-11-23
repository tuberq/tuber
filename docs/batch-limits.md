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
memory before sending (src/server.rs cmd_reserve_batch), and the connection
task then serialises it into its write buffer, a second copy of every body
(a single `reserve` is written without that copy: `Response::split_body`).
With the default 1 MiB max job size (`--max-job-size`), worst case is
`count × 1 MiB` per copy:

| count  | worst-case response | peak, both copies |
| ------ | ------------------- | ----------------- |
| 1 000  | 1 GiB               | 2 GiB             |
| 10 000 | 10 GiB              | 20 GiB            |

That is per batch in flight, and `--max-jobs-size` doesn't count it: the
budget covers what jobs hold, not what a reply materialises.

With persistence on, every body over `HEAP_INLINE_MAX` (256 bytes) lives in
TOAST and is read back synchronously in the engine (`fetch_body`), so a
batch of large bodies also stalls every other connection while it reads
them. The engine isn't blocked on the socket write (reply goes via
channel). Raising this limit is risky in proportion to your max job size.

## `delete-batch` — bounded by engine-loop time

Per-op work is cheap: state mutation, idempotency/group bookkeeping, one WAL
append, no body. The dominant cost is WAL fsync policy:

- **Interval-based fsync** (default): 10 000 deletes appends 10 000 records
  to a buffered writer, fsynced on the configured cadence. Sub-second.
- **Strict fsync (`--sync-interval 0`)**: group commit. The engine fsyncs
  once after each batch of commands it drains, before acking any of them,
  so a `delete-batch` costs one fsync however many ids it names — not one
  per delete.

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
The costs of raising `delete-batch` are the two above: a larger worst-case
line a client can force, and a longer engine-loop stall per command.

## Before raising either limit

1. Confirm your max job size. Workloads with job-size limits above the
   1 MiB default should keep `reserve-batch` lower than the default, not
   higher.
2. Benchmark on the target disk with persistence on: TOAST reads for
   `reserve-batch` and WAL appends for `delete-batch` both run in the
   engine.
