# Architecture

## Overview

beanstalkd-rs is a Rust rewrite of [beanstalkd](https://beanstalkd.github.io/), a simple
fast work queue. It is wire-compatible with existing beanstalkd clients.

## Core Concepts

### Jobs

A job is a unit of work with a body (arbitrary bytes), priority, delay, and TTR
(time-to-run). Jobs move through a state machine:

```
         put(delay>0)          deadline
    ┌──────────────────► DELAYED ─────────────► READY
    │                       ▲                    │  ▲
    │          release(     │                    │  │
    │          delay>0)     │         reserve    │  │ kick
    │                       │                    ▼  │
  CLIENT              ◄──────────────────── RESERVED
                      release(delay=0)          │
                            │                   │ bury
                            │                   ▼
                            │                BURIED
                            │                   │
                     delete from any state ──► DELETED
```

### Tubes

Tubes are named queues. Each tube has:
- **ready heap** -- priority-ordered min-heap of jobs ready for consumption
- **delay heap** -- deadline-ordered min-heap of jobs waiting to become ready
- **buried list** -- jobs that have been buried (failed), kicked to re-enter ready
- **waiting connections** -- connections blocked on `reserve` for this tube

The default tube is named `"default"`.

### Connections

Each TCP connection has:
- **use tube** -- the tube that `put` commands insert into (default: `"default"`)
- **watch set** -- the set of tubes that `reserve` commands draw from (default: `{"default"}`)
- **reserved jobs** -- jobs currently reserved by this connection
- **reserve mode** -- FIFO (default) or weighted selection across watched tubes

## Rust Architecture

### Engine Pattern (Single-owner state)

```
  TCP Connection tasks ──(Command, response_tx)──► Engine task
                        ◄──────(Response)──────────┘
```

The C beanstalkd is single-threaded with an event loop. We preserve this
simplicity with a single **engine task** that owns all server state:

- `ServerState` holds all tubes, jobs (in a `HashMap<u64, Job>`), connections,
  and stats
- Each TCP connection spawns its own tokio task for reading/writing the socket
- Connection tasks send `Command` messages to the engine via an `mpsc` channel
- The engine processes commands sequentially (no locking needed) and sends
  responses back via per-request `oneshot` channels

For blocking `reserve`: the engine stores the `oneshot::Sender` and fires it
when a matching job becomes available (or on timeout).

The engine also runs a tick timer to:
- Promote delayed jobs to ready when their deadline passes
- Expire reserved jobs past their TTR back to ready
- Unpause paused tubes

### Job Ownership

Jobs are stored in a central `HashMap<u64, Job>`. All other structures
(tube heaps, buried lists, connection reserved lists) reference jobs by their
`u64` ID. This avoids `Rc`/`Arc`/`RefCell` entirely.

Heap entries store `(sort_key, job_id)` tuples so ordering comparisons don't
need to look up the job.

### Custom IndexHeap

Rust's `std::collections::BinaryHeap` does not support removal by index, which
is needed for deleting/kicking jobs from tube heaps. We implement a custom
`IndexHeap<K>` that maintains an internal `HashMap<u64, usize>` mapping IDs to
positions, updated during sift operations.
