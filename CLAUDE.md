# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust rewrite of [beanstalkd](https://github.com/beanstalkd/beanstalkd), a simple and fast work queue. The original C source is in `tmp/` for reference. We use tokio for async I/O.

## Build & Test Commands

```bash
cargo build                        # Build
cargo test                         # Run all tests
cargo test test_name               # Run a single test
cargo clippy                       # Lint
cargo fmt -- --check               # Check formatting
cargo run -- -l 0.0.0.0 -p 11300  # Run server
```

## Architecture

The codebase mirrors the original C beanstalkd structure:

- **`src/main.rs`** - CLI entry point using clap subcommands (`server`, `put`, `stats`, `tubes`, `work`). Parses args and dispatches.
- **`src/lib.rs`** - Library root, exports all modules.
- **`src/server.rs`** - Tokio TCP server, accepts connections and dispatches to protocol handler.
- **`src/protocol.rs`** - Beanstalkd text protocol parser and command dispatch. The protocol is line-based (`\r\n` terminated). See `tmp/prot.c` for the full C implementation. Includes tuber extensions: `idp:`, `grp:`, `aft:`, `con:` tags on `put`, `reserve-mode`, and `flush-tube`.
- **`src/job.rs`** - Job struct and job storage (hash table). Jobs have states: Ready, Reserved, Buried, Delayed. Jobs are ordered by priority then ID. Includes fields for idempotency key, group, after-group, and concurrency key.
- **`src/tube.rs`** - Named queues ("tubes"). Each tube has a ready heap, delay heap, buried list, and waiting connections list. Default tube is "default".
- **`src/conn.rs`** - Per-connection state: current tube (`use`), watched tubes (`watch`), reserved jobs list, reserve mode (FIFO or weighted), and read/write buffers.
- **`src/heap.rs`** - Binary min-heap used for ready queue (priority ordering), delay queue (deadline ordering), and connection timeouts.
- **`src/wal.rs`** - Write-ahead log for persistence (optional, enabled with `-b`).
- **`src/client.rs`** - TCP client for connecting to a tuber server. Used by CLI subcommands.
- **`src/cmd_put.rs`** - CLI `put` command: submits jobs to a tube, supports stdin piping.
- **`src/cmd_stats.rs`** - CLI `stats` command: displays global or per-tube statistics.
- **`src/cmd_tubes.rs`** - CLI `tubes` command: lists all tubes with job count summaries.
- **`src/cmd_work.rs`** - CLI `work` command: reserves and executes jobs as shell commands with parallel workers.
- **`src/metrics.rs`** - Prometheus metrics HTTP server (optional, enabled with `--metrics-port`).

## Key Constants from Original C (dat.h)

- `MAX_TUBE_NAME_LEN`: 201 (tube name max is 200 chars)
- `URGENT_THRESHOLD`: 1024 (jobs with pri < 1024 are "urgent")
- `JOB_DATA_SIZE_LIMIT_DEFAULT`: 65535 bytes
- `JOB_DATA_SIZE_LIMIT_MAX`: 1GB (1073741824)
- `MAX_TUBE_WEIGHT`: 9999 (for weighted reserve mode)
- Default port: 11300
- WAL version: 7

## Beanstalkd Protocol Commands

Standard beanstalkd commands: `put`, `reserve`, `reserve-with-timeout`, `reserve-job`, `delete`, `release`, `bury`, `kick`, `kick-job`, `touch`, `peek`, `peek-ready`, `peek-delayed`, `peek-buried`, `use`, `watch`, `ignore`, `stats`, `stats-job`, `stats-tube`, `list-tubes`, `list-tube-used`, `list-tubes-watched`, `pause-tube`, `quit`.

Tuber extensions beyond standard beanstalkd:
- `reserve-mode <default|weighted>` - switch between priority-first and weighted-random reserve strategies.
- `peek-reserved` - peek at the oldest reserved job in the current `use` tube. Returns `FOUND <id> <bytes>` or `NOT_FOUND`.
- `flush-tube <tube>` - delete all jobs from a tube. Returns `FLUSHED <count>`.
- `put` extension tags (appended after `<bytes>`): `idp:<key>` (idempotency), `grp:<name>` (job group), `aft:<name>` (after-group dependency), `con:<key>` (concurrency key).
- `watch <tube> [weight]` - optional weight parameter for weighted reserve mode.

Responses are text, e.g. `INSERTED <id>\r\n`, `RESERVED <id> <bytes>\r\n`, `DELETED\r\n`, `NOT_FOUND\r\n`, `BAD_FORMAT\r\n`.

## Testing Strategy

Port tests from `tmp/test*.c`:
- **`testjobs.c`** - Unit tests for job creation, priority comparison, hash table operations
- **`testheap.c`** - Unit tests for binary heap insert/remove/ordering
- **`testms.c`** - Unit tests for the resizable multiset (Ms)
- **`testserv.c`** - Integration tests that fork a server process, connect via TCP, and send protocol commands. These should become tokio-based integration tests.

## Connection Types (bitmask)

- `CONN_TYPE_PRODUCER` (1) - set on first `put`
- `CONN_TYPE_WORKER` (2) - set on first `reserve`
- `CONN_TYPE_WAITING` (4) - set when connection is waiting for a job

## Job States

`Ready` -> `Reserved` -> `Deleted`
              \-> `Buried` -> `Kicked` -> `Ready`
              \-> `Delayed` -> `Ready` (after deadline)
`Reserved` -> `Released` -> `Ready`

## Skill

The `skill/SKILL.md` file is mirrored from `tuber-tui` (tuberq/tuber-rs). When updating it, keep both copies in sync.
