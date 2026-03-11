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

- **`src/main.rs`** - CLI entry point using clap. Parses args (`-l` listen addr, `-p` port, `-b` binlog dir, `-z` max job size) and starts the server.
- **`src/server.rs`** - Tokio TCP server, accepts connections and dispatches to protocol handler.
- **`src/protocol.rs`** - Beanstalkd text protocol parser and command dispatch. The protocol is line-based (`\r\n` terminated). See `tmp/prot.c` for the full C implementation.
- **`src/job.rs`** - Job struct and job storage (hash table). Jobs have states: Ready, Reserved, Buried, Delayed. Jobs are ordered by priority then ID.
- **`src/tube.rs`** - Named queues ("tubes"). Each tube has a ready heap, delay heap, buried list, and waiting connections list. Default tube is "default".
- **`src/conn.rs`** - Per-connection state: current tube (`use`), watched tubes (`watch`), reserved jobs list, and read/write buffers.
- **`src/heap.rs`** - Binary min-heap used for ready queue (priority ordering), delay queue (deadline ordering), and connection timeouts.
- **`src/wal.rs`** - Write-ahead log for persistence (optional, enabled with `-b`).

## Key Constants from Original C (dat.h)

- `MAX_TUBE_NAME_LEN`: 201 (tube name max is 200 chars)
- `URGENT_THRESHOLD`: 1024 (jobs with pri < 1024 are "urgent")
- `JOB_DATA_SIZE_LIMIT_DEFAULT`: 65535 bytes
- `JOB_DATA_SIZE_LIMIT_MAX`: 1GB (1073741824)
- Default port: 11300
- WAL version: 7

## Beanstalkd Protocol Commands

Commands to implement (from `tmp/prot.c`): `put`, `reserve`, `reserve-with-timeout`, `reserve-job`, `delete`, `release`, `bury`, `kick`, `kick-job`, `touch`, `peek`, `peek-ready`, `peek-delayed`, `peek-buried`, `use`, `watch`, `ignore`, `stats`, `stats-job`, `stats-tube`, `list-tubes`, `list-tube-used`, `list-tubes-watched`, `pause-tube`, `reserve-mode`, `quit`.

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

`Invalid` -> `Ready` -> `Reserved` -> `Deleted`
                    \-> `Buried` -> `Kicked` -> `Ready`
                    \-> `Delayed` -> `Ready` (after deadline)
`Reserved` -> `Released` -> `Ready`
