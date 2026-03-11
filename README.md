# tuber

A fast, simple work queue — a Rust rewrite of [beanstalkd](https://github.com/beanstalkd/beanstalkd). Wire-compatible with existing beanstalkd clients.

## Quick Start

```bash
# Start the server
tuber server

# Put a job
tuber put "hello world"

# Put jobs from stdin (one per line)
echo -e "job1\njob2\njob3" | tuber put

# Check stats
tuber stats
```

## Features

All the great hits from beanstalkd, plus:

### Weighted Reserve

By default, `reserve` picks the highest-priority job across all watched tubes (FIFO). Switch to weighted mode and each tube is chosen randomly in proportion to its weight:

```text
watch email
watch notifications 2
watch another-tube 6
reserve-mode weighted
reserve
```

Tubes default to weight 1. Here, `another-tube` is selected 3x as often as `notifications` and 6x as often as `email`.

### Idempotent Jobs

Attach an idempotency key to a `put` to prevent duplicate jobs. If a live job with the same key already exists in the tube, the original job ID is returned instead of creating a new one:

```text
put 100 0 30 5 idp:my-key
<body>
→ INSERTED 1

put 100 0 30 5 idp:my-key
<body>
→ INSERTED 1   (same ID, no duplicate created)
```

The key is scoped to the tube and cleared when the job is deleted, so the same key can be reused afterwards.

### Job Groups

Group related jobs together with `grp:` and chain dependent work with `aft:`. After-jobs are held until every job in the group they depend on has been deleted:

```text
put 0 0 30 11 grp:import
import-row-1
put 0 0 30 11 grp:import
import-row-2
put 0 0 60 14 aft:import
send-summary
```

The `send-summary` job stays held until both `import` group jobs are deleted. Buried jobs block group completion — kick them to let the group finish.

### Concurrency Keys

Limit parallel processing of related jobs. When a job with a `con:` key is reserved, other ready jobs sharing the same key are hidden from `reserve` until the reservation ends (via delete, release, bury, TTR timeout, or disconnect):

```text
put 0 0 30 7 con:user-42
payload1
put 0 0 30 7 con:user-42
payload2
```

Only one `con:user-42` job can be reserved at a time, ensuring serial processing per key.

### Prometheus Metrics

Expose a `/metrics` endpoint for Prometheus scraping:

```bash
tuber server -l 0.0.0.0 -p 11300 -V --metrics-port 9100
```

## Server

```bash
tuber server [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `-l`, `--listen` | `0.0.0.0` | Listen address |
| `-p`, `--port` | `11300` | Listen port |
| `-b`, `--binlog-dir` | — | WAL directory (enables persistence) |
| `-z`, `--max-job-size` | `65535` | Max job size in bytes |
| `-V` | warn | Verbosity (`-V` info, `-VV` debug) |
| `--metrics-port` | — | Prometheus metrics endpoint port |

```bash
# Listen on a custom port with persistence
tuber server -p 11301 -b /var/lib/tuber

# Verbose mode with metrics
tuber server -VV --metrics-port 9100
```

## Put

```bash
tuber put [OPTIONS] [BODY]
```

| Option | Default | Description |
|---|---|---|
| `-t`, `--tube` | `default` | Tube name |
| `-p`, `--pri` | `0` | Priority (0 is most urgent) |
| `-d`, `--delay` | `0` | Delay in seconds before job becomes ready |
| `--ttr` | `60` | Time-to-run in seconds |
| `-i`, `--idp` | — | Idempotency key (prevents duplicate jobs) |
| `-a`, `--addr` | `localhost:11300` | Server address |

```bash
# Put a job on a specific tube with priority
tuber put -t emails --pri 100 "send welcome email"

# Pipe jobs from a file
cat jobs.txt | tuber put -t batch
```

## Work

Reserve and execute jobs as shell commands.

```bash
tuber work [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `-t`, `--tube` | `default` | Tube to watch |
| `-j`, `--parallel` | `1` | Number of parallel workers |
| `-a`, `--addr` | `localhost:11300` | Server address |

```bash
# Process jobs from the "emails" tube with 4 workers
tuber work -t emails -j 4
```

## Stats

```bash
tuber stats [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `-t`, `--tube` | — | Tube name (omit for global stats) |
| `-a`, `--addr` | `localhost:11300` | Server address |

## Building

```bash
cargo build --release
```

The binary will be at `target/release/tuber`.

## Protocol

Tuber speaks the [beanstalkd protocol](https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt), so any beanstalkd client library works out of the box.

## License

MIT — see [LICENSE](LICENSE).

Originally created by Keith Rarick and contributors. The original beanstalkd is licensed under the [MIT License](https://github.com/beanstalkd/beanstalkd/blob/master/LICENSE).
