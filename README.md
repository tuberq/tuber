# tuber

An experimental, simple, fast work queue — a Rust rewrite of [beanstalkd](https://github.com/beanstalkd/beanstalkd). Wire-compatible with existing beanstalkd clients.

## Quick Start

```bash
# Start the server
tuber server

# Put a job
tuber put "hello world"

# Put jobs from stdin (one per line)
echo -e "job1\njob2\njob3" | tuber put

# List tubes
tuber tubes

# Check stats
tuber stats
```

## The case for Tuber / Beanstalkd

Redis-backed queues are popular and performant, but Redis isn't a natural fit for job storage. Jobs aren't ephemeral cache entries — they need durability, not RAM. Sizing Redis to hold your entire queue in memory is wasteful when disk storage is perfectly adequate.

SQLite-backed queues are simple and fast, but limited to a single host. PostgreSQL and MySQL-backed queues can scale beyond one host, but a job queue should be separate from your application database for capacity planning — which means another instance to manage with connection pooling, tuning, vacuuming, backups, and restores.

Tuber and Beanstalkd are purpose-built for this. A single binary, easy to deploy in Docker, with optional write-ahead log for durability. No capacity planning, no tuning, no surprises. Workers wait efficiently at any scale.

## Features

All the great hits from beanstalkd - backwards compatible, plus:

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

### Unique Jobs (Idempotency)

Prevent duplicate jobs with an `idp:` key on `put`. If a live job with the same key already exists in the tube, the original job ID is returned instead of creating a new one:

```text
put 100 0 30 5 idp:my-key
<body>
→ INSERTED 1

put 100 0 30 5 idp:my-key
<body>
→ INSERTED 1   (same ID, no duplicate created)
```

The key is scoped to the tube and cleared when the job is deleted, so the same key can be reused afterwards.

#### Cooldown TTL

By default, the idempotency key is removed as soon as the job is deleted. Add a TTL with `idp:key:N` to keep deduplicating for N seconds after deletion:

```text
put 0 0 30 5 idp:report:300
<body>
→ INSERTED 1

(reserve → delete job 1)

put 0 0 30 5 idp:report:300
<body>
→ INSERTED 1   (still deduped — within 300s cooldown)
```

After the cooldown expires, the key is freed and a new job will be created. `idp:key` (no TTL) keeps the original behaviour — key removed immediately on delete.

### Job Groups (Fan-out / Fan-in)

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

Chain stages together by combining `aft:` and `grp:` on the same job — the after-job becomes a member of the next group:

```text
put 0 0 30 5 grp:extract
row-1
put 0 0 30 5 grp:extract
row-2
put 0 0 30 5 aft:extract grp:transform
transform
put 0 0 30 5 aft:transform
load
```

Here `transform` waits for the extract group to finish, then becomes part of the `transform` group. `load` waits for `transform` to complete — giving you a simple DAG pipeline.

Group names are global — jobs in the same group can span multiple tubes. Note that the server does not detect cycles: if two groups depend on each other, the waiting jobs will be held indefinitely. Cycle avoidance is the client's responsibility.



### Concurrency Keys

Limit parallel processing of related jobs. When a job with a `con:` key is reserved, other ready jobs sharing the same key are hidden from `reserve` until the reservation ends (via delete, release, bury, TTR timeout, or disconnect):

```text
put 0 0 30 7 con:user-42
payload1
put 0 0 30 7 con:user-42
payload2
```

Only one `con:user-42` job can be reserved at a time, ensuring serial processing per key.

Set a higher limit with `con:key:N` to allow N concurrent reservations:

```text
put 0 0 30 7 con:api:3
payload1
put 0 0 30 7 con:api:3
payload2
```

Up to 3 `con:api` jobs can be reserved simultaneously. `con:key` (no `:N`) defaults to a limit of 1.

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
| `-i`, `--idp` | — | Idempotency key — `key` or `key:ttl` (TTL seconds keeps deduping after delete) |
| `-g`, `--grp` | — | Group name (for job grouping) |
| `--aft` | — | After-group dependency (wait for this group to complete) |
| `-c`, `--con` | — | Concurrency key — `key` or `key:N` (N = max concurrent reservations, default 1) |
| `-a`, `--addr` | `localhost:11300` | Server address |

```bash
# Put a job on a specific tube with priority
tuber put -t emails --pri 100 "send welcome email"

# Pipe jobs from a file
cat jobs.txt | tuber put -t batch

# Put a job with a concurrency key
tuber put -c deploy "deploy-service-a"

# Put grouped jobs with a dependent follow-up
tuber put -g import "import-row-1"
tuber put -g import "import-row-2"
tuber put --aft import "send-summary"
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

## Tubes

List all tubes with a summary of job counts.

```bash
tuber tubes [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `-a`, `--addr` | `localhost:11300` | Server address |

```bash
$ tuber tubes
default: ready=4 reserved=0 delayed=0 buried=0
my-tube: ready=16 reserved=0 delayed=0 buried=0
```

## Stats

Show global server statistics or per-tube statistics.

```bash
tuber stats [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `-t`, `--tube` | — | Tube name (omit for global stats) |
| `-a`, `--addr` | `localhost:11300` | Server address |

```bash
# Global stats
tuber stats

# Per-tube stats
tuber stats -t emails
```

## Shell Jobs

Tuber's `work` command reserves jobs and executes each job body as a shell command. Combined with `put`, this gives you a simple distributed task runner from the command line.

```bash
# Start a server and two workers
tuber server &
tuber work -j 2 &

# Queue some work
tuber put "echo 'hello world'"
tuber put "curl -s https://example.com/api/webhook -d '{\"event\": \"done\"}'"
```

### Preventing duplicate work with idempotency keys

Use `-i` to ensure a job is only queued once. If a live job with the same key already exists, tuber returns the original job ID instead of creating a duplicate.

```bash
# Transcode a video — safe to retry without double-processing
tuber put -i "transcode-video-42" "ffmpeg -i /data/video-42.raw -c:v libx264 /data/video-42.mp4"

# Generate a nightly report — re-running the script won't queue it twice
tuber put -i "report-2026-03-12" "./generate-report.sh 2026-03-12"

# Keep deduplicating for 5 minutes after the job completes
tuber put -i "report-2026-03-12:300" "./generate-report.sh 2026-03-12"
```

### Serialising work with concurrency keys

Use `-c` to ensure only one job with a given key is processed at a time. Other jobs sharing the key wait in the queue until the current one finishes.

```bash
# Deploy to each host — one deploy per host at a time, but different hosts in parallel
tuber put -c "deploy-web1" "./deploy.sh web1"
tuber put -c "deploy-web1" "./deploy.sh web1"  # waits for the first to finish
tuber put -c "deploy-web2" "./deploy.sh web2"  # runs in parallel with web1

# Crawl pages from a site without hammering it — serialise per-domain
tuber put -c "example.com" "curl -o /data/page1.html https://example.com/page1"
tuber put -c "example.com" "curl -o /data/page2.html https://example.com/page2"
tuber put -c "other.com"   "curl -o /data/index.html https://other.com/"

# Allow up to 3 concurrent API calls per service
tuber put -c "api-svc:3" "curl -X POST https://api.example.com/job1"
tuber put -c "api-svc:3" "curl -X POST https://api.example.com/job2"
tuber put -c "api-svc:3" "curl -X POST https://api.example.com/job3"
```

### Chaining pipelines with groups

Use `-g` to group related jobs and `--aft` to hold a follow-up until the group completes.

```bash
# Import rows in parallel, then send a summary when all are done
tuber put -g import "./import-row.sh 1"
tuber put -g import "./import-row.sh 2"
tuber put -g import "./import-row.sh 3"
tuber put --aft import "./send-import-summary.sh"
```

## Protocol Reference

Tuber speaks the [beanstalkd protocol](https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt), so any beanstalkd client library works out of the box. Commands marked with **⚡** are tuber extensions beyond standard beanstalkd.

All commands are `\r\n`-terminated. `<id>` is a 64-bit job ID, `<pri>` is a 32-bit priority (0 = most urgent), `<delay>` and `<ttr>` are seconds, `<bytes>` is body length.

### Producer commands

| Command | Description |
|---|---|
| `put <pri> <delay> <ttr> <bytes> [tags]\r\n<body>\r\n` | Submit a job. Returns `INSERTED <id>` or `BURIED <id>`. |
| `use <tube>\r\n` | Set the tube for subsequent `put` commands. Returns `USING <tube>`. |

**⚡ Put extension tags** — append space-separated tags after `<bytes>`:

| Tag | Effect |
|---|---|
| `idp:<key>` or `idp:<key>:<ttl>` | Idempotency — deduplicates jobs by key within the tube. Optional TTL (seconds) keeps deduplicating after deletion. See [Unique Jobs](#unique-jobs-idempotency). |
| `grp:<name>` | Assigns the job to a group for fan-out/fan-in. See [Job Groups](#job-groups-fan-out--fan-in). |
| `aft:<name>` | Holds the job until all jobs in the named group are deleted. See [Job Groups](#job-groups-fan-out--fan-in). |
| `con:<key>` or `con:<key>:<limit>` | Concurrency key — limits how many jobs per key can be reserved at once (default 1). See [Concurrency Keys](#concurrency-keys). |

### Worker commands

| Command | Description |
|---|---|
| `reserve\r\n` | Block until a job is available. Returns `RESERVED <id> <bytes>\r\n<body>`. |
| `reserve-with-timeout <seconds>\r\n` | Like `reserve` but times out. Returns `RESERVED …` or `TIMED_OUT`. |
| `reserve-job <id>\r\n` | Reserve a specific job by ID. Returns `RESERVED …` or `NOT_FOUND`. |
| **⚡** `reserve-mode <mode>\r\n` | Set reserve strategy: `default` (priority-first) or `weighted` (random by tube weight). See [Weighted Reserve](#weighted-reserve). |
| `delete <id>\r\n` | Delete a job. Returns `DELETED` or `NOT_FOUND`. |
| `release <id> <pri> <delay>\r\n` | Release a reserved job back to ready (or delayed). Returns `RELEASED`. |
| `bury <id> <pri>\r\n` | Bury a reserved job. Returns `BURIED`. |
| `touch <id>\r\n` | Reset the TTR timer on a reserved job. Returns `TOUCHED`. |
| `watch <tube> [weight]\r\n` | Add a tube to the watch list. Optional **⚡** weight for weighted mode. Returns `WATCHING <count>`. |
| `ignore <tube>\r\n` | Remove a tube from the watch list. Returns `WATCHING <count>` or `NOT_IGNORED`. |

### Peek / inspect commands

| Command | Description |
|---|---|
| `peek <id>\r\n` | Peek at a job by ID. Returns `FOUND <id> <bytes>\r\n<body>` or `NOT_FOUND`. |
| `peek-ready\r\n` | Peek at the next ready job in the used tube. |
| `peek-delayed\r\n` | Peek at the next delayed job in the used tube. |
| `peek-buried\r\n` | Peek at the next buried job in the used tube. |

### Admin commands

| Command | Description |
|---|---|
| `kick <bound>\r\n` | Kick up to `<bound>` buried/delayed jobs in the used tube. Returns `KICKED <count>`. |
| `kick-job <id>\r\n` | Kick a specific buried or delayed job. Returns `KICKED` or `NOT_FOUND`. |
| `pause-tube <tube> <delay>\r\n` | Pause a tube for `<delay>` seconds. Returns `PAUSED`. |
| **⚡** `flush-tube <tube>\r\n` | Delete all jobs from a tube. Returns `FLUSHED <count>`. |
| `stats\r\n` | Server-wide statistics in YAML. |
| `stats-job <id>\r\n` | Statistics for a single job in YAML. |
| `stats-tube <tube>\r\n` | Statistics for a tube in YAML. |
| `list-tubes\r\n` | List all existing tubes in YAML. |
| `list-tube-used\r\n` | Show the currently used tube. Returns `USING <tube>`. |
| `list-tubes-watched\r\n` | List watched tubes in YAML. |
| `quit\r\n` | Close the connection. |

## Building

```bash
cargo build --release
```

The binary will be at `target/release/tuber`.

## License

MIT — see [LICENSE](LICENSE).

Originally created by Keith Rarick and contributors. The original beanstalkd is licensed under the [MIT License](https://github.com/beanstalkd/beanstalkd/blob/master/LICENSE).
