# Connection & File-Descriptor Lifecycle

How tuber spends file descriptors, what happens when it runs out, and what to
watch. The short version: **connections yield to storage.** A refused
connection is a client retry; a TOAST segment that cannot be opened is failed
puts and unreadable job bodies.

## What holds a descriptor

| Consumer | Count | Grows? | Released when |
|---|---|---|---|
| Listener | 1 | no | shutdown |
| Client connection | 1 each | with traffic | peer disconnects, or the server closes |
| TOAST segment | 1 per segment (~64 MiB each) | **yes, with body volume** | compaction unlinks the segment |
| WAL file | 1 per file (10 MiB each) | slowly | GC unlinks the file |
| Metrics listener + scrapes | 1 + in-flight | no | scrape completes |
| Runtime (kqueue/epoll, signals, stdio) | a handful | no | shutdown |

The row that matters is TOAST. `BodyStore` keeps every segment open for the
life of the process (`segments: BTreeMap<u64, Segment>`, each holding an
`Arc<File>`), so descriptor use grows with stored body volume and is only
returned by compaction. A store that has grown to 200 segments is holding 200
descriptors that connections cannot have.

## Connection lifecycle

```
  accept()
     │
     ├─ Err(EMFILE/ENFILE) ─→ warn (≤1/5s) ─→ sleep 50ms ─→ retry   [never fatal]
     ├─ Err(other)         ─→ warn (≤1/5s) ─→ retry
     │
     └─ Ok(socket)
           │
           ├─ FdBudget::try_acquire() fails ─→ close immediately, count refusal
           │
           └─ slot acquired
                 │
                 ├─ set_nodelay, spawn per-connection task
                 ├─ command loop: read line (≤ MAX_LINE_LEN) → dispatch → reply
                 │     ├─ over-long line with no newline ─→ BAD_FORMAT, close
                 │     ├─ put body trailer missing        ─→ EXPECTED_CRLF, close
                 │     └─ idle past --conn-idle-timeout ─→ engine IdleCheck
                 │             ├─ holds nothing ─→ count prune, close (no reply)
                 │             └─ busy          ─→ resume the read
                 └─ task ends ─→ engine Disconnect ─→ FdBudget::release()
```

Two paths deliberately close rather than continue, both because framing is
lost and there is no safe way to find the next command boundary:

- **Over-long command line** — more than `MAX_LINE_LEN` bytes with no newline.
- **Body trailer missing** — the declared `<bytes>` disagreed with what was
  sent. See [Getting `<bytes>` wrong](protocol.md) for why resuming here is a
  command-injection path.

The slot is released when the connection *task* ends, not when the command loop
exits, so it covers the socket's full life including its drop.

## The descriptor budget

The connection ceiling is derived, not configured:

```
max_connections = fd_soft_limit − (toast_segments + wal_files) − FD_SLACK
```

`FD_SLACK` is 64, covering the listener, the metrics listener and its in-flight
scrapes, the runtime's own descriptors, stdio, and the extra segment compaction
holds open while rewriting. It is deliberately generous: over-reserving costs a
few refused connections, under-reserving costs the EMFILE cliff the budget
exists to avoid.

The storage term is republished by the engine tick, so **the ceiling tracks
storage growth automatically**. Verified end to end: writing ~260 MiB of 1 MiB
bodies grew TOAST from 0 to 5 segments, storage descriptors from 1 to 6, and
the ceiling fell by exactly 5. Connections yield 1:1 to storage.

Override with `--max-connections N`, or `--max-connections 0` for unlimited.
Unlimited is also the fallback when `getrlimit` fails — better to serve than to
guess a ceiling.

## Failure modes

**At the ceiling.** The socket is accepted by the kernel — there is no way to
decline in the kernel — then closed immediately with no reply. No reply is
deliberate: the client has sent nothing yet, so an unsolicited line would be
read as the response to whatever command it sends next. Clients see a clean
EOF. Each refusal increments `connections-refused`, and a warning is logged at
most once every 5 s.

**Out of descriptors anyway** (limit lowered at runtime, or another consumer in
the process). `accept()` returns `EMFILE`/`ENFILE`; tuber warns at most once
every 5 s, backs off 50 ms, and retries. It never exits — this was previously
fatal, and 55 connections against `ulimit -n 64` were enough to terminate the
process. The backoff matters: the pending connection keeps the listener
readable, so an immediate retry spins at 100% CPU. Measured at 0.3% CPU while
fully exhausted, with automatic recovery once descriptors free up.

For reference, beanstalkd logs and continues here too (`h_accept`, `prot.c`) —
but without a backoff, so it busy-spins and floods the log for the duration.

## Observability

`stats` fields, all also exported to Prometheus:

| Field | Metric | Meaning |
|---|---|---|
| `fd-soft-limit` | `tuber_fd_soft_limit` | `RLIMIT_NOFILE` soft limit (0 = unknown) |
| `fd-storage-used` | `tuber_fd_storage_used` | descriptors held by TOAST + WAL |
| `fd-connections-used` | `tuber_fd_connections_used` | descriptors held by clients |
| `max-connections` | `tuber_fd_max_connections` | current ceiling (0 = unlimited) |
| `connections-refused` | `tuber_connections_refused_total` | cumulative refusals |
| `connections-pruned` | `tuber_connections_pruned_total` | cumulative idle closes |
| `conn-idle-timeout` | `tuber_conn_idle_timeout_seconds` | pruning period (0 = disabled) |
| `tcp-keepalive` | `tuber_tcp_keepalive_seconds` | keepalive idle time (0 = off) |

**What to alert on:** the *ratio* `tuber_fd_connections_used /
tuber_fd_max_connections` approaching 1, and any rise in
`tuber_connections_refused_total`. Not the raw connection count — because the
ceiling moves as the body store grows, a count that was comfortable last month
can be at the limit today without the client population changing at all.

`tuber_fd_storage_used` climbing steadily is the leading indicator: it means
TOAST is accumulating segments and quietly reducing connection headroom.
Compaction is what returns them.

## Sizing `ulimit -n`

Budget for all three consumers, not just connections:

```
ulimit -n  ≥  peak_connections
            + expected_toast_segments   (total body bytes ÷ 64 MiB)
            + wal_files                 (retained WAL bytes ÷ 10 MiB)
            + 64                        (FD_SLACK)
```

The TOAST term is the one that surprises people: a 16 GiB body store is ~256
segments, so on a 1024-descriptor limit it has already taken a quarter of the
budget. `BodyStore` warns once when open segments reach 75% of the soft limit.

## Idle pruning

The cap bounds the damage but does not reclaim: without pruning, a client that
opens connections and leaves them open holds those slots until it disconnects,
locking others out. `--conn-idle-timeout <secs>` closes connections that have
gone quiet. **Off by default** (`0`), because "quiet" is normal for a lot of
legitimate clients and the cost of getting the period wrong is dropped work.

A connection is pruned when *both* halves agree:

| Checked by | Condition |
|---|---|
| Connection task | No completed command and no byte received for the whole period |
| Engine (`IdleCheck`) | Not parked in `reserve`, and `reserved_jobs` is empty |

The split matters. **A worker blocked in `reserve` is silent but completely
legitimate** — that is the normal steady state for a worker pool, hundreds of
connections sending nothing for hours — and **a worker running jobs it reserved
is silent for as long as the work takes**. A naive "no traffic for N seconds"
sweep would kill exactly the connections most worth keeping and present as
random worker deaths whenever the queue is quiet.

### Why the timer lives in the connection task

Not as a sweep over `ConnState` in the engine tick, for three reasons:

- **A parked `reserve` is excluded structurally.** That task is awaiting the
  engine's reply, watching the socket only for a hang-up (below), so its idle
  timer cannot fire at all — no predicate to remember and get right. (The predicate that suggests itself,
  `!ConnState::is_waiting()`, would not have worked anyway: `CONN_TYPE_WAITING`
  is defined but never set anywhere, so `is_waiting()` is always false. The
  authoritative source for "parked" is the engine's `waiters` list.)
- **A connection that has never sent a command has no `ConnState`.**
  `handle_command` registers lazily, so connect-and-send-nothing — the cheapest
  way to hold a slot — is invisible to anything walking `conns`. The per-task
  timer catches it; the engine answers `true` for an unknown id.
- No per-tick clock scan across every connection.

The deadline runs from the last *completed command*, not the last byte received,
and it covers the body read and the two refused-body drain paths as well as the
command line. Otherwise each of those would be a bypass: a client could hold a
slot forever by dribbling one byte per period, or by sending a single `put`
header and then going silent.

Pruning closes with no reply, for the same reason a refused connection does: the
client is not waiting on anything, so an unsolicited line would be read as the
response to whatever it sends next. Clients see a clean EOF. Each close
increments `connections-pruned`.

### A worker that hangs up while parked

Pruning never closes a parked worker, so something else has to notice when one
dies mid-wait — killed, OOM-killed, restarted by a deploy. While a `reserve`,
`reserve-with-timeout` or `reserve-batch` is parked, its connection task watches
the socket as well as the engine's reply. On EOF or a read error it sends the
engine `Hangup`, which ends the wait with the reserve's empty answer
(`TIMED_OUT`, or `RESERVED_BATCH 0`). The reply is still written, because a
half-closed client (`nc -N`) can read it. The next read then sees EOF and the
connection closes as usual. This matches beanstalkd.

Before this, a dead parked worker kept its slot, fd and waiter entry until a job
arrived. The job was then handed to each dead waiter in turn, and bounced back to
ready from each, before reaching a live worker. Every worker restart left one
such ghost per parked connection.

Bytes that arrive during the wait are the client's next command, not a hang-up.
They stay buffered and run once the reserve is answered.

This relies on the client's host closing the connection. A peer that vanishes
without a FIN or RST is left to TCP keepalive, below.

### Choosing a period

Pick one comfortably longer than the longest gap your legitimate clients leave
between commands. The awkward case is not the worker pool — those are protected
— but a **long-lived producer that puts a job rarely**: a connection held open
for an hourly job looks exactly like an abandoned socket, and gets closed. Most
clients reconnect transparently; ones that don't will surface the close as an
error on their next `put`.

The period also sets a small engine cost. A connection the engine vetoes is
re-checked once per period for as long as it stays silent, so the steady-state
message rate is roughly `busy_silent_connections / period` — negligible at
sensible settings (10k workers on a 60 s period is ~170 msg/s), but worth not
combining a very short period with a very large worker pool, since the engine is
single-threaded. A pruned connection costs one message, once.

## TCP keepalive

Some peers vanish with nothing on the wire: a host loses power or panics, the
network partitions, or a NAT, firewall or load balancer silently drops a quiet
flow. The server's socket then looks healthy forever. A parked worker on such a
peer is never sent anything, so nothing ever fails. It holds its slot and waiter
entry indefinitely. A job handed to it sits reserved until its TTR expires, and
the connection lingers until TCP gives up retransmitting (about 15 minutes on
Linux).

So accepted sockets get TCP keepalive, on by default (`--tcp-keepalive 60`, env
`TUBER_TCP_KEEPALIVE`). After 60 s with nothing received, the kernel probes every
10 s (a sixth of the period) and fails the socket after 6 unanswered probes, so a
dead peer is noticed within about two minutes. The failure arrives as a read
error: a parked reserve ends as above, and any other connection disconnects,
with its reserved jobs going straight back to ready rather than waiting out
their TTR.

Two properties make this the right tool for what idle pruning can't do:

- **No false positives from busy workers.** The probes are answered by the
  peer's kernel, not the client, so a worker silent on a two-hour job is never
  mistaken for a dead one. Only a dead host or a broken path fails.
- **It keeps middleboxes from dropping parked workers.** The default sits under
  the common cloud load-balancer idle timeouts (AWS NLB 350 s, Azure 240 s), so
  a parked worker's flow is never idle long enough to be silently dropped.

The cost is one small packet per idle connection per period: 10k parked workers
at the default is about 170 packets/s.

**The trade-off is the window.** A shorter period notices dead peers sooner but
closes connections across a network outage they would otherwise have survived.
That matters on the client side: the server's close can't reach the client
during the outage, so afterwards a worker blocked in plain `reserve` still
thinks it is parked and waits forever. Workers should use `reserve-with-timeout`
(or enable keepalive on their own sockets), which covers this and a server host
dying without closing its sockets alike.

`0` turns keepalive off. There is rarely a reason to; it is there as an escape
hatch. Values above 32767 are refused at startup, as Linux caps the idle time
there.

Behind a proxy, keepalive covers only the hop to the proxy. The proxy has to
notice dead clients itself; when it does, it closes its connection to tuber and
the parked-worker handling above takes over.
