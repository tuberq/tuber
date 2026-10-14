# Authentication

How tuber gets an authenticated identity without implementing any
authentication. The short version: **tuber does no cryptography.** Identity
arrives as HTTP headers from a forward-auth proxy, and tuber's only two jobs
are to be unreachable except through that proxy, and to fail closed when the
headers are absent.

> **Status:** design. The transport has been verified against Caddy 2.11.4
> (results below); nothing here is implemented yet. Items marked *(clinch)*
> are changes needed in the identity provider, not in tuber.

## The shape

```
  ┌─ tuber CLI / tuber-tui ─┐
  │  speaks the upgrade     │──TLS──┐
  └─────────────────────────┘       │
                                    ▼
  ┌─ beaneater / greenstalk ┐   ┌───────┐  forward_auth  ┌────────┐
  │  raw TCP, unmodified    │──▶│ Caddy │───────────────▶│ clinch │
  │  via `tuber proxy`      │   └───────┘                └────────┘
  └─────────────────────────┘       │ 101 + Remote-User/Remote-Groups
                                    ▼
  ┌─ same-host workers ─────┐   ┌───────┐
  │  plain TCP, loopback    │──▶│ tuber │
  └─────────────────────────┘   └───────┘
```

Two listeners, side by side:

| Listener | Bind | Auth |
|---|---|---|
| `-l/-p` (11300) | trusted network only | none — as today |
| `--upgrade-listen` | **loopback or unix socket** | trusted headers from the proxy |

## Why not OIDC in tuber

The obvious design — hand tuber an OIDC issuer URL and let it validate JWTs —
was evaluated and rejected, for the same reason `metrics.rs` hand-rolls an HTTP
server and the tree has no error-reporting SDK: the dependency is out of
proportion to the value.

| | OIDC in tuber | Forward-auth |
|---|---|---|
| Validates the credential | tuber | clinch |
| TLS | new rustls dep in the server | Caddy already terminates it |
| Key material | JWKS fetch, rotation, HTTP+TLS client | none |
| Token expiry mid-connection | re-auth protocol, blocked-reserve edge cases | checked once, at the upgrade |
| Authorization input | parse and verify claims | read two headers |
| New server dependencies | ~100 crates | **zero** |

Forward-auth also puts revocation in the right place: a key disabled in clinch
stops working on the next connection, with no TTL lag and nothing cached in
tuber.

## The transport: HTTP Upgrade

The connection begins life as an HTTP request so the proxy has something to
authenticate, then becomes the ordinary line protocol on the same socket.

```
GET /tuber HTTP/1.1
Host: tuber.example.com
Connection: Upgrade
Upgrade: tuber
Authorization: Bearer clk_xxxxxxxx

HTTP/1.1 101 Switching Protocols
Upgrade: tuber

use orders\r\n          ← protocol starts here, byte-for-byte unchanged
USING orders\r\n
```

**Verified:** Caddy's `reverse_proxy` passes a non-websocket `Upgrade: tuber`
straight through, returns the 101, and gives a raw bidirectional stream. No
websocket framing is required — no RFC 6455 masking, fragmentation or
ping/pong, and no `tokio-tungstenite`.

Because the socket *is* the session, every piece of connection-scoped state —
`use`, `watch`, `reserved_jobs`, `min_deadline`, `DEADLINE_SOON` — keeps
working exactly as it does on a plain TCP connection.

### Client status branches

| Status | Meaning | Client action |
|---|---|---|
| `101` | authenticated, upgraded | speak the protocol |
| `401` | missing, expired or revoked credential | **fail fast** — do not retry, do not back off |
| `403` | authenticated, not permitted on this host | fail fast |
| `302` / `200` | the proxy wanted a browser session | fail with "use an API key" *(clinch: see Open questions)* |

The 401 case must bypass the reconnect budget in `cmd_work.rs`. Today every
error increments `failures` and backs off; retrying a bad token ten times over
thirty seconds and then reporting "giving up after 10 consecutive failures"
hides the actual cause. (`MAX_CONSECUTIVE_FAILURES` is 10 with a 1s→30s
exponential backoff, so a bad token burns about two and a half minutes before
saying anything useful.)

## What the proxy sends the auth service

Observed, in full, for a `forward_auth` subrequest:

| Header | Value | Note |
|---|---|---|
| `Authorization` | `Bearer clk_…` | forwarded verbatim |
| `Accept` | `application/json` | **forwarded verbatim** — content negotiation is viable |
| `User-Agent` | `tuber-cli/0.13.0` | forwarded verbatim |
| `X-Forwarded-Host` | `tuber.example.com` | **a copy of the client's `Host`** |
| `X-Forwarded-Proto` | `http` | |
| `X-Forwarded-Uri` | `/tuber` | |
| `X-Forwarded-Method` | `GET` | |
| `Upgrade`, `Connection` | `tuber`, `Upgrade` | the upgrade intent is visible to the auth service |

`X-Forwarded-Host` is copied from the request's `Host` header, not derived from
the socket — a portless `Host` yields a portless `X-Forwarded-Host` on the same
listener. On `:443` it therefore compares cleanly against an OIDC `resource` of
`https://tuber.example.com` with no port arithmetic; the port appears only when
a client actually sent one, which in practice means development.

## Deployment invariants

Both are properties of the deployment, not of the code. Tuber cannot enforce
either, which is why they are stated here rather than left implied.

### 1. Tuber's upgrade listener must be unreachable except through the proxy

Trusted-header auth means the headers **are** the credential. Connecting
directly to the upgrade listener and supplying `Remote-User: root` was tested
and yields exactly that:

```
$ (direct to the upgrade listener, no proxy)
  Remote-User: root
  Remote-Groups: admin
→ HTTP/1.1 101 Switching Protocols
  WHOAMI root groups=admin
```

Bind to `127.0.0.1` or a unix socket. Never to `0.0.0.0`.

### 2. The proxy's site block must be host-matched

`X-Forwarded-Host` is client-controlled, so the site block decides whether the
identity provider can be lied to about which application is being reached:

| Site block | A forged `Host: evil.example.com` |
|---|---|
| `:9080 { … }` — port-only catch-all | **forwarded verbatim** to the auth service |
| `http://tuber.example.com:9080 { … }` — host-matched | **empty 200 from Caddy**; reaches neither the auth service nor tuber |

Under a catch-all, the identity provider resolves its application — and
therefore its access rules — from a string the client chose, while the proxy
still routes to tuber. Authorization gets evaluated against one application and
access granted to another. Always name the host.

## Caddyfile

```caddyfile
http://tuber.example.com {
	forward_auth clinch:3000 {
		uri /api/verify
		copy_headers Remote-User Remote-Groups Remote-Email
	}

	reverse_proxy 127.0.0.1:11301
}
```

`copy_headers` **replaces** rather than merges. Two cases were tested and both
fail closed:

- client sends a valid token *and* a forged `Remote-User` → the forgery is
  overwritten with the authenticated identity
- the auth service returns `200` but sets no `Remote-*` at all → the client's
  forged headers are **deleted**, and tuber sees nothing

## Identity on the connection

`Remote-User` and `Remote-Groups` land on `ConnState` at upgrade time and are
fixed for the life of the connection. The hot path then costs a bitmask check,
not a signature verification.

```rust
pub struct ConnState {
    // …
    pub user: Option<String>,
    pub groups: Vec<String>,
}
```

**Absent groups mean no permissions, never all permissions.** This is not
theoretical: an access token lacking `groups` scope produces a successful
authentication with no `Remote-Groups` header, and per the `copy_headers`
behaviour above tuber sees an empty value rather than a forged one. A
connection with no groups must be able to do nothing that a group gates.

Start with two levels and grow later:

| Level | Commands |
|---|---|
| authenticated | the ordinary producer/worker surface |
| admin group | `drain`, `undrain`, `flush-tube`, `flush-buried`, `pause-tube` |

Per-tube scoping (`use`/`watch` against a pattern from the group claim) is the
natural third level. Retrofitting a permission check into thirty-odd command
handlers is far easier if the seam exists from the first commit, so add the
check site even while it always returns "allowed".

## Credentials

| Client | Credential | Why |
|---|---|---|
| `cmd_work` in a container, CI | clinch API key (`clk_…`) | no browser, no refresh, revocable, optionally non-expiring |
| `tuber` CLI on a laptop, tuber-tui | OIDC device flow *(clinch)* | no pasting; refresh handled by the token store |

Tuber never parses the credential. It is an opaque bearer string forwarded to
clinch, so token format changes need no tuber release.

**v1 ships with API keys only.** The full path has been verified end to end
against unmodified clinch with a `clk_` key, so none of tuber's work is blocked
on the device-flow items below.

### Device flow *(clinch)*

Device flow is a conversation between the **client and clinch directly**. The
proxy is not involved and cannot be: `forward_auth` is a single yes/no
subrequest with nowhere to put an interactive exchange.

```
$ tuber stats -a https://tuber.example.com
  no token in --token, $TUBER_TOKEN, or ~/.config/clinch-cli/tuber.json

  POST https://auth.booko.info/device_authorization      ← client ↔ clinch
  ← user_code=ABCD-EFGH  verification_uri=…/device

  Visit https://auth.booko.info/device and enter code ABCD-EFGH

  poll /token until approved → ~/.config/clinch-cli/tuber.json (0600)

  GET /tuber  Authorization: Bearer <token>              ← the upgrade, as above
```

The CLI must request `groups` scope, or it authenticates successfully into a
connection that can do nothing.

### Token resolution order

1. `--token <value>` — scripts, CI
2. `TUBER_TOKEN` — containers, systemd units
3. `~/.config/clinch-cli/tuber.json` — written by `ClinchCli::TokenStore`, 0600
4. otherwise: error naming all three, plus the device-flow hint if discoverable

Re-read the token on **every** reconnect rather than caching it at startup, so
a rotated key takes effect without restarting the worker.

## Client addressing

| `--addr` | Path |
|---|---|
| `localhost:11300` | plain TCP, today's behaviour, no auth |
| `https://tuber.example.com` | TLS → proxy → forward-auth → upgrade |
| `http://tuber.internal:8080` | same, no TLS — co-located proxy, and testing |

Bare `host:port` keeps its current meaning, so nothing existing breaks.

TLS is terminated by the proxy, which means the **client** speaks TLS —
`tokio-rustls` + `webpki-roots`, roughly 25–30 crates. It is worth a cargo
feature; the `http://` form needs none of it, which is what makes the test rig
cheap.

### `tuber proxy` — for clients that cannot change

```
tuber proxy --listen 127.0.0.1:11300 \
            --remote https://tuber.example.com \
            --token clk_xxx
```

Accept a plain local connection, open one upgraded session upstream, then
`copy_bidirectional` until either side closes. Strictly one upstream connection
per local connection: reserved-job ownership is per-connection, and
multiplexing would scramble it.

beaneater, greenstalk, go-beanstalk and every other beanstalkd library then
connect to `localhost:11300` unaware any of this exists. Same shape as
cloud-sql-proxy. This is what keeps "tuber grew authentication" from meaning
"tuber broke every existing client".

## Implementation notes

**The upgrade carry-over.** After consuming headers up to `\r\n\r\n`, the
`BufReader` may already hold bytes beyond it. The *same* reader must be handed
to the protocol layer — constructing a fresh one after the 101 silently eats
whatever was already buffered. This applies on both sides of the upgrade. It is
the classic upgrade bug and it only appears under load, when the next bytes
happen to coalesce into the same segment as the header block.

**The enabling refactor.** `handle_connection` (`src/server.rs`) takes a
concrete `TcpStream` and calls `.into_split()`; `TuberClient` (`src/client.rs`)
stores `BufReader<OwnedReadHalf>` and `OwnedWriteHalf`. Both need to be generic
over the stream — boxed trait objects are simplest, and one vtable dispatch per
read is invisible next to the syscall. Every other method in `client.rs`
(`put`, `reserve_with_timeout`, `touch_all`, …) is untouched, since they only
ever touch `self.reader` and `self.writer`. This also unblocks unix sockets.

**Never log the credential.** The `-V` tracing paths and any `BAD_FORMAT` echo
must redact `Authorization`.

**Caddy gotcha.** Naming a site on a non-80 port makes Caddy treat it as TLS
*even with `auto_https off`*, answering plain HTTP with `400 Client sent an
HTTP request to an HTTPS server`. The site address needs an explicit `http://`
prefix.

## Open questions *(clinch)*

None of these block tuber's implementation; all of them block device flow.

1. **`/api/verify` accepts `clk_` API keys only.** `authenticate_bearer_token`
   does one `ApiKey.find_by_token` lookup, so an OIDC access token returns 401.
   Adding a fallback is additive, but the lookup is the easy half: `ApiKey` and
   `OidcAccessToken` hang off different kinds of `Application` row, and
   forward-auth's access control lives in that binding. The domain pin has to
   come from `resource` and be mandatory (an unbound token would be a universal
   forward-auth pass, strictly weaker than a `clk_` key); authorization must be
   resolved from `X-Forwarded-Host`, not from the token's own application; the
   CLI's client must be re-checked at use time; and scope must gate which
   `Remote-*` headers are emitted.

2. **No `WWW-Authenticate`, and no 401 on the no-token path.** With no
   `Authorization` header, `/api/verify` falls through to the cookie branch and
   issues a `302` to `/signin`, which the proxy passes to the CLI verbatim —
   a redirect to an HTML login page. Both halves need fixing: content-negotiate
   on `Accept` (confirmed to arrive intact) so a non-browser gets `401`, and
   emit a challenge header. `resource_metadata` per RFC 9728 §5.1 is the
   standardised spelling and buys off-the-shelf OAuth 2.1 clients for one extra
   GET; `authorization_uri` alongside it is the zero-hop fast path. Unknown
   auth-params are ignored, so both can be sent.

   The metadata document must be served from tuber's origin to be trustworthy,
   and must not itself be forward-authed or discovery 401-loops:

   ```caddyfile
   handle /.well-known/oauth-protected-resource* {
       reverse_proxy clinch:3000
   }
   ```

   Tuber then serves nothing and knows nothing.

3. **Keep `clk_` regardless.** Access tokens expire hourly and need refresh. For
   a long-lived worker — or rclone against WebDAV — a non-expiring key pinned to
   one application is the right object.

## Verified behaviour

Against Caddy 2.11.4, with stand-ins for clinch's `/api/verify` and tuber's
upgrade listener.

| # | Case | Result |
|---|---|---|
| 1 | Valid token | `101`, identity `dkam` / `admin,family` |
| 2 | No token | `401`, auth service's JSON body passed through |
| 3 | Revoked token | `401` |
| 4 | Second identity | `101`, identity `alice` / `family` |
| 5 | Direct to upstream, forged `Remote-User` | `101` as `root` — **invariant 1** |
| 6 | Valid token + forged `Remote-User` via proxy | forgery overwritten |
| 7 | Auth returns `200` with no `Remote-*`, client forges | forged headers deleted |
| 8 | `Accept`/`User-Agent` reaching the auth service | forwarded verbatim |
| 9 | `Host` with and without port | `X-Forwarded-Host` mirrors it exactly |
| 10 | Auth service `302` | passed to the client verbatim, `Via: 1.1 Caddy` |
| 11 | Forged `Host`, host-matched site block | empty `200` from Caddy; reaches nothing — **invariant 2** |
