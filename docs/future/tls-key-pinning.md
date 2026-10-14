# TLS with pinned keys — reaching tuber without a VPN

> **Status: deferred (YAGNI), 2026-10-11.** Tailscale already covers this:
> tuber listens on a trusted network and the VPN supplies both encryption and
> reachability. Captured so the evaluation of Iroh, QUIC and shared passwords
> isn't redone if tuber ever has to face an untrusted network directly.

## Context

Today tuber speaks plain TCP and trusts whoever can reach the port. That is
fine behind Tailscale. Without a VPN, three things are needed:

1. **Encryption** on the wire.
2. **Authorization**: only known clients get in.
3. **Reachability** across NAT, which Tailscale also does today.

Pinned-key TLS solves 1 and 2. It does nothing for 3: the server needs a
reachable address and port, as any public service does.

### Relationship to `docs/auth.md`

`auth.md` is forward-auth: Caddy terminates TLS, clinch validates a bearer
token, and identity arrives as `Remote-User`/`Remote-Groups` headers. That is
the right shape for **humans** — groups, central revocation, device flow.

This note is the shape for **machines**: no proxy, no identity provider, and
nothing running beyond tuber and its clients. It reverses `auth.md`'s "tuber
does no cryptography", but rustls does the cryptography, and it costs 9 crates
rather than the ~100 that in-server OIDC would have cost. The two can coexist,
and both put identity in the same place (`ConnState.user`).

## Options considered

Crate counts are unique crates in the normal dependency tree (`cargo tree -e
normal`). They were measured in a scratch crate holding tuber's own
dependencies (baseline **50**), with each candidate added in turn.

| Option | Version | Crates added | Verdict |
|---|---|---|---|
| Iroh (QUIC peer-to-peer, keys as addresses) | iroh 1.3.0 | **+215** | no |
| QUIC (`quinn`, minimal features, ring) | quinn 0.11.12 | +23 (+31 default) | no |
| Shared password over TLS or QUIC | — | +9 or +23, plus a protocol change | no |
| **TLS over TCP, mutually pinned keys** | tokio-rustls 0.26.6, rustls 0.23.45 | **+9** | if ever needed |

### Why not Iroh

- **Size:** +215 crates, against the ~100 that ruled out both Sentry and
  in-server OIDC. The added crates include `reqwest`, `portmapper` and
  `netwatch`.
- **Not hostname TLS:** endpoints are raw Ed25519 keys inside QUIC, so a
  standard TLS client cannot talk to it.
- **Still needs a relay:** relays and discovery default to n0's public
  servers. Production means self-hosting `iroh-relay`, which descends from
  Tailscale's DERP. "No VPN" becomes "run a relay".
- **Client reach:** official bindings exist for Python, Node, Swift and
  Kotlin. There are none for Ruby or Go, so beaneater, greenstalk and
  go-beanstalk would need a sidecar regardless.
- **Zero-code trial if it ever matters:** `dumbpipe` in front of an
  unmodified tuber (`listen-tcp` on the server, `connect-tcp` on the client).
  Note that dumbpipe admits anyone holding the ticket.

### Why not QUIC

QUIC's encryption is TLS 1.3, which runs equally well over TCP. What QUIC adds
on top doesn't help tuber:

- **Stream multiplexing:** tuber sessions are already cheap TCP connections.
- **Connection migration and 0-RTT:** these matter for phones changing
  networks, not for queue workers.

What it costs:

- **UDP:** more firewalls block it.
- **Nothing can sit in front of it:** Caddy's `reverse_proxy`, stunnel,
  HAProxy and socat all front TLS over TCP. Over TCP, existing beanstalkd
  clients can be wrapped by off-the-shelf tools.

### Why not a shared password

It looks simpler but isn't.

- **The server still has to prove itself.** A password is only safe if the
  client knows who it is talking to. A self-signed server with verification
  skipped lets anything in the middle collect the password. So clients must
  pin the server's key anyway, which is half of key auth already done.
- **It changes the protocol.** It needs an `auth` command, plus a
  not-yet-authenticated state that every command in `protocol.rs` checks.
  Keys are checked in the TLS handshake, so a client without an allowed key
  never reaches `protocol.rs`.
- **No identity.** You can't revoke one worker or tell who did what, and
  changing the password touches every client.
- **No easier for existing clients.** beaneater et al. speak neither TLS nor
  `auth`, so they need a sidecar either way. A sidecar holding a key file is
  no harder than one holding a password.

## The design

This is the SSH model. Clients pin the server's key the way `known_hosts`
does, and the server lists allowed clients the way `authorized_keys` does.

### Server

```
tuber server -l 127.0.0.1 -p 11300 \
             --tls-listen 0.0.0.0:11443 \
             --tls-cert server.pem --tls-key server.key \
             --tls-clients clients.txt
```

`clients.txt` holds one line per allowed client: a SHA-256 certificate
fingerprint, then a name.

```
# fingerprint (openssl's colon form or bare hex)                          name
3A:7F:...:C1                                                              worker-splat-1
9e04...b2                                                                 dkam-laptop
```

- **Accept what openssl prints.** `openssl x509 -in worker.pem -noout
  -fingerprint -sha256` prints uppercase hex with colons. Parse that, and
  normalise it to bare lowercase hex.
- **Name goes onto the connection.** After the handshake, the matched name
  goes onto `ConnState.user` (`src/conn.rs:27`), the same seam `auth.md`
  plans. Permission levels then work identically for both paths.
- **Plain listener stays private.** The plain `-l/-p` listener must stay on
  loopback or a trusted network, the same invariant as `auth.md`'s upgrade
  listener. The TLS listener is the only one facing out.

### Client

```
tuber stats --addr tls://tuber.example.com:11443 \
            --tls-cert worker.pem --tls-key worker.key \
            --server-fingerprint 5B:02:...:9D
```

Also read `TUBER_TLS_CERT`, `TUBER_TLS_KEY` and `TUBER_SERVER_FINGERPRINT`,
matching how `TUBER_URL` is read today. A bare `host:port` keeps its current
plain-TCP meaning.

### Keys: self-signed X.509, not RFC 7250 raw keys

rustls 0.23.45 does support RFC 7250 raw public keys on both sides
(`AlwaysResolvesServerRawPublicKeys`, `AlwaysResolvesClientRawPublicKeys`).
They are still the wrong choice here:

- **Tool support:** stunnel, `openssl s_client`, Caddy and most client
  libraries don't speak raw keys.
- **Same security model:** a self-signed certificate pinned by fingerprint
  has the identical model. Identity is a key hash, and there is no
  certificate authority.

Generate keys with:

```
openssl req -x509 -newkey ed25519 -days 36500 -nodes \
            -subj /CN=worker-splat-1 -keyout worker.key -out worker.pem
```

The pin is the hash of the whole certificate DER, which is the same thing
openssl's `-fingerprint` prints, so no x509 parsing is needed. The cost is
that re-issuing a certificate for the same key changes its pin. With a
100-year validity there is no reason ever to re-issue.

### Verifiers

Both sides replace the WebPKI verifier with a pinning one. The server side
implements `rustls::server::danger::ClientCertVerifier`:

- `verify_client_cert`: SHA-256 the end-entity DER with ring's digest, which
  adds no extra crate, and look it up in the allowlist. Ignore
  `intermediates`, since there is no chain.
- `verify_tls13_signature`: delegate to `rustls::crypto::verify_tls13_signature`
  with the provider's supported schemes. This proves the peer holds the
  private key; skipping it would make the pin worthless.
- `client_auth_mandatory`: `true`.
- `root_hint_subjects`: empty.

The client side implements the matching `ServerCertVerifier`. Use TLS 1.3
only, which removes the `tls12` feature and the TLS 1.2 signature path.

### Enabling refactor

This is the same one `auth.md` needs. `handle_connection`
(`src/server.rs:5071`) takes a concrete `TcpStream` and calls `.into_split()`.
`TuberClient` (`src/client.rs:6-7`) stores `OwnedReadHalf` and
`OwnedWriteHalf`. Both need to become generic over the stream, or boxed.
Whichever feature lands first pays for it, and the other gets it free.

### Existing clients

- **Sidecar:** `tuber proxy` from `auth.md`, extended with a `tls://`
  upstream, or plain stunnel in client mode.
- **One upstream per local connection:** reserved-job ownership is
  per-connection, so a sidecar must never multiplex.

## Trying it without changing tuber

stunnel on both ends gives this exact model, allow/deny only, with names not
reaching tuber. It shows whether pinned keys are pleasant to operate before
tuber grows any of it. This is an untested sketch; check the stunnel docs.

```ini
; server — tuber stays on 127.0.0.1:11300
[tuber]
accept     = 0.0.0.0:11443
connect    = 127.0.0.1:11300
cert       = server.pem
key        = server.key
verifyPeer = yes
CAfile     = clients.pem      ; every allowed client's self-signed cert, concatenated
```

```ini
; client — beaneater connects to localhost:11300 as before
client = yes
[tuber]
accept     = 127.0.0.1:11300
connect    = tuber.example.com:11443
cert       = worker.pem
key        = worker.key
verifyPeer = yes
CAfile     = server.pem
```

## Open questions

- **Revocation timing.** The options are to re-read `clients.txt` on every
  handshake (it's a tiny file, and needs no reload signal) or to load it on
  `SIGHUP`. Either way, should removing a line drop that client's
  connections that are already open? Doing so releases their reserved jobs.
  Leaving them open means a revoked worker keeps working until it
  reconnects.
- **Cargo feature?** At 9 crates this is borderline. `ring` needs a C
  toolchain at build time, which is the stronger argument for gating it.
- **Metrics.** Possible additions are handshake failures by reason (unknown
  fingerprint, bad signature, protocol error) and connections by identity
  name.
