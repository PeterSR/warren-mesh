# The transport API (`/v1/transport`)

Warren reaches other nodes only through a **transport**: a separate process
that knows the network, carries the peer protocol between nodes, and vouches
for who is calling. Warren contains no network code. This is the contract
between Warren and a transport, for anyone who writes one (`warren-tailscale`
is the reference). The transport conformance harness in
`conformance/transport` checks a transport against it.

A transport never looks inside what it carries. It moves HTTP requests and
answers between Warren on its own node and Warren on another node, and it
proves, from what its network says, which node sent a request.

Contents: 1 Two ways to run, 2 The wire, 3 Link A, 4 Link B, 5 Errors,
6 Requirements, 7 What Warren does with what you say, 8 Testing a transport.

## 1. Two ways to run

Both links speak **HTTP/2 with prior knowledge** (h2c: no TLS, no Upgrade), so
many requests share one connection. There are two links:

- **link A**: the transport serves, Warren is the client. Warren asks it for
  its hello, its peers, and to carry requests to other nodes.
- **link B**: Warren serves, the transport is the client. The transport hands
  Warren the requests that arrive from other nodes.

### Managed

Warren starts the transport (`transport: tailscale`, or
`transport: {run: [...]}`) and hands it two connected unix socket pairs as
**inherited descriptors**: **fd 3 is link A** (the transport serves it) and
**fd 4 is link B** (the transport is its client). No paths exist, so no other
process can reach the link. The connections are made once per start: a managed
link **cannot be dialled again**. When either connection closes, the link is
over: the transport should exit, and Warren starts it again with new
descriptors (backoff 1 s doubling to 60 s). A managed transport also stops on
SIGTERM, which Warren sends at shutdown (after the services; then SIGKILL after
10 s).

The environment is minimal (`PATH`, `HOME`, `USER`, `LANG`, `TZ`,
`XDG_RUNTIME_DIR`). The shipped transports get their block of Warren's config
as JSON in `WARREN_TRANSPORT_SETTINGS`; a third-party transport reads its own
configuration, since `run:` gives it no settings of Warren's.

### External

Something else (systemd, a container) starts the transport, which Warren
reaches through two configured socket paths:

```yaml
transport:
  link_a: /run/transport/a.sock    # served by the transport
  link_b: /run/warren/b.sock       # served by Warren
  uid: 1001                        # the user the transport runs as
```

The transport **must run as a different uid from Warren**, and `uid` names it.
Warren refuses a link where both sides are the same user, serves `link_b`
and closes, at accept, every connection that is not from `uid`
(SO_PEERCRED); it checks that whoever serves `link_a` is `uid` on every
connection it makes. The transport should in turn check that the server it
connects to as link B is Warren's user, and that clients of link A are Warren.
An external link can be dialled again: Warren connects to `link_a` again with
backoff after a failure, and the transport connects to `link_b` again.

## 2. The wire

- Every request and answer is an HTTP/2 message on the link, any `:authority`.
- Paths: `/v1/transport/...` on link A for the transport's own calls, and
  `/v<N>/peer/...` for carried requests. A transport carries **every**
  version `N` of `/vN/peer/`: a Warren that speaks two peer protocols (the one
  release window of a breaking change) needs no change in it.
- Any other path, on either link, answers **404 with
  `X-Warren-Error: unsupported`** and the JSON body
  `{"error": "unsupported", "detail": "..."}`. This is frozen in v1, so a newer
  party can tell "not supported" from other errors.
- Both sides of each link allow at least **1000 concurrent HTTP/2 streams**
  (`SETTINGS_MAX_CONCURRENT_STREAMS`). Warren checks at start that its config's
  need (long polls for peers and services, plus every socket's `outflight` and
  `inflight`) fits.
- Header names used here, all case-insensitive: `X-Warren-Transport-To`,
  `X-Warren-Transport-From`, `X-Warren-Transport-Error`,
  `X-Warren-Deadline-Ms`.

## 3. Link A (Warren to the transport)

### hello

```
GET /v1/transport/hello
200 {"version": 1, "name": "tailscale", "software_version": "1.0.0",
     "node": "server", "max_body": 5242880}
```

- `version`: the transport API version, 1.
- `node`: this machine's node name on the network. It must match the `node:`
  of Warren's config, and must be a label (`[a-z0-9][a-z0-9_-]{0,62}`).
- `max_body`: the largest request body the transport carries, in bytes. It
  must be at least **4 MiB + 64 KiB** (4259840 bytes), the largest peer list
  answer plus headers.
- `name` and `software_version` are optional; Warren shows them in status.
- Warren **refuses** the transport, loudly, when `version` differs, when `node`
  differs from its own, or when `max_body` is below the minimum. At start that
  stops Warren; later (a restarted transport) it marks every peer down and
  logs. A hello that is **not answered** (the network is down, tailscaled is
  logged out) is not a refusal: Warren starts with the link down, its local
  services work, and it keeps asking. Answer hello as soon as the link is
  served; if the node name is not known yet, wait for it (up to a few seconds)
  rather than saying a wrong one.

### peers

```
GET /v1/transport/peers?since=<v>&wait=<s>
200 {"version": "<v>", "peers": ["laptop", "desktop"]}
```

The other nodes on the network that are reachable now, by node name (labels),
never this node. A long poll: `version` is any string that changes when the
list changes; with `since` equal to the current version the answer waits up to
`wait` seconds (whole seconds, 0 to 25, default 25) for a change and then
answers with the current list, changed or not; with `since` empty or different,
or `wait=0`, it answers at once. A `wait` that is not a whole number from 0 to 25
is a `bad-request`. A peer that is no longer reachable must leave
the list, so that Warren sees it down instead of failing calls one by one.

### carrying a request

Any request on a path `/v<N>/peer/...` with the header
`X-Warren-Transport-To: <node>` is carried to that node, and the answer is
returned.

The transport carries, in the request: the method, the path and query, the
body, `Content-Type`, and **every `X-Warren-*` header except
`X-Warren-Transport-*`**; and back, in the answer: the status, `Content-Type`,
every `X-Warren-*` header except `X-Warren-Transport-*`, and the body. Nothing
else is carried (no cookies, no `Authorization`, no other headers), in either
direction.

Every carried request has `X-Warren-Deadline-Ms: <ms>`, the time Warren will
wait. The transport **must not impose a shorter timeout of its own**, and it
**abandons the request when Warren resets the stream** (the cancelled stream is
how Warren enforces the deadline). It passes `X-Warren-Deadline-Ms` on to the
other node, reduced by the time spent if it can.

A request with a missing or invalid `X-Warren-Transport-To`, or to a node that
is not a peer, is not sent (see errors). Methods are not restricted: GET, POST,
PUT and the rest are carried as they are.

`X-Warren-Transport-To` and the other `X-Warren-Transport-*` headers belong to
one hop. They are never carried; the transport on the far side sets its own.

## 4. Link B (the transport to Warren)

- Only `/v<N>/peer/...` requests, each with `X-Warren-Transport-From: <node>`,
  **set by the transport from what its network proved**, never from what the
  sender wrote. Warren checks that it is a label and not its own node name
  (`bad-request` otherwise), and answers `unsupported` for any other path and
  `not-ready` (503) before it has started serving.
- The transport **strips every `X-Warren-Transport-*` header that arrived over
  the network** before it sets its own, so a peer cannot forge `From` or smuggle
  a hop header. It hands over the method, path and query, body, `Content-Type`
  and the `X-Warren-*` headers, and nothing else.
- What Warren answers (status, `Content-Type`, `X-Warren-*` except
  `X-Warren-Transport-*`, body) goes back to the sender over the network. Every
  peer answer carries `X-Warren-Node`, which must equal the node the sender's
  transport reached; the transport just carries it.
- A transport refuses callers its network does not vouch for (an untagged
  device, another tailnet, a stranger) before it reads a body.
- Warren's answers on link B can be slow: peers poll Warren with long polls
  (hello, the services list, the entries lists) that wait up to 25 s.

## 5. Errors of the transport itself

A failure of the transport to carry a request is an answer with the header
**`X-Warren-Transport-Error`**, a JSON body `{"error": "<kind>", "detail":
"<text>"}`, and one of these statuses. An answer **without** the header is the
other node's own answer (whatever its status), carried unchanged: an `X-Warren-Error`
in it is Warren's, not the transport's.

| `X-Warren-Transport-Error` | Status | When | Warren treats it as |
|---|---|---|---|
| `not-sent` | 502 | the request provably never left: no connection, no responders, the node is not a peer | the call was not sent: `offline` (safe to retry) |
| `not-sent` | 413 | the request body is over `max_body` | the same |
| `busy` | 429 | the transport refuses a request for its own limits, before sending it | `busy` (not `offline`; safe to retry) |
| `lost` | 502 | any other failure after the request was sent (connection lost, reset, no answer in time) | outcome unknown: `unknown` |
| `too-big` | 502 | the request was sent and answered, but the answer is over what the transport carries back | `lost` for that request; for a peer list poll, only that list is dropped |

If in doubt between `not-sent` and `lost`, say `lost`: `not-sent` is a promise
that the request never reached the other node, and Warren retries on it.

Limits that make a transport answer `busy` must be counted per caller and in
total, and checked **before the request body is read**. Warren's long polls
(hello, services, entries: `GET` on `/vN/peer/hello`, `/services`, `/entries`)
get places of their own, so a burst of calls never starves them (6).

## 6. Requirements

1. **`prctl(PR_SET_DUMPABLE, 0)` first thing.** Call it before anything else
   that matters, before reading the descriptors' content or opening files. The
   setting does not survive `exec`, so every transport makes this call itself:
   Warren (and `warren-tailscale`) do. It stops other processes of the same
   user from attaching with ptrace or copying the link's descriptors with
   `pidfd_getfd` (Fedora allows same-user ptrace by default). There is a short
   window between `exec` and the call; that is why it is the first thing. (On
   macOS there is no such flag; Linux is the supported target.) The harness
   checks the result: the process's `/proc/<pid>/environ` is not readable by
   another process of the same user.
2. **No idle timeouts on the two connections of a managed link**, on either side
   and at any layer: no HTTP/2 server idle timeout on link A, no
   `IdleConnTimeout` on the client of link B (Go's `http.DefaultTransport`
   closes idle connections after 90 s; a plain `http.Transport{}` does not).
   A managed link cannot be dialled again, and a closed one means a transport
   restart. Keep-alive pings are fine. (Connections to other nodes on the
   network are the transport's business; idle timeouts there are fine.)
3. **Write deadlines on answers sent to the network.** A peer that stops
   reading must not hold a place and an answer forever: end the write at the
   request's own deadline (or a default of about 30 s without one).
4. **Room for Warren's long polls.** Warren already bounds them per peer (at
   most 2 open per list per peer, at most 32 services); the transport keeps
   places for them apart from the places for calls, so that calls refused for
   its own limits (`busy`) never block hello and the list polls.
5. **Strip and set the hop headers**, as in 3 and 4: `X-Warren-Transport-*`
   never crosses the network; `X-Warren-Transport-From` is set from the proof the
   network gives, and only on link B.
6. **No shorter timeout than the deadline**, and abandon what Warren resets (3).
7. Both sides of each link allow at least **1000 concurrent streams** (2).
8. When a connection of a managed link closes or fails, exit; Warren restarts
   the transport. When a connection to another node is known to be stale (a
   resume from suspend), close it and fail what is in flight on it with `lost`.
9. Memory: bound what you hold for peers. Warren's own bounds are 1 MiB for a
   call body and 4 MiB for a list; `warren-tailscale` holds at most 64 MiB of
   request bodies for the tailnet in all.

## 7. What Warren does with what you say

- It asks hello at start (a few seconds) and again after a restart, then
  long-polls `peers` continuously, one poll at a time.
- It carries a call to a peer as `POST /v1/peer/call?...` with
  `X-Warren-Transport-To`, a body of at most 1 MiB, and the deadline of the
  call. It enforces that deadline itself and resets the stream when it passes.
- It long-polls each peer's services list and entries lists through the
  transport, with `wait` of 25 s, so a request can legitimately take that long
  and more: do not time a request out before its `X-Warren-Deadline-Ms`.
- A peer is up while its `hello` is answered; a `not-sent` from the transport
  makes calls to it `offline`; a `lost` makes the call `unknown`.
- When link A or link B closes or fails after start, every peer is marked down;
  a managed transport is restarted with backoff, and an external one is dialled
  again with backoff.
- Warren watches for clock jumps (suspend and resume) and restarts its own long
  polls.

## 8. Testing a transport

`conformance/transport` runs a transport binary the way Warren does: with
descriptors 3 and 4 against a fake Warren, and checks hello (its fields and
Warren's checks), the peers long poll, carrying (methods, paths, the
allow-listed headers both ways, `X-Warren-Transport-*` stripped and set),
`not-sent`, `lost`, `busy` and `too-big`, deadlines (no shorter timeout, the
request abandoned when the stream is reset), `max_body`, that both managed
connections stay open for 95 s without a request, that the process exits
within about 10 s when a managed link connection closes, that it is
non-dumpable, and that a transport keeps room for long polls.

A transport carries requests over a network, so the harness needs a fixture of
one: a Go type with `Join(t, names...) []Spec` that registers nodes on a network
(each reaching the others) and returns the command and environment of a
transport for each. The checks run side by side, so `Join` is called
concurrently and must be safe for that. Optionally the fixture can also
`Leave` (make a node vanish from the others' peer lists), `Inject` (send a raw
request as another node would, to check forged hop headers and non-peer paths)
and `Strangers` (send a request as a caller the network does not vouch for,
such as an untagged device; the harness checks that it is refused, that
nothing it sends reaches Warren, and that the stranger never shows in a peer
list). The test of the package shows the fixture for `warren-tailscale`:
loopback addresses and a fake tailscaled.

```go
func TestMyTransport(t *testing.T) {
	transport.Run(t, myNetwork(t), transport.Options{Burst: 400})
}
```

`Options.Burst` is the number of concurrent requests that exceeds your limits,
to check `busy`; 0 skips that check. `go test -short` shortens the idle (5 s)
and slow-answer (3 s) waits; leave them at their defaults for a real run: the
idle wait must be longer than 90 s to catch a Go default.
