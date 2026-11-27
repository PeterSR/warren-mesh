# The service socket API (`/v1/service`)

This is the contract between Warren and a **service**: a program that
becomes service S at one relay by reaching S's unix socket. Any language
works, since the API is plain HTTP/1.1. A Go client is in the package
`github.com/PeterSR/warren-mesh/client`; the suite in `conformance/` checks a
Warren against everything below; `warren dev` runs real Warren nodes for you
to test against (see the end).

The contract is public and additive within v1: new optional fields (ignore
the ones you do not know) and new endpoints (an older Warren answers
`unsupported`, see 1) may appear; nothing here changes meaning.

Contents: 1 Conventions, 2 Errors, 3 Identity and status, 4 Leases, polls and
answers, 5 Calling, 6 Entries and the view, 7 Delivery semantics, 8 Grammar,
9 Limits and timers, 10 Trying it with curl, 11 Testing against Warren.

## 0. Words

- A **node** is one Warren (one per machine), named by a label such as
  `server`. A **relay id** names a place on it: the node itself
  (`server`) or a **child**, `<label>.<node>` (`sandbox.server`).
  A child is a directory under the node's `attach.dir`; its sockets are in it.
- A **service** is a name and a major version. The socket
  `<services.dir>/<S>.sock` makes the process behind it service S at the
  node; `<attach.dir>/<label>/<S>.sock` makes it service S at that child. One
  socket is one trust domain: anything that can connect to it is S at that
  relay. Who can is decided by directory permissions and an optional `uid`
  checked by Warren with SO_PEERCRED. On the node, a socket of a service without
  a `uid` is always 0600, and `services.dir` is 0700, or 0711 when some node
  service names a `uid` (that socket is 0666 with the uid check), so no other
  user can take the lease of a service without one. Warren creates sockets
  with a staging directory beside them, so it needs permission to create a
  directory there. A client may check the other way round
  that the socket is served by the expected Warren user: set `WARREN_UID` (the
  Go client does so when the variable is set).
- A managed service is started by Warren with `WARREN_SERVICE_SOCKET` set to its
  socket; an external one is started by something else and is told the path.
- An **entry** is a named thing a service publishes (an agent, say), with a
  small piece of JSON. Its address is `<name>@<namespace>`.
- A **call** is one request and one response, each at most 1 MiB, from service
  S at one relay to service S at another relay or entry, in the same mesh. A
  call always goes to the *same service name* (and version). There are no
  calls between different services.
- The **origin** of a call is the relay id of the socket it was made on.
  `allow` (a list of exact relay ids, `*.<node>` for every child of that node
  but not the node itself, or `*`; omitted means nobody) is decided on the
  origin, on the target's node.

## 1. Conventions

- HTTP/1.1 over the unix socket. The host name is not looked at.
- Every request carries the header `X-Warren: 1`. A request without it is a
  `bad-request`.
- Request bodies need a `Content-Length`. Chunked bodies are refused
  (`bad-request`).
- Parameters are in the query, URL-encoded. Integers are decimal, without
  sign, space or fraction. `wait` is in whole seconds, 0 to 25, default 25.
- Requests and answers are bodies and statuses only. A caller's request
  headers never reach the server, and the server's answer reaches the caller
  as a body only, with status 200. Only the headers listed in this document
  mean anything; others are ignored.
- A socket accepts at most 64 connections at once. A connection beyond that
  is closed at once, without an answer. Keep-alive connections that stay idle
  for 2 minutes are closed by Warren, and a request needs its headers within
  10 seconds; a request body must arrive within 30 seconds of its start
  (otherwise `bad-request`).
- JSON answers have `Content-Type: application/json`. A handed-out call and
  the answer to a call are raw bytes (`application/octet-stream`).
- **Unknown paths.** A path or a method this Warren does not have answers
  404 with `X-Warren-Error: unsupported`. This is frozen in v1, so a newer
  caller can tell "not supported" from `no-target` and `no-service`. It is
  decided before anything else about the request is looked at, so it needs no
  `X-Warren` header.

## 2. Errors

An error is a non-2xx status, the header `X-Warren-Error: <code>` and a JSON
body `{"error": "<code>", "detail": "<text>"}`. The detail is at most 200
bytes, with control and invisible characters removed; it is for people, never
for programs. Codes from a remote Warren are passed on with their code (the
detail is cleaned and capped again at each hop). A code this table does not
have is a newer one: treat it as the nearest meaning you know, or as "give up".

| Code | Status | Meaning | Caller should |
|---|---|---|---|
| `bad-request` | 400 | malformed or missing parameters, no `X-Warren: 1`, a chunked body, a list that does not parse | fix the call |
| `too-large` | 413 | a body over its limit | split it |
| `refused` | 403 | not allowed: no such service at the target, the target's `allow`, a `from` this socket may not use | give up |
| `no-target` | 404 | no such relay or entry known now | give up, or retry later |
| `no-service` | 404 | the target has the service but no live lease; or the lease ended before the call was handed out | retry later |
| `version` | 409 | the target's lease serves another major version | give up |
| `already-served` | 409 | another lease serves this socket | wait, or take over |
| `gone` | 410 | a lease, poll or call that has ended (or a lease token Warren does not know) | drop it |
| `busy` | 429 | a pool on the way is full, or a rate limit | retry with backoff |
| `offline` | 503 | a node on the way cannot be reached; the call was not sent | retry with backoff |
| `not-ready` | 503 | Warren is starting or shutting down | retry shortly |
| `unknown` | 502 | the call was sent or handed out, and its outcome is lost | retry only if idempotent |
| `timeout` | 504 | no answer in time; outcome unknown | retry only if idempotent |
| `unsupported` | 404 | an endpoint this Warren does not have (an older version) | do without it |

`busy`, `offline`, `not-ready` and `no-service` mean the call did not happen.
`unknown` and `timeout` mean it may have (section 7).

**Order of the checks.** A request meets the following in order, and the first
failure is the answer.

1. The path and method (`unsupported`), then `X-Warren: 1` and, for a POST or
   PUT, a `Content-Length` (`bad-request`).
2. The endpoint's own parameters (the order of the sections below).
3. For a call: at the target, in this order: **the service exists** at the
   target relay (`refused`), **`allow`** admits the origin (`refused`), a
   **live lease** (`no-service`), the lease's **version** (`version`), and
   last the **entry** (`no-target`). A relay or node that is not known at all
   is `no-target`; a node that was listed a moment ago and cannot be reached
   is `offline`.

## 3. Identity and status

```
GET /v1/service/self
200 {"relay": "host.server", "node": "server", "service": "chat",
     "flat": true, "entry_namespace": "server", "namespaces": [],
     "boot": "9c1f...", "max_call_body": 1048576}
```

- `relay`: the relay id this socket is at. `node`: its node. `service`: the
  service this socket is.
- `flat`: this is a flat child. A flat child has a relay id of its own, but
  its entries take addresses in the node's namespace (`alice@server`),
  after the node's own, and it is as trusted as the node for names there.
- `entry_namespace`: where this socket's entries go by default: the node
  (`server`) for a socket on the node or on a flat child, the child's
  relay id (`sandbox.server`) for a child.
- `namespaces`: the full namespaces this service may publish under besides
  `entry_namespace` (`["web.server"]`): configured per service on
  the node only. Always a list, empty when there are none.
- `boot` changes on every Warren start. `max_call_body` is the limit of a call
  body, 1 MiB.

```
GET /v1/service/status
200 {"node": "server", "boot": "9c1f...",
     "warren": {"version": "1.0.0", "peer_protocol": [1], "features": []},
     "transport": {"name": "tailscale", "version": "1.0.0"},
     "peers": [{"node": "laptop", "up": true, "version": "1.0.0",
                "peer_protocol": [1], "features": []}],
     "children": [{"label": "host", "flat": true}],
     "services": [{"relay": "host.server", "name": "chat", "lease": true}],
     "link_up": true}
```

`peers` lists the other nodes with their Warren version, peer protocols and
features (a peer entry may carry `error` when its protocol cannot be shared).
`services` is every socket of the node and its children, with whether it has a
live lease. `link_up` says whether the transport link works. More fields may
come; ignore the ones you do not know.

## 4. Leases, polls and answers

```
POST /v1/service/serve?version=<n>[&take=1]  200 {"lease": "<token>", "inflight": <n>}
GET  /v1/service/calls?lease=<t>&wait=<s>    200 one call, or 204 (none came)
POST /v1/service/answer?lease=<t>&id=<id>    204   (body: the answer)
POST /v1/service/answer?lease=<t>&id=<id>&fail=1   204   (no body: give the call up)
POST /v1/service/release?lease=<t>           204
```

A **lease** is the right to serve the socket. A service takes it with `serve`,
polls for calls, answers them, and gives it up with `release`.

### serve

- `version` (required, 1 or more) is the major version of the service this
  process speaks. A call carries a version, and a call to another version
  fails with `version`.
- One lease per socket. `serve` while one is live answers `already-served`.
  `take=1` takes over at once: the old lease ends (4.2).
- At most one `serve` per second per socket, takeovers included: the next is
  `busy`.
- During shutdown: `not-ready`.
- The answer gives the lease token and **`inflight`**: for this socket, the
  number of calls held for the lease or handed out and not yet answered. A
  call beyond it is answered `busy` to its caller. The lease may have at most
  `inflight` polls open; one more poll is `busy`. The default is 16 (4 for a
  child made from the template); the node's config sets it per socket, at most
  **32** (a larger value is refused at load), so a client never needs more than
  32 polls.
- A service always `PUT`s its entries after `serve` (6): a new lease starts
  with no published list of its own.

### calls (the poll)

- `wait` seconds (0 to 25, default 25): the poll waits for a call. 200 gives
  one call; 204 means none came in time. Poll again at once.
- Run several polls in parallel, up to `inflight`, to handle calls in parallel.
- Errors, in order: `bad-request` (a bad `wait`, a missing `lease`),
  `not-ready` (shutdown), `gone` (no such live lease), `busy` (the lease
  already has `inflight` polls open).
- A call comes back as 200 with the raw request body and the headers:

| Header | Meaning |
|---|---|
| `X-Warren-Call` | the call's id, unique per lease; what `answer` takes |
| `X-Warren-Origin` | the relay id the call came from |
| `X-Warren-From` | the entry address it was made on behalf of, if any (`bob@sandbox.server`) |
| `X-Warren-To` | the entry address, or relay id, it was sent to |
| `X-Warren-Method` | the method the caller named |
| `X-Warren-Deadline-Ms` | the time left, in milliseconds, before the caller gives up (at least 1) |

### answer

- The body is the answer, at most 1 MiB. Anything the caller receives is this
  body; status and headers of the answer request are never passed on.
- The lease and the id are checked **before the body is read**: no such live
  lease is `gone` (or `bad-request` with no lease at all), an id that is not
  handed out on this lease is `gone`. While another answer for the same call
  is still being read the answer is `busy`. A body over 1 MiB is `too-large`
  (the call stays open, and can still be answered).
- **Giving a call up.** `fail=1` with no body ends the call at once as
  `unknown` for the caller (it was handed out, and its outcome is lost) and
  frees its place, instead of leaving the caller to wait for its deadline. Use
  it when the service cannot answer (a handler failed). The lease and id are
  checked as for an answer; a body, or any other value of `fail`, is
  `bad-request`. A Warren without this feature would take it as an empty
  answer, so it needs a Warren of this version or later.
- The body is read by the call's deadline at the latest; one that stalls past
  it is dropped. A second answer for the same id, or one after the call's
  deadline, is `gone`. A deadline that passes is `timeout` to the caller.
- Answers are accepted while Warren shuts down and drains (4.3).

### release

`release` ends the lease at once (204). A second `release` of the same token
is `gone`.

### 4.1 Lease rules in one place

A lease stays live while it has a poll open or a call handed out whose
deadline has not passed. It ends **20 s (the lease grace)** after the last of
those, on `release`, on takeover, when the socket is removed (a reload), or at
once when a managed process exits (Warren ends the lease on that service's
socket). A service that is only idle for longer than the grace loses its
lease: keep a poll open.

### 4.2 When a lease ends

- Calls handed out and not answered fail to their caller with `unknown`.
- Calls held for the lease, not yet handed out, fail with `no-service`.
- Its open polls answer `gone`.
- Its entries become stale (6). A `PUT` within the stale window revives the
  same names.
- Every request with that token is `gone` from now on. A lease from before a
  Warren restart is `gone` too: after `gone`, serve again.

A call to a target with a live lease but no poll open is **held** until a poll
comes or the lease ends; to a target with no live lease it fails at once with
`no-service`.

### 4.3 Shutdown

During shutdown Warren stops handing out calls: `serve`, `calls` and `view`
answer `not-ready`, calls held for a poll fail with `not-ready`, and it waits up
to 5 s for calls already handed out to be answered, then ends the leases.

### 4.4 Restarts

`boot` (in `self`, `status` and `view`) changes on every Warren start. After a
restart every lease is gone: a service that sees `gone`, or a changed `boot`,
must `serve` and `PUT` again. While Warren is down, connecting to the socket
fails: retry with backoff.

## 5. Calling

```
POST /v1/service/call?to=<relay id or entry address>&method=<m>&version=<n>
                     [&from=<entry name>[@<namespace>]][&timeout_ms=<t>]
     (body: the request, at most 1 MiB)
200  the target's answer, as the body
```

- Any process on the socket may call, with a lease or without one.
- `to`: a relay id (`sandbox.server`) or an entry address
  (`alice@server`).
- `method`: a label (8). Warren never interprets it.
- `version` is required: the major version of the service called.
- `timeout_ms`: default 30000, from 1 to 300000. The call waits at most that
  long for its answer, then `timeout`.
- `from`, if given, is the entry name (or `name@namespace`) the call is made
  on behalf of. It is any name this socket may publish: in its
  `entry_namespace` or one of its `namespaces`; matching the child's `names`
  patterns, if it has them; not held by an earlier publisher by precedence (6);
  not reserved by the node. Otherwise `refused`. It need not be listed yet: a
  new chat can send before its first `PUT` lands, and an entry cut from the
  node's list can still send. Replies and receipts to a `from` that is not
  listed get `no-target` until it is. The target sees it in full in
  `X-Warren-From`.
- Delivery: a call is carried to the target's node over the transport if the
  target is on another node, and handed to the target's lease there. The
  caller gets the target's answer or an error of section 2 (codes of a remote
  Warren keep their code).
- **Order inside the call.** First the parameters: a missing `to`, a bad
  `method`, a missing or bad `version` or `timeout_ms` are `bad-request` (the
  grammar of `to` itself is checked later, with the target). Then `outflight`
  (the number of calls this socket may have in flight, default 16, 4 for
  template children) is taken, before the body is read: a full socket is
  `busy`. Then `from`, then the target is looked up (the checks of section 2,
  a `to` that is not a relay id or entry address is `bad-request` here)
  *before the body is read*, so an unknown target is `no-target` whatever the
  body is. Then the body is read (`too-large` over 1 MiB), `from` and the
  target are checked again (a name may have been reserved, a target may have
  gone, meanwhile), and the call is queued. The target refuses with `busy` when its lease has
  `inflight` places taken, or when too many of them belong to the caller's
  origin (`per_root`, default 8, 2 for template children: each origin node, or
  each local relay, has its own share).
- The answer to a call is written by the call's deadline plus a grace of 2 s
  (so an answer accepted just before the deadline is still delivered): a
  caller that does not read it loses it, and the `outflight` place is free
  again then.
- A caller that goes away while its call is held takes the call with it. A call
  already handed out runs to its deadline, and its answer is discarded.

## 6. Entries and the view

```
PUT /v1/service/entries?lease=<t>
  {"entries": [{"name": "...", "namespace": "...", "data": <json or null>}],
   "reserved": ["default", ...]}
  200 {"refused": [{"name": "...", "namespace": "...", "reason": "<code>"}]}

GET /v1/service/view?since=<v>&wait=<s>
  200 {"version": "<v>", "boot": "<id>", "ready": true,
       "reserved": {"server": ["default", ...]},
       "lost": [{"name": "...", "namespace": "...", "reason": "<code>"}],
       "entries": [{"address": "...", "origin": "<relay id>", "data": ...,
                    "stale": false}]}
```

### PUT

- Only the lease holder may `PUT` (a missing token is `bad-request`, a token
  that is not live is `gone`). A `PUT` **replaces** the previous list.
- Checked before the body is read: the lease, then the rate (one `PUT` per
  second per socket, more get `busy`), then that no other `PUT` body is being
  read on this socket (`busy`), then the announced length (over 4 MiB is
  `too-large`). A `PUT` on a flat child may be held (up to 10 s after Warren
  starts) until the node's own managed services have published, and is then
  processed as usual; it is answered `not-ready` only if Warren shuts down while
  it waits.
- `namespace`: empty (the socket's `entry_namespace`), or `entry_namespace`, or
  one of `namespaces`. `data`: any JSON value or null (absent is null), at most
  3 KiB measured in compact form. At most 1000 entries and 64 reserved names
  per `PUT`; the body at most 4 MiB; duplicate names in a list, a body that is
  not that JSON, a bad reserved name, and data over its limit are `bad-request`.
- Order by importance: lists are cut from the tail.
- `reserved`: names kept in the service's own namespace for entries to come.
  Only services on the node itself reserve; a child's reservations are
  ignored and reported in `refused` with the reason `not-allowed`.
- The answer's `refused` lists what is not published, with a reason, and, at
  once, the entries of this socket that precedence displaces right now.
- Changes reach other nodes debounced by 1 s.

Reasons: `held-by-node` (the node's own service holds the name),
`held-by-earlier-child` (an earlier flat child holds it), `reserved` (the node
reserves it), `namespace` (not a namespace this socket may publish in),
`grammar` (not an entry name), `cut` (over the child's `max_entries`, 100 for
template children and 1000 otherwise, or the node's list of 1000 per service),
`not-allowed` (the child's `names` patterns do not allow it, or a child
reserved).

### Precedence

In the node's namespace, per service: names the node's service lists or
reserves come first, then the flat children in config order. The names the node
service lists (and its reserved names) of its last `PUT` are persisted, so
after a Warren restart a flat child cannot take them before the node service
has published again. Precedence follows config and persisted names, never
presence: when the earlier holder drops a name, a displaced later entry comes
back without a new `PUT`. Children that are not flat have namespaces of their
own and never compete. Entries are visible to every node, whatever `allow`
says: `allow` gates calls only.

### view

- This service's entries across the mesh (the same service name on every
  node), each with its `origin` (the relay id that published it), the reserved
  names per node namespace, and `lost`: this socket's entries that are
  displaced now.
- `data` is the entry's JSON, compact. `stale` is explained below.
- `ready` is false while a flat child is held back until the node's own
  services have published (at most 10 s after boot); true otherwise.
- `since`: pass the `version` of the last view you have. With `since` empty,
  from another boot, or not the current version, the answer comes at once with
  everything. With the current version it waits up to `wait` seconds (default
  25) for a change, and then answers with the current view, changed or not:
  compare `version` to see. A change of `boot` means Warren restarted: the
  service must `serve` and `PUT` again.
- A bad `wait` is `bad-request`. At most **4 view polls** per
  socket (`busy`). During shutdown: `not-ready`.

### Staleness

An entry becomes **stale** when the node it lives on cannot be reached, or when
its service's lease has ended (a new lease also leaves the list stale until its
service `PUT`s). It is shown `stale: true` for 60 s, then dropped. A `PUT` by a
new lease within that time revives the names. Calls to a stale entry are made as
usual: the target's checks decide.

## 7. Delivery semantics

At most once per call attempt; no ordering between calls; `timeout` and
`unknown` leave the outcome unknown. Methods should be idempotent or
resumable; receivers dedupe by an id of their own, keyed with `from` or the
origin. `busy`, `offline`, `not-ready` and `no-service` happen before the
service sees the call, so they are safe to retry.

## 8. Grammar

- **label**: `[a-z0-9][a-z0-9_-]{0,62}`.
- **relay id**: a label, or `<label>.<node label>`; at most 64 bytes. Compared
  exactly. A **namespace** has the same shape.
- **service name**: a label of at most 32 bytes. **method**: a label.
- **entry name**: `[a-z0-9_][a-z0-9._-]{0,63}`.
- **entry address**: `<entry name>@<namespace>`.
- **allow** patterns: an exact relay id, `*.<node>` (every child of that node,
  not the node), or `*`. `*.<node>` admits children created later. An
  asymmetric `allow` also blocks replies and receipts.
- **names** patterns of a child: shell-style (`ok-*`, `sandbox-?`) matched
  against the whole entry name.

## 9. Limits and timers

| What | Value |
|---|---|
| connections per socket | 64 |
| call body, answer body | 1 MiB each |
| `PUT` body | 4 MiB; 1000 entries; 64 reserved names; data 3 KiB per entry |
| view polls per socket | 4 |
| polls per lease | `inflight` |
| `PUT` rate | 1 per second per socket (`busy`), and one body being read at a time |
| `serve` rate | 1 per second per socket (`busy`) |
| `wait` | 0 to 25 s, default 25 |
| writing a response | 30 s (the call's deadline plus 2 s for the answer to a call) |
| `timeout_ms` | 1 to 300000, default 30000 |
| `inflight`, `per_root`, `outflight` | config per socket; 16, 8, 16 (4, 2, 4 for template children); `inflight` at most 32, and `inflight` + `outflight` + 4 at most 64 (the connections of a socket) |
| lease grace | 20 s |
| stale window | 60 s |
| change reaches other nodes | about 1 s (debounce) |
| shutdown drain | up to 5 s |
| new child directories picked up | within 3 s |

## 10. Trying it with curl

Start the nodes of `examples/dev.yaml` in a directory you know, and set two
sockets: the `chat` service on the node `laptop`, and the `chat` service of its
child `sandbox` (which may only publish entry names that match `bot-*`):

```
warren dev --config examples/dev.yaml --dir /tmp/wd      # in another terminal

w() { curl -sS --unix-socket /tmp/wd/laptop/services/chat.sock -H 'X-Warren: 1' "$@"; }
c() { curl -sS --unix-socket /tmp/wd/laptop/attach/sandbox/chat.sock -H 'X-Warren: 1' "$@"; }

w http://warren/v1/service/self
w -X POST 'http://warren/v1/service/serve?version=1'
# {"lease":"6f0f0b0e-...","inflight":16}
L=6f0f0b0e-...
# A lease with no poll open ends after the 20 s grace: do the next steps in
# time, or keep a poll open.

# publish an entry
w -X PUT "http://warren/v1/service/entries?lease=$L" \
  -d '{"entries":[{"name":"alice","data":{"kind":"demo"}}]}'
# {"refused":[]}

# wait for a call (from another terminal: the call below)
w -i "http://warren/v1/service/calls?lease=$L&wait=25"
# HTTP/1.1 200 OK
# X-Warren-Call: 2d3c...
# X-Warren-From: bot-1@sandbox.laptop
# X-Warren-Method: hello
# X-Warren-Origin: sandbox.laptop
# X-Warren-To: alice@laptop
# ...
# ping

w -X POST "http://warren/v1/service/answer?lease=$L&id=2d3c..." -d 'pong'

# call it, from the child's socket, on behalf of an entry name it may use
c -X POST 'http://warren/v1/service/call?to=alice@laptop&method=hello&version=1&from=bot-1' -d 'ping'
# pong

# the view, then wait for the next change with the version it gave
c 'http://warren/v1/service/view?wait=0'
c 'http://warren/v1/service/view?since=<version from above>&wait=25'

# an error: the code is in the header and the body
c -i -X POST 'http://warren/v1/service/call?to=nobody.laptop&method=m&version=1'
# HTTP/1.1 404 Not Found
# X-Warren-Error: no-target
# {"detail":"no such relay","error":"no-target"}

w -X POST "http://warren/v1/service/release?lease=$L"
```

## 11. Testing against Warren

`warren dev --config dev.yaml` runs several nodes in one process on the
in-memory transport, with real sockets in a temporary directory (or the one
given with `--dir`), and prints the socket of every service of every relay.
The file names the nodes, their children (flat or not, with an optional
template for directories made while it runs) and the services each relay has
with their `allow`, limits and namespaces; see `examples/dev.yaml`. With
`fast: true` the timers (lease grace, stale window, scan, debounce, serve and
`PUT` intervals) are shorter, so tests need not wait; the values in section 9
are the contract's. Services are always external under `dev`: it starts no
processes.

The conformance suite checks a Warren against this document:

```
go test ./conformance                          # this repository's Warren, in process
go test ./conformance -args -warren ./warren   # a binary that supports `warren dev`
go test ./conformance -args -real              # with the real timers (minutes)
```

The Go client (`github.com/PeterSR/warren-mesh/client`) does the lease
management of this document for you: serve, take over, parallel polls up to
`inflight`, answer, release, serve again on `gone`, back off on `busy`, publish
again after every serve and when `boot` changes, a typed error carrying the
code, and a view watcher. A call that was written and then lost (the caller's
context ended, the connection dropped) is returned as `unknown`, like the
server's own; only failures before anything was sent are plain errors.

### In Go tests: `warrentest`

The package `github.com/PeterSR/warren-mesh/warrentest` is `warren dev` as a
library, for the tests of a service written in Go. `warrentest.Start(t, yaml)`
takes a dev configuration, starts the nodes in the test's process on the
in-memory transport, and closes them when the test ends. `Socket(relay,
service)` returns the path to give to `client.Dial`; `Sockets()` lists them all.
Services are external: the test runs them on those paths. Fast timers are
process-wide, so do not mix fast and real-time meshes in parallel tests of one
process.

```go
m := warrentest.Start(t, devYAML)
c, _ := client.Dial(m.Socket("host.laptop", "chat"))
```
