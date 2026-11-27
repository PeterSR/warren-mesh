# Warren architecture

Warren is one daemon per machine. It knows relay ids, entries and calls, and
nothing about any particular program or any particular network. Networks plug
in as **transports**, programs ride on it as **services**; both are separate
processes that talk to Warren over unix sockets or inherited descriptors.
This document describes the concepts, how nodes talk to each other, the
limits, and the threat model. The three public contracts have their own
documents: [service-api.md](service-api.md),
[transport-api.md](transport-api.md) and [control-api.md](control-api.md).

Platforms: Linux is the supported target. Every OS-specific call (peer
credentials, `prctl`, socket creation, `sun_path`) sits in a per-OS file
behind a small function, with a darwin fallback that compiles, and CI builds
and vets for darwin. Nothing is tested or supported on macOS, and Windows is
out of scope. Warren has no systemd code: systemd, launchd or a container's
restart policy is only how it is deployed and restarted.

Non-goals: routing beyond one link between machines, encryption beyond what
the transport gives, streaming, transports or services inside Warren's own
process, nested children.

## Concepts

### Places and relay ids

A machine runs one Warren, the **node**. A **relay id** names a place on it:

- the node itself: `server`;
- a **child**: `<label>.<node>`, such as `sandbox.server` or `host.server`.

A child is a directory, `<attach.dir>/<label>/`, served by the node. It is
mounted into a container (the directory, never the socket file), or used by a
host process next to a node that runs in Docker. Programs there connect to
service sockets in it and are services *of that child*. No Warren runs in a
child.

A **flat** child has its own relay id, but its services' entries take
addresses in the node's namespace (`alice@server`), after the node's own. A
flat child is as trusted as the node for names in that namespace.

Grammar (public contract):

- label: `[a-z0-9][a-z0-9_-]{0,62}`;
- relay id: a label, or `<label>.<node label>`; at most 64 bytes;
- node names come from the transport and must fit the label grammar, or the
  peer is refused.

Relay ids are compared exactly.

### Services

A **service** is a name (label grammar, at most 32 bytes) and a major version
(integer, 1 or more). A process becomes service S at relay R by reaching S's
socket:

- node: `<services.dir>/<S>.sock`;
- child: `<attach.dir>/<label>/<S>.sock`.

Sockets are leaves directly in a directory. Warren makes a directory inside a
socket's directory only for a moment: a private (0700) staging directory
beside a socket whose mode is closed, and a quarantine directory beside a
stale socket it replaces, both removed again (so it needs permission to create
a directory there, a child's directory included); it creates sockets without
following symlinks and with owner checks, and checks the path against `sun_path` (108 bytes) at
load. The host-side path a child's process uses may differ in length; the
docs say so.

One socket is one trust domain: anything that reaches it is S at R. Who that
may be is decided by directory permissions and an optional `uid` checked with
SO_PEERCRED. A client may check that the socket is served by the expected
Warren user (`WARREN_UID`, SO_PEERCRED). Warren creates a socket only for a
service its config (or the child template) lists.

On the node, a socket of a service without a `uid` is always mode 0600,
whatever its directory's mode: only Warren's user needs it. `services.dir` is
0700, and 0711 (others may pass through, not list) when some node service
names a `uid`, so that user can reach its socket, which is 0666 with the
SO_PEERCRED check. Warren sets an existing `services.dir` it owns to exactly
0711 when a `uid` is added (at start and on reload), whatever mode it had (a
directory that others could write to would let them replace sockets in it),
and never changes a directory it does not own or a link; one it does not own
that others may write to is only reported in the log: it is someone else's
directory, which Warren has no right to change (and tightening it could cut
off its owner), and what it protects stays protected without it, since the
sockets of services without a `uid` are 0600 and every request is checked
against the `uid` of one that has it. A reload that adds or removes a `uid` changes the socket's
mode at once. A node socket without a `uid` also admits only Warren's own user, checked
at connect and on every request (a file mode is looked at only when a
connection is made, so a connection that was open when a reload removed a
`uid` is refused on its next request and closed). A socket whose mode is
closed is bound in a private staging directory (`.stage-*`) beside it and
renamed into place without replacing anything, so it never exists under its
name with the process umask's looser mode; this means Warren needs permission
to create a directory in the socket's directory, and it removes staging
directories a crashed Warren left. A socket left by a Warren that was killed
is removed the same way: the entry is first renamed into a fresh private (0700)
directory, looked at there, and unlinked only if it is a socket of Warren's user;
anything else is renamed back and left alone, so a name swapped for another
file at the wrong moment is never deleted (on Linux; elsewhere the checks are
made by path and a narrow window remains). When no node service has a `uid`, Warren
also closes an existing `services.dir` it owns to 0700; for that reason
`attach.dir`, the control socket and an external transport's link sockets may
not be `services.dir` or inside it (a config that puts them there is refused;
the check is on the paths as written, so symlinks are not resolved),
and the socket directories must not be shared between Warrens. A reload that
changes the `uid` of a socket ends the lease of its old holder and closes the
connections of users it no longer admits, long polls included. Sockets of children are 0666: the
child's directory decides who may reach them.

A call from service S always goes to service S elsewhere, same version. There
are no calls between different services. That keeps a service from reaching
into another one, and keeps Warren from having to understand what services
mean.

Services run in one of two ways, with the same API:

- **managed**: Warren starts, restarts and stops the process (`run:`);
- **external**: something else starts it (systemd, a container, by hand).

### Entries

A service publishes **entries**: named things with small opaque data, such as
agents or bots. An entry's address is `<name>@<namespace>`:

| Published by | Namespace |
|---|---|
| a service on the node | the node (`server`), or one of the service's configured namespace labels under it (`web.server`) |
| a service on a flat child | the node |
| a service on a child | the child (`sandbox.server`) |

Entries are per service. Each service sees the merged entries of the same
service across the mesh, each with its **origin** (the relay id that
published it). Entries are visible to every node, whatever `allow` says;
`allow` gates calls only.

Entry names: `[a-z0-9_][a-z0-9._-]{0,63}`.

**Precedence** in the node's namespace, per service: names the node's service
lists or reserves, then flat children in config order. The node services'
reservations and the entry names of their last `PUT` are persisted per
service in `<state_dir>/names.json` (replaced on every `PUT`, not
accumulated), so after a restart a flat child cannot take them before the
node service has published again. The file is written through a temporary file
made exclusively under an unpredictable name, never one that exists. Warren
refuses to start when `state_dir` may be written by its group or by others,
and warns when it is not owned by Warren's user; this is checked right after
the directory is made, before anything is read from it or locked in it.
Temporary files a crashed Warren left there are removed at start. A flat child's `PUT` is held (up to 10 s
after boot) until the node's service of the same name has published, if the
node runs one with `run:`; `/view` shows `ready: true` once done.

**The node's list** sent to peers is built in precedence order: node
services' entries, then flat children in config order, then other configured
children in config order, then template children sorted by label; it is cut
from the tail at 1000 entries per service. Each child has `max_entries`
(default 100 for template children, 1000 otherwise).

### Calls

A **call** is one request and one response, each at most `MaxCallBody`
(1 MiB), from service S at relay X (optionally on behalf of an entry name) to
service S at a relay or entry. Bulk data is the service's business. See
[service-api.md](service-api.md), "Calling" and "Delivery semantics", for the
exact rules and the error codes.

### Transports

Warren reaches other nodes only through a **transport**: a separate process
that knows the network, carries the peer protocol (below) and vouches for who
is calling. Warren contains no network code of its own.

Transports run in one of two ways, with the same API (`/v1/transport`):

- **managed**: Warren starts it and hands it two connected socket pairs as
  inherited descriptors (fd 3 is link A, fd 4 is link B). No paths exist.
  Warren and the shipped transports call `prctl(PR_SET_DUMPABLE, 0)` at
  start, so other processes of the same user cannot attach with ptrace or
  copy the descriptors with `pidfd_getfd` (some distributions, Fedora among
  them, allow same-user ptrace by default).
- **external**: started by something else, reached through two configured
  socket paths. It must run as a different uid from Warren; Warren refuses a
  link where both sides are the same user.

The shorthand `transport: tailscale` means: run `warren-tailscale` from next
to Warren's own binary as a managed transport, with the `tailscale:` block as
its settings. Third-party transports are named in full:
`transport: {run: [...]}` or `transport: {link_a: ..., link_b: ..., uid: ...}`.

The wire format of both links, the errors a transport reports, what Warren
requires of a transport and how to test one are in
[transport-api.md](transport-api.md). When a link closes or fails after start,
every peer is marked down; a managed transport is restarted with backoff, and
an external one is dialled again with backoff. A transport that is not
answering at start (the network is down) is not a refusal: Warren starts with
the link down, its local services work, and it keeps retrying.

Inside Warren, a small internal interface wraps the link; the tests use an
in-memory fake behind it. That fake is not a supported way to run Warren,
except under `warren dev`.

`warren-tailscale` listens on the tailnet address, or on `listen` (for a
published Docker port), dials peers on `port` (default 7778), finds peers by
tag through tailscaled's local API (`socket`, by default
`/var/run/tailscale/tailscaled.sock`), and identifies callers with WhoIs. It
rebinds when the tailnet address changes and refuses callers that are not
tagged or are this node.

### The seams

| Seam | Implemented by | Form | Stability |
|---|---|---|---|
| Transport API (`/v1/transport`) | `warren-tailscale`, third parties | h2c over inherited fds or uid-checked sockets; [transport-api.md](transport-api.md); transport conformance harness | public; additive within v1 |
| Service socket API (`/v1/service`) | services of any language | HTTP/1.1 over a unix socket; [service-api.md](service-api.md); service conformance suite; `warren dev` | public; additive within v1 |
| Control API (control socket) | Warren; used by `warren status` and `warren-update` | HTTP over a unix socket (mode 0600, Warren's user or root); [control-api.md](control-api.md) | public; additive |
| Peer protocol (`/v1/peer`) | Warren only | HTTP, carried by transports | internal; versioning below |

The service socket API is described in [service-api.md](service-api.md): its
conventions and errors, identity and status, leases, polls and answers,
calling, entries and the view, delivery semantics, limits, and `warren dev`
for trying it.

## Peer protocol (`/v1/peer`, internal)

Carried by the transport, with `X-Warren-Peer: 1` and the sender's software
version and features (`X-Warren-Version`, `X-Warren-Features`).

```
GET  /v1/peer/hello                            {"node", "version", "peer_protocol": [1], "features": []}
GET  /v1/peer/services?since=&wait=            the services this node lists entries for
GET  /v1/peer/entries?service=&since=&wait=    this node's list for one service:
                                               entries with origins, reserved names
POST /v1/peer/call?service=&version=&method=&to=&origin=&from=&timeout_ms=
```

- Every answer carries the header `X-Warren-Node`, which must equal the node
  the transport reached (a call's body belongs to the service, so the header
  is used everywhere).
- A node has at most 32 services. It polls a peer's `services` list once and
  that peer's entries only for services it has itself, one long poll each. It
  accepts at most 2 concurrent long polls per peer per list, and caches its
  own serialized lists per service and version.
- One list answer is at most 4 MiB: 1000 entries of at most 3 KiB data, plus
  names and origins.

### Vouching: who may speak for what

For calls from peer B:

- origin is `B` or `x.B`;
- `from`, if set, is an address under B (B's namespace, a child of B, or a
  service namespace of B) that the origin may publish under (below). It need
  not be in B's last list: B is trusted for everything under B.

For entry lists from B, the receiver checks every entry:

- origin is `B` or `x.B`;
- namespace `x.B` only with origin `x.B`;
- namespace `B` with origin `B` or `x.B` (a flat child);
- a service namespace `l.B` only with origin `B`;
- `reserved` applies only to namespace `B`.

Entries failing a check are dropped and logged once. (B itself refuses at
load a service namespace that equals one of its child labels.) The receiver
never lets peer input create state of its own: a peer can only speak for
itself and what lies under it.

On a node, a local socket's calls have origin = that socket's relay id;
`from` follows the rules in [service-api.md](service-api.md), "Calling".
`allow` is decided on the origin.

### Routing

| Caller | Target | Path |
|---|---|---|
| socket on node A | relay or entry on A | delivered on A |
| socket on node A | relay or entry on node B | A -> B |

An entry's node is where its listing came from; a relay id's node is the one
it names. A node never passes a call from a peer to another peer. Routing
beyond one link is a non-goal: it would turn every node into a router and
make "who vouched for this" a chain instead of a single check.

## Limits and fairness

- **Keys.** Incoming pools are shared out per origin root (the node an origin
  belongs to) and, for this node's own sockets, per relay id.
- **Per service socket** (config): `inflight` (default 16), `per_root`
  (default 8), `outflight` (default 16); for template children the defaults
  are 4, 2 and 4. `inflight` is at most 32, and `inflight` + `outflight` + 4 (view
  polls) is at most 64, the connections of a socket; a config over it is
  refused.
- **Per socket:** 64 connections, 4 `view` polls, 1 `serve` per second, 1
  `PUT` per second, one `PUT` body at a time.
- **Children:** `attach.max_children` (default 16); directories beyond it get
  nothing and are logged.
- **Peer bodies:** the per-root place is taken before the body is read. A
  transport buffers a peer's body whole, within its own read limit (30 s for
  `warren-tailscale`), before handing it to Warren, so Warren sets no read
  deadline of its own on link B. A list answer from a peer is bounded by the
  deadline of the poll that asked for it (the wait plus a 10 s margin).
- **Answers and expired calls.** An answer body is read by the call's
  deadline at the latest. A call that expires while its answer is still being
  read keeps its `inflight` and `per_root` places until the read is over, so
  expiry never lets more buffers in than the places allow. The response to a
  call is written by the call's deadline plus a 2 s grace too, and the `outflight` place is
  free once it passes, whether or not the caller reads. Other responses on a
  service socket (views, polls, errors) have 30 s to be written.
- **Bodies are decoded as they are read.** The entry and reserved name counts
  and the size of each value of a `PUT` or of a peer's list are enforced while
  decoding, before anything is kept, so a body full of empty entries costs no
  more than its own size.
- **Departed peers** (no longer listed by the transport) are remembered for 60
  s (the stale window), so that a call to one that has just left is `offline`
  and not `no-target`, and at most 256 of them are kept at once, the oldest
  forgotten first.
- **Memory bound:** every `inflight` and `outflight` place holds a request
  and a response (2 MiB), every `PUT` 4 MiB, every socket up to 64
  connections of request headers (16 KiB each), and serialized views and
  lists are cached once per version (4 MiB per service per node in the mesh).
  A list poll being answered holds the raw answer (up to 8 MiB) and its
  decoded copy, for each service and each peer. Warren computes the worst
  case from its config, including `max_children`, at start, logs it, and
  `warren status` shows it. The figure assumes 10 peers (status says so); a
  mesh with more peers has a higher real bound. It is a bound that real use
  never approaches; typical use is a few MiB. Lower the defaults per service
  on small machines.

The point of the per-root pools is fairness between machines: one busy or
misbehaving peer fills its own share of a service's places and no more.

## Configuration

A configuration file is YAML; `examples/warren.yaml` is a complete one. The
rules that matter for the design:

- `allow`: exact relay ids, `*.<node>` (every child of that node, not the
  node), `*` (everyone). Omitted means nobody. `*.<node>` admits children
  created later, and an asymmetric `allow` also blocks replies.
- `names:` on a child, when omitted, means any name.
- `default:` (the template for directories made while Warren runs) may not
  be `flat`; a config that tries is refused, since anyone who can create a
  directory would get the node's namespace.
- **Children are picked up while running**: a directory under `attach.dir`
  not named under `children` gets the `default` services within 3 s; without
  a `default` it gets nothing.
- `warren reload` (or SIGHUP) applies changes without a restart and without
  changing `boot`. A removed service or child: its leases end and its entries
  are dropped. A changed `flat`: treated as removed and added. A reload that
  removes a configured child whose directory still exists is refused, so a
  host's programs can never silently fall back to the template's namespace.
  `reload` cannot change `node`, `state_dir`, `transport`, `services.dir`,
  `attach.dir` or `control`; those need a restart.
- Namespace labels are checked against child labels, against each other and
  against `children` at load and reload; a child directory named like a
  namespace gets nothing.
- **Control socket** (`control:`, default `<services.dir>/../control.sock`,
  mode 0600, SO_PEERCRED must be Warren's uid or root): the API that
  `warren status`, `warren reload` and `warren drain-and-exit` use. `warren
  status --health` exits 0 when the config is loaded and the transport link
  is up, 1 otherwise, 2 when the control socket cannot be reached; a
  container healthcheck can use it. [control-api.md](control-api.md) is its
  contract: status, per-service status, and restarting a managed service.
- **`pinned: <reason>`** (optional, free text, one line of at most 200 bytes):
  this node's binaries are replaced from outside Warren, for example
  `pinned: docker image example:warren`. Warren only reports it, in
  `warren status` and in the control `/status`; updaters refuse to install or
  revert on a pinned node. Unset means not pinned. `reload` applies a change.
- **Updatable.** For each managed service, and for Warren itself, the control API
  also says whether a binary could be replaced by an update (`updatable`) and,
  if not, why (`not_updatable`): `read-only` when Warren's user cannot write the
  file or write and search its directory; `container` when Warren runs in a
  container (`/.dockerenv` or `/run/.containerenv` exists) and the binary's directory
  is on the same file system as `/`, in the container's own layer rather than on a
  volume; or `mount-point` when the binary is a mount point of its own (a bind-mounted
  file), which a rename cannot replace. Both are also left out when the check
  cannot say: off Linux, or on an error other than a denial (a missing file, an I/O
  error). Details are in [control-api.md](control-api.md).
- **Loading** is done by the public package `confload`, which services and
  transports may use for their own config:
  - YAML is decoded strictly (unknown keys refused). Then every
    **string-typed** field is expanded with Docker Compose syntax, using
    Compose's own implementation: `$VAR`, `${VAR}`, `${VAR:-default}`,
    `${VAR-default}`, `${VAR:?message}`, `${VAR?message}`, `$$` for a
    literal `$`. Numbers and booleans are never expanded.
  - A bare `$VAR` or `${VAR}` that is unset is a load error, never an empty
    string. An empty default (`${VAR:-}`) is a deliberate empty value. The
    `+` forms are refused. A literal `$` in any string, such as a token or a
    bcrypt hash, is written `$$`; a stray `$` fails to load rather than
    passing silently.
  - Expansion is single pass and after parsing: a value that contains `$` or
    YAML is never expanded again and cannot add structure.
  - Values that must not live in the file are `Secret` fields: a string
    (normally `${VAR}`) or `{file: <path>}` (relative to the config file, one
    trailing newline trimmed, at most 64 KiB). They print as `[secret]`.
    `confload` never shows the package's own error text (it can quote the
    whole string); it writes its own message from the key path, plus the
    variable name when one is missing.
  - Load and reload resolve everything again: a rotated secret file takes
    effect when the process that reads it reloads. A changed variable needs
    that process restarted, since its environment is fixed at start.

## Managed processes

A service or transport with `run:` is started by Warren as Warren's own user,
with a minimal environment (`PATH`, `HOME`, `USER`, `LANG`, `TZ`,
`XDG_RUNTIME_DIR`), plus `env:` values and the variables named in `env_pass`,
plus `WARREN_SERVICE_SOCKET` for a service. A service that needs other
variables lists them in `env_pass`. The values of `env:` are not `Secret`
fields; nothing prints them, `warren status` included.

Warren restarts a managed process with backoff (1 s doubling to 60 s, reset
after 5 min up), logs its output under its name, and on shutdown drains
(stops handing out calls, waits up to 5 s for answers), sends SIGTERM, waits
10 s, then SIGKILL. When a service process exits, its lease ends at once, and
the process group it led is killed with it, whether Warren stopped it or it
exited by itself, so nothing it started outlives it (on Linux; elsewhere only
when Warren stopped it, since a group cannot be signalled safely after its
leader is reaped).
Warren sets `PR_SET_CHILD_SUBREAPER` and reaps every child, so it can run as
PID 1 and collect re-parented grandchildren. `warren status` shows each
process: running, restarting (last exit), or stopped. `run:` is for node
services and transports only.

A service that needs a user's files runs under a Warren that runs as that
user, for example a user systemd service with `services.dir` under
`$XDG_RUNTIME_DIR/warren`. In Docker, Warren runs as the image's user and
manages the service and the transport inside the container.

## Threat model

- Anything that reaches a service socket is that service, including taking
  over its lease. On a workstation, any process of the user can be the
  service and speak for the node's own names. That is the trust of a local
  socket.
- The transport link is the identity of other machines. A managed link has no
  path, and Warren and the shipped transports are not dumpable, so other
  processes of the same user cannot attach to it at runtime (apart from a
  short window between exec and the `prctl` call). That is only a boundary
  where the binaries and config are not writable by that user, as in a
  container with a read-only root and read-only config. Where they belong to
  the user, a process of the user can swap a binary or edit the config and
  own the link at the next restart: there, the link is not a boundary
  against the user's own processes. An external transport must run as
  another uid. Root can do anything.
- Anything that can reach the control socket (Warren's user, or root) can restart
  any managed service, and drain Warren. `warren-update`, which uses it, is as
  trusted as Warren: it can replace Warren's managed binaries and restart them.
  Whoever can write the updater's config (`update.yaml`) can add a key and so
  install code, which runs as Warren's user; see "What you trust" in
  [updates.md](updates.md).
- A child is as trustworthy as whoever can write its directory. A flat child
  is as trusted as the node for names in the node's namespace.
- A peer is as trustworthy as the transport's proof of identity. With
  Tailscale, whoever controls the tailnet's tags can impersonate a node. A
  transport over a shared server would let that server's operator
  impersonate any node.
- Entries, origins and child relay ids are visible to every node.

Report a vulnerability as described in [../SECURITY.md](../SECURITY.md).

## Versions

- The software version (semver) and the protocol versions are separate. Most
  releases change no protocol.
- Within `/v1/peer`, `/v1/service` and `/v1/transport`, changes are additive:
  new optional fields (ignored by older parties) and new endpoints (an older
  party answers 404, read as "not supported"). Each node announces its
  version and features in `/v1/peer/hello` and on every peer request; a node
  uses a feature with a peer only when the peer announced it. Ordinary
  upgrades roll through the mesh one machine at a time.
- A breaking peer change introduces `/v2/peer`. The release that introduces
  it speaks both `v1` and `v2` to peers (choosing the highest both
  announce); the next release drops `v1`. This window of one release is the
  only tolerance for old versions anywhere, and it covers wire protocol only,
  never stored data. A newer Warren that finds state in an older format
  (`names.json`) discards it and logs that.
- Services and transports ship separately from Warren and keep working across
  Warren upgrades within v1 of their APIs.

## Roadmap (not implemented)

None of this exists yet; it is here so the design is visible.

- **warren-nats**: a transport over a NATS server. Each node is a NATS user
  allowed to publish only on `warren.req.<to>.<self>` and `warren.hb.<self>`
  and to subscribe only on its own request, heartbeat and reply subjects, so
  the server proves the sender. Requests use request/reply with no reconnect
  buffer (a request while disconnected fails as `not-sent`); TLS is
  required, and the server's operator can impersonate any node.
- **More updates**: `warren-update` ([updates.md](updates.md)) installs a service
  component from a directory. Still to come: updating Warren and the updater
  itself, HTTP and mesh sources, and an opt-in automatic roll-out where the first
  machine to install a release is its canary.
