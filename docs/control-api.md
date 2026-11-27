# Control API (v1)

Warren answers a small HTTP API on its control socket. `warren status`,
`warren reload` and `warren drain-and-exit` use it, and so can a supervisor-style
tool that manages Warren's services (`warren-update` is one). This page is its
contract.

- **Socket**: `control:` in the config, by default `control.sock` next to
  `services.dir`. Mode 0600; a peer must be Warren's user or root (SO_PEERCRED).
  HTTP over a unix socket, JSON bodies.
- **Unknown query parameters are refused** with 400 on every endpoint that exists,
  so a caller that asks for something this Warren does not know is told so instead
  of being ignored. A path that is no endpoint is 404, whatever its query.
- Status codes: 200 (or 204 where there is no body), 400 unknown query parameter,
  404 not found (an unknown path, or a service that is not managed now), 405 wrong
  method, 409 a reload that was refused (the body says why), 503 a restart
  while Warren is shutting down or draining to exit.
- An older Warren answers 404 to the endpoints it does not have. A caller that
  needs one treats that as "Warren too old".

```
GET  /status                      Warren's status
GET  /services                    every managed service
GET  /services/<name>             one managed service
POST /services/<name>/restart     stop it and start it again at once
POST /reload                      apply the config (same as SIGHUP)
POST /drain-and-exit              stop handing out calls, then exit
```

## `GET /status`

The node, its boot id, Warren's version and features, the transport, peers,
children, services and processes, and `healthy` (the config is loaded and the
transport link is up). `pinned` is present, with its reason, when the config sets
`pinned:` (this node's binaries are replaced from outside Warren, and updaters
stay away). `warren.updatable` and `warren.not_updatable` say whether Warren's own
binary could be replaced, as described below for services.

## Services

Only services with `run:` are listed; any other name is 404, and so is a service
that a reload has removed and not yet added back. A caller treats 404 as "not
there now", never as a failure. The names are read from a snapshot taken under a
lock that a reload holds only briefly, so a slow reload cannot stall a health poll.
Each object is then read on its own, so a list read during a reload may show one
service as of the old config next to one as of the new.

```json
{"name": "myapp", "exe": "/opt/myapp/bin/myapp", "updatable": true,
 "state": "running", "pid": 4121, "start_seq": 7, "started_at": "2026-11-26T10:00:00Z",
 "starts": 7, "exits": 6, "requested_exits": 2, "start_failures": 0,
 "last_exit": "exit status 1", "last_exit_requested": false, "last_start_error": "",
 "lease": {"held": true, "since": "2026-11-26T10:00:03Z", "epoch": 3},
 "boot": "9c1f..."}
```

- `exe` is the absolute path of `argv[0]` as resolved at the latest start (a bare
  command through `PATH`, a relative path made absolute). It is empty, with
  `exe_error` set, when that is not possible. Never the rest of `argv`.
- The counters are cumulative per service name for the life of a boot. They
  survive a reload that changes a service's `run` (the service is removed and added
  again) and reset only when Warren restarts, which is a new `boot`. A poller
  therefore loses nothing between polls: the unrequested failures are the change in
  `exits - requested_exits + start_failures`.
- `updatable` says whether the `exe` could be replaced by renaming a new file over it,
  and `not_updatable` says why not, in this order: `read-only` (Warren's user fails
  `faccessat` with `AT_EACCESS` for `W_OK` on the file, or `W_OK|X_OK` on its
  directory, with EACCES, EROFS or EPERM), `container` (`/.dockerenv` or
  `/run/.containerenv` exists and the exe's **directory** is on the same device as `/`:
  the container's own layer, not a volume) or `mount-point` (the file is on another
  device than its directory, as a single-file bind mount is, so a rename over it
  fails). Both fields are left out where the system cannot say, which means unknown:
  off Linux, or when the check fails in another way (a missing file, an I/O error, or in a
  container a root device that cannot be read). A
  symlinked `exe` is judged by what it points to for the file checks; an updater
  refuses a symlink anyway.
- The `processes` of `GET /status` carry the same `updatable` and `not_updatable`, and
  so does `warren status`.
- `started_at` and `lease.since` are left out until there is a start and a lease.
- `start_seq` increases each time a start is begun, failed or not; `starts` counts
  the successful ones. `requested_exits` counts exits of processes that were
  stopped on purpose: by a restart request, a reload or shutdown. (A process that
  exits by itself at the very moment a request stops it is counted as requested.) A link break of a
  transport stops it too, but is not a request: it counts as a failure and keeps the
  doubling backoff.
- `lease.epoch` increases on each new lease (also across a reload that makes the
  socket again), `since` is when the current one began. The lease ends when the
  process exits, not when it is sent SIGTERM.

### `POST /services/<name>/restart`

Stops the process (SIGTERM, then SIGKILL after 10 s) and starts it again at once.
It also acts on a process that is waiting in backoff, and resets the backoff, so a
requested restart is never delayed by an earlier crash loop. It answers at once:

```json
{"accepted": true, "min_start_seq": 8}
```

`min_start_seq` is worked out atomically with the start counter: every start of
this service with a `start_seq` of that number or higher begins after the request.
A start that was already in progress has a lower number, and is stopped and
replaced. A request is never lost, whatever the service was doing.
