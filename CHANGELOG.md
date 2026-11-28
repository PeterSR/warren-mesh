# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

Initial release.

### Added

- `warren`, the per-machine node: relay ids, entries and calls, children served from a directory,
  managed and external services, and the control commands `run`, `status`, `reload`,
  `drain-and-exit`, `dev` and `version`. A `state_dir` that is writable by its group or by others
  is refused at start, and one not owned by Warren's user is warned about: make it 0700.
- The service socket API (`/v1/service`, v1), documented in `docs/service-api.md`, with a Go
  client, a conformance suite and `warren dev` for testing services. A service can give a
  handed-out call up (`answer` with `fail=1`): the caller gets `unknown` at once and the place is
  free. The Go client does so when a handler returns an error or panics.
- The transport API (`/v1/transport`, v1), documented in `docs/transport-api.md`, with a
  conformance harness.
- The control API, documented in [docs/control-api.md](docs/control-api.md): `GET /status`, `GET
  /services`, `GET /services/<name>` (state, absolute `exe`, `start_seq`, cumulative counters
  that survive a reload, and the lease with its epoch), `POST /services/<name>/restart`, which
  acts at once, also on a process in backoff, and returns `min_start_seq`, `POST /reload` and
  `POST /drain-and-exit`. Every endpoint answers 400 to an unknown query parameter, and 404 to a
  path that is no endpoint.
- `pinned: <reason>` in the config says this node's binaries are replaced from outside Warren.
  `warren status` and the control `/status` report it.
- The control API reports `updatable` and, when not, `not_updatable` (`read-only`; `container`
  for a binary in a container's own layer; `mount-point` for a bind-mounted file), per service
  and for Warren itself. `warren status` marks what is not updatable.
- `warren-tailscale`, the transport for a tailnet.
- The peer protocol between nodes, with vouching, per-node pools and merged views.
- `warren-update`: signed installs of a service component from a directory, with a health
  watch, revert, recovery after a crash and a fixed `state.json` format, as a service Warren
  runs (see [docs/updates.md](docs/updates.md)). `warren-release` makes the releases:
  `keygen`, `publish` and `verify`, with minisign signatures that the stock `minisign -V`
  verifies. Their dependencies are `aead.dev/minisign`, `golang.org/x/crypto` and
  `golang.org/x/term`.
- `warren-launch` supervises Warren, restarts it with backoff and cleans up its descendants
  on Linux.
- The release archives hold `warren`, `warren-launch`, `warren-tailscale`, `warren-update`,
  `warren-release`, config examples and systemd units for system and user services.
- The packages `confload` (strict YAML with `${VAR}` expansion and secrets), `client` and
  `warrentest`.
