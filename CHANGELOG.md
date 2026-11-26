# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - Unreleased

Initial release.

### Added

- `warren`, the per-machine node: relay ids, entries and calls, children served from a directory,
  managed and external services, and the control commands `run`, `status`, `reload`,
  `drain-and-exit`, `dev` and `version`.
- The service socket API (`/v1/service`, v1), documented in `docs/service-api.md`, with a Go
  client, a conformance suite and `warren dev` for testing services. A service can give a
  handed-out call up (`answer` with `fail=1`): the caller gets `unknown` at once and the place is
  free. The Go client does so when a handler returns an error or panics.
- The transport API (`/v1/transport`, v1), documented in `docs/transport-api.md`, with a
  conformance harness.
- `warren-tailscale`, the transport for a tailnet.
- The peer protocol between nodes, with vouching, per-node pools and merged views.
- The packages `confload` (strict YAML with `${VAR}` expansion and secrets), `client` and
  `warrentest`.

[Unreleased]: https://github.com/PeterSR/warren-mesh/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/PeterSR/warren-mesh/releases/tag/v0.1.0
