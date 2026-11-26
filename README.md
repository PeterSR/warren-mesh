# Warren

Warren is a small daemon that runs on each machine and lets programs on
different machines call each other, without any of them knowing about the
network in between. A program talks to a unix socket on its own machine;
Warren finds the other machine and carries the call there.

Status: early (0.1.0). The service and transport APIs are v1 and additive.
Linux only; darwin builds. Transports: warren-tailscale; more planned.

## Why

Programs that need to reach each other across a few machines usually grow
their own discovery, their own authentication and their own idea of the
network. Warren pulls that out into one place. It knows three things:

- **relay ids**, the names of places in the mesh;
- **entries**, named things a program publishes (an agent, a bot, a
  mailbox) so others can find them;
- **calls**, one request and one response between two programs that are the
  same service on different machines.

It knows nothing else: not what the programs mean, and not how bytes get
between machines. Networks plug in as **transports** and programs ride on
Warren as **services**, both as separate processes. You can write either in
any language, against a documented HTTP API.

## How it fits together

```
  laptop                                           server
 +--------------------------------------+        +------------------------------+
 |  warren (node "laptop")              |        |  warren (node "server")      |
 |    |                                 |        |    |                         |
 |    +-- service "chat" ---- socket    |        |    +-- service "chat" -- socket
 |    |                                 |        |    |                         |
 |    +-- child "sandbox" (a directory) |        |    +-- child "host"          |
 |    |     service "chat" -- socket    |        |    |                         |
 |    |                                 |        |    |                         |
 |    +-- transport: warren-tailscale <-+--------+--> transport: warren-tailscale
 +--------------------------------------+ tailnet+------------------------------+
```

- A **node** is one Warren, one per machine. It is named by a label such as
  `laptop`.
- A **relay id** names a place in the mesh: a node (`laptop`) or a child of
  a node (`sandbox.laptop`). Every call comes from a relay id and goes to
  one, and `allow` lists decide which relay ids may call a service.
- A **child** is a directory on the node, for a container or another user
  on the machine. Programs that reach a socket in it are services of that
  child. No Warren runs inside.
- A **service** is a program that reached a socket Warren made for it, such
  as `chat.sock`. It calls the same service on other nodes and publishes
  entries (`alice@laptop`, `bot-1@sandbox.laptop`).
- A **transport** is a separate process that knows the network.
  `warren-tailscale` carries calls over a tailnet and proves who is calling
  from what Tailscale says.

The details are in [docs/architecture.md](docs/architecture.md).

## Install

You need Go 1.26 or newer.

```
go install github.com/PeterSR/warren-mesh/cmd/warren@latest
go install github.com/PeterSR/warren-mesh/cmd/warren-tailscale@latest
```

Both go in the same directory (`$(go env GOBIN)`, or `$(go env GOPATH)/bin`).
Keep them together: `transport: tailscale` runs `warren-tailscale` from next
to the `warren` binary. Release archives for Linux (amd64 and arm64) are on
the releases page; [RELEASING.md](RELEASING.md) says how to verify them.
Third-party licenses are in [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).

## Quick start

Try the contract on one machine, with no network: `warren dev` runs several
nodes in one process on an in-memory transport, with real service sockets.

```
git clone https://github.com/PeterSR/warren-mesh && cd warren-mesh
go run ./cmd/warren dev --config examples/dev.yaml --dir /tmp/wd
```

It prints the socket of every service of every relay. In another terminal,
follow the curl walkthrough in
[docs/service-api.md](docs/service-api.md#10-trying-it-with-curl): become a
service, publish an entry, receive a call from a child and answer it.

To run a real node, copy [examples/warren.yaml](examples/warren.yaml) to
`/etc/warren/config.yaml`, set `node:` and the Tailscale `tag:`, and use
`warren run`; [contrib/warren.service](contrib/warren.service) is a systemd
unit for it.

## Commands

```
warren run [--config path]
    Run the node. The config defaults to /etc/warren/config.yaml.
warren status [--health] [--control path] [--config path]
    Show the running node: peers, services, leases, processes. With
    --health, print nothing but the answer: exit 0 when the config is
    loaded and the transport link is up, 1 if not, 2 if Warren cannot be
    reached. For a container health check.
warren reload [--control path] [--config path]
    Apply the config without a restart (also on SIGHUP).
warren drain-and-exit [--control path] [--config path]
    Stop handing out calls, let the ones in flight finish, and exit.
warren dev --config dev.yaml [--dir path]
    Several nodes in one process on the in-memory transport, with real
    service sockets, for testing services.
warren version
    Print the version.

warren-tailscale
    The tailnet transport. Started by Warren (`transport: tailscale`) on
    inherited descriptors 3 and 4; or, to run it as another user,
    --link-a PATH --link-b PATH --warren-uid N.
warren-tailscale --version
```

Configuration is YAML, loaded with the package `confload`: unknown keys are
refused, `${VAR}` is expanded in string values (an unset variable is an
error), and secrets print as `[secret]`.

## The contracts

Services and transports are separate processes. Each contract is public and
has a document, a conformance check and a way to try it:

- **Service socket API** (`/v1/service`, HTTP/1.1 over a unix socket):
  [docs/service-api.md](docs/service-api.md). The Go client is
  `github.com/PeterSR/warren-mesh/client` (serve with lease management,
  call, entries, a view watcher, typed errors). The suite in `conformance/`
  checks a Warren against the document: `go test ./conformance`, or against a
  built binary with `go test ./conformance -args -warren /path/to/warren`.
- **Transport API** (`/v1/transport`, h2c on inherited descriptors 3 and 4,
  or two sockets): [docs/transport-api.md](docs/transport-api.md). The
  harness in `conformance/transport` runs a transport binary against a fake
  Warren; the test of that package runs it against `warren-tailscale` on a
  fake tailnet (`go test ./conformance/transport`, which waits 95 s to check
  that the links stay open while idle; `-short` shortens that).
- **`warren dev`** runs real Warren nodes for service authors to test
  against, in any language. In Go tests,
  `github.com/PeterSR/warren-mesh/warrentest` starts the same mesh in the
  test's process and returns the socket paths.

## More

- [docs/architecture.md](docs/architecture.md): concepts, the peer protocol,
  limits, the threat model, versioning and what is planned.
- [SECURITY.md](SECURITY.md): how to report a vulnerability.
- [CONTRIBUTING.md](CONTRIBUTING.md) and [CHANGELOG.md](CHANGELOG.md).

Warren builds and tests on Linux. It builds for darwin, with the protections
that need Linux features reduced (see `internal/platform` and
`internal/socketdir`), but darwin is not supported.

## License

MIT, see [LICENSE](LICENSE).
