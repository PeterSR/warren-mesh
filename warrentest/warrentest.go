// Package warrentest starts an in-process Warren dev mesh for the tests of
// a service written in Go (or any language: the sockets are real).
//
// It is `warren dev` as a library: several nodes joined by the in-memory
// transport, each with real service sockets in a temporary directory, with
// children (flat or not), services with their allow lists and namespaces,
// and an optional fast-timers mode, so a test need not wait for the lease
// grace or the stale window. Services are always external: the test starts
// them (for example with the client package) on the returned socket paths.
//
//	m := warrentest.Start(t, `
//	fast: true
//	nodes:
//	  - name: alpha
//	    services:
//	      - name: chat
//	        allow: ["*"]
//	    children:
//	      - name: host
//	        flat: true
//	        services:
//	          - name: chat
//	            allow: ["*"]
//	  - name: beta
//	    services:
//	      - name: chat
//	        allow: ["*"]
//	`)
//	path := m.Socket("host.alpha", "chat")
//
// The configuration has the format of `warren dev` (see docs/service-api.md
// and examples/dev.yaml).
//
// Start reports a bad configuration through the test. The lookups on Mesh
// (Socket, ChildSocket) panic when asked for a relay, node or
// service the configuration does not have, since that is a mistake in the
// test itself and a silent empty path would only fail later and less clearly.
//
// Fast timers are package variables of Warren and so process-wide. They are
// set by the first mesh that starts and put back when the last one stops, so
// any number of meshes of the same kind may run at once (t.Parallel is fine
// for tests that all use `fast: true`). A mesh of the other kind is refused
// with an error while one runs: do not mix `fast: true` and real timers in
// tests that run at the same time in one process.
package warrentest

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/PeterSR/warren-mesh/internal/dev"
)

// Socket is one service socket of the mesh.
type Socket struct {
	// Node is the node the socket belongs to.
	Node string
	// Relay is the relay id: the node, or <label>.<node> for a child.
	Relay string
	// Service is the service name.
	Service string
	// Path is the unix socket path to give to client.Dial.
	Path string
	// Allow is the socket's allow list.
	Allow []string
	// Flat is true for a flat child's socket.
	Flat bool
	// Child is true for a socket of a child (flat or not).
	Child bool
}

// Mesh is a running dev mesh.
type Mesh struct {
	cl *dev.Cluster
}

// Option changes how Start runs the mesh.
type Option func(*options)

type options struct {
	dir string
	log *slog.Logger
}

// WithDir keeps the mesh's files in dir (not removed at Close) instead of a
// temporary directory.
func WithDir(dir string) Option { return func(o *options) { o.dir = dir } }

// WithLogger sends Warren's log to l. The default discards it.
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.log = l } }

// Open starts a mesh from a dev configuration (YAML) and returns it, or the
// error. The caller must Close it. Most tests want Start.
func Open(yaml string, opts ...Option) (*Mesh, error) {
	var o options
	for _, f := range opts {
		f(&o)
	}
	cfg, err := dev.Parse([]byte(yaml))
	if err != nil {
		return nil, err
	}
	log := o.log
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	cl, err := dev.Start(cfg, o.dir, log)
	if err != nil {
		return nil, err
	}
	return &Mesh{cl: cl}, nil
}

// Start is Open for a test: it fails the test on an error and closes the
// mesh when the test ends.
func Start(t testing.TB, yaml string, opts ...Option) *Mesh {
	t.Helper()
	m, err := Open(yaml, opts...)
	if err != nil {
		t.Fatalf("warrentest: %v", err)
	}
	t.Cleanup(m.Close)
	return m
}

// Close stops every node and removes the temporary directory. It may be
// called more than once.
func (m *Mesh) Close() { m.cl.Stop() }

// Dir is the directory the mesh keeps its files in.
func (m *Mesh) Dir() string { return m.cl.Dir }

// Sockets lists the sockets of the configuration: each node's own, then each
// configured child's. Children made from a `default` template while the mesh
// runs are not listed; see ChildSocket.
func (m *Mesh) Sockets() []Socket {
	var out []Socket
	for _, s := range m.cl.Sockets() {
		out = append(out, Socket(s))
	}
	return out
}

// Socket returns the path of a service's socket at a relay (a node name or
// <label>.<node>). It panics when the configuration has no such socket, since
// that is a mistake in the test.
func (m *Mesh) Socket(relay, service string) string {
	for _, s := range m.cl.Sockets() {
		if s.Relay == relay && s.Service == service {
			return s.Path
		}
	}
	panic(fmt.Sprintf("warrentest: no socket for service %q at relay %q", service, relay))
}

// ChildSocket returns the path a service of a child directory made under a
// node's attach directory would use (a child of the `default` template is
// created by making its directory; the socket appears within a few seconds).
// It panics when the node does not exist.
func (m *Mesh) ChildSocket(node, label, service string) string {
	for _, in := range m.cl.Nodes {
		if in.Name == node {
			return in.Config.ChildPath(label, service)
		}
	}
	panic(fmt.Sprintf("warrentest: no node %q", node))
}

// ChildDir returns the directory of a child of a node, where a test makes
// the directory of a template child.
func (m *Mesh) ChildDir(node, label string) string {
	return filepath.Dir(m.ChildSocket(node, label, "x"))
}
