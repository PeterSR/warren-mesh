package client_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/client"
	"github.com/PeterSR/warren-mesh/internal/dev"
)

// devYAML is the topology of the tests and examples: two nodes, a flat child
// and a child on the first.
const devYAML = `
fast: true
nodes:
  - name: laptop
    services:
      - name: chat
        allow: ["*"]
        namespaces: [web]
      - name: locked
        allow: ["server"]
    children:
      - name: host
        flat: true
        services:
          - name: chat
            allow: ["*"]
      - name: sandbox
        services:
          - name: chat
            allow: ["*"]
  - name: server
    services:
      - name: chat
        allow: ["*"]
`

// startDev runs the dev topology in this process and returns the cluster.
func startDev(t testing.TB, dir string) *dev.Cluster {
	t.Helper()
	return startDevYAML(t, dir, devYAML)
}

// startDevYAML is startDev on another topology.
func startDevYAML(t testing.TB, dir, yaml string) *dev.Cluster {
	t.Helper()
	cfg, err := dev.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	cl, err := dev.Start(cfg, dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Stop)
	return cl
}

// sock is the socket of a service at a relay in a cluster.
func sock(t testing.TB, cl *dev.Cluster, relay, service string) string {
	t.Helper()
	for _, s := range cl.Sockets() {
		if s.Relay == relay && s.Service == service {
			return s.Path
		}
	}
	t.Fatalf("no socket %s at %s", service, relay)
	return ""
}

func dial(t testing.TB, path string, opts ...client.Option) *client.Client {
	t.Helper()
	c, err := client.Dial(path, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// eventually polls cond until it holds or ten seconds pass.
func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	dl := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(dl) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// echoServer runs a Server on c that answers every call with the method, a
// colon and the body, until the test ends.
func echoServer(t testing.TB, c *client.Client, opts client.ServeOptions) *client.Server {
	t.Helper()
	if opts.Version == 0 {
		opts.Version = 1
	}
	srv := c.NewServer(opts, func(_ context.Context, call *client.Call) ([]byte, error) {
		return append([]byte(call.Method+":"), call.Body...), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the server did not stop")
		}
	})
	return srv
}

func tempDir(t testing.TB) string {
	d, err := os.MkdirTemp("", "wc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return filepath.Join(d, "x")
}
