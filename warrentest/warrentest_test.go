package warrentest_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/client"
	"github.com/PeterSR/warren-mesh/warrentest"
)

const meshYAML = `
fast: true
nodes:
  - name: alpha
    services:
      - name: chat
        allow: ["*"]
        namespaces: [web]
    children:
      - name: host
        flat: true
        services:
          - name: chat
            allow: ["*"]
    default:
      services:
        - name: chat
          allow: ["*"]
  - name: beta
    services:
      - name: chat
        allow: ["*"]
`

func dial(t *testing.T, path string) *client.Client {
	t.Helper()
	c, err := client.Dial(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestMeshCarriesCallsAndEntries(t *testing.T) {
	m := warrentest.Start(t, meshYAML)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a := dial(t, m.Socket("host.alpha", "chat"))
	b := dial(t, m.Socket("beta", "chat"))

	srv := b.NewServer(client.ServeOptions{Version: 1}, func(_ context.Context, call *client.Call) ([]byte, error) {
		return append([]byte("echo:"), call.Body...), nil
	})
	srv.SetEntries(client.EntriesRequest{Entries: []client.Entry{{Name: "bob"}}})
	sctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- srv.Run(sctx) }()
	defer func() { stop(); <-done }()

	var out []byte
	deadline := time.Now().Add(20 * time.Second)
	for {
		var err error
		out, err = a.Call(ctx, client.CallRequest{To: "bob@beta", Method: "hi", Version: 1, Body: []byte("x")})
		if err == nil {
			break
		}
		if !(client.IsRetryable(err) || client.IsCode(err, client.CodeNoTarget)) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if string(out) != "echo:x" {
		t.Fatalf("answer %q", out)
	}
	self, err := a.Self(ctx)
	if err != nil || self.Relay != "host.alpha" || !self.Flat {
		t.Fatalf("self %+v %v", self, err)
	}
}

func TestSocketsAndClose(t *testing.T) {
	m, err := warrentest.Open(meshYAML)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, s := range m.Sockets() {
		if s.Service != "chat" {
			t.Errorf("service %q", s.Service)
		}
		if _, err := os.Stat(s.Path); err != nil {
			t.Errorf("%s: %v", s.Path, err)
		}
		n++
	}
	if n != 3 {
		t.Fatalf("%d sockets", n)
	}
	if m.ChildSocket("alpha", "sandbox", "chat") == "" {
		t.Fatal("empty path")
	}
	dir := m.Dir()
	m.Close()
	m.Close()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("the temporary directory is still there: %v", err)
	}
}

func TestBadConfig(t *testing.T) {
	if _, err := warrentest.Open("nodes: []"); err == nil {
		t.Fatal("no nodes accepted")
	}
	if _, err := warrentest.Open("bogus: 1\nnodes: [{name: a}]"); err == nil {
		t.Fatal("unknown key accepted")
	}
}

func TestMissingSocketPanics(t *testing.T) {
	m := warrentest.Start(t, meshYAML)
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	m.Socket("beta", "nope")
}
