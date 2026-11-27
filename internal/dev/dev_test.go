package dev_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/client"
	"github.com/PeterSR/warren-mesh/internal/dev"
)

const fastYAML = `
fast: true
nodes:
  - name: a
    services: [{name: chat, allow: ["*"]}]
    children:
      - {name: kid, services: [{name: chat, allow: ["*"]}]}
  - name: b
    services: [{name: chat, allow: ["*"]}]
`

func startFast(t *testing.T) *dev.Cluster {
	t.Helper()
	cfg, err := dev.Parse([]byte(fastYAML))
	if err != nil {
		t.Fatal(err)
	}
	cl, err := dev.Start(cfg, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Stop)
	return cl
}

func path(cl *dev.Cluster, relay string) string {
	for _, s := range cl.Sockets() {
		if s.Relay == relay && s.Service == "chat" {
			return s.Path
		}
	}
	return ""
}

// TestParallelFastMeshes runs several fast meshes at once, each doing real
// work (a lease, a call, entries crossing nodes), and stops them at
// different times: under -race this finds any write of the timers while a
// mesh runs.
func TestParallelFastMeshes(t *testing.T) {
	for i := 0; i < 4; i++ {
		i := i
		t.Run(fmt.Sprint("mesh", i), func(t *testing.T) {
			t.Parallel()
			cl := startFast(t)
			srvC, err := client.Dial(path(cl, "b"))
			if err != nil {
				t.Fatal(err)
			}
			defer srvC.Close()
			srv := srvC.NewServer(client.ServeOptions{Version: 1}, func(_ context.Context, c *client.Call) ([]byte, error) { return c.Body, nil })
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { srv.Run(ctx); close(done) }()
			defer func() { cancel(); <-done }()
			srv.SetEntries(client.EntriesRequest{Entries: []client.Entry{{Name: "x"}}})
			caller, err := client.Dial(path(cl, "kid.a"))
			if err != nil {
				t.Fatal(err)
			}
			defer caller.Close()
			deadline := time.Now().Add(20 * time.Second)
			for {
				out, err := caller.Call(context.Background(), client.CallRequest{To: "x@b", Method: "m", Version: 1, Body: []byte("hi"), Timeout: 2 * time.Second})
				if err == nil && string(out) == "hi" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("no answer: %v", err)
				}
				time.Sleep(50 * time.Millisecond)
			}
			// Stop at different times.
			time.Sleep(time.Duration(i) * 100 * time.Millisecond)
		})
	}
}

// TestMixedTimersAreRefused: the timers are process-wide, so a real-timer mesh
// is refused while a fast one runs, and a fast one is again fine once all
// have stopped.
func TestMixedTimersAreRefused(t *testing.T) {
	cl := startFast(t)
	real, err := dev.Parse([]byte(strings.Replace(fastYAML, "fast: true", "fast: false", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dev.Start(real, "", nil); err == nil || !strings.Contains(err.Error(), "process-wide") {
		t.Fatalf("a real mesh next to a fast one: %v", err)
	}
	cl.Stop()
	r, err := dev.Start(real, "", nil)
	if err != nil {
		t.Fatalf("a real mesh after the fast one stopped: %v", err)
	}
	fast, _ := dev.Parse([]byte(fastYAML))
	if _, err := dev.Start(fast, "", nil); err == nil {
		t.Error("a fast mesh next to a real one was accepted")
	}
	r.Stop()
}
