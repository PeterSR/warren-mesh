package conformance

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/dev"
)

// Timing is the timers of the Warren under test, as the suite needs to
// know them.
type Timing struct {
	// LeaseGrace is how long a lease outlives its last poll or handed call.
	LeaseGrace time.Duration
	// StaleFor is how long entries of a gone lease stay, marked stale.
	StaleFor time.Duration
	// ServeInterval and PutInterval are the least times between serves and
	// between PUTs on one socket.
	ServeInterval, PutInterval time.Duration
	// Scan is how often the attach directory is scanned for new children.
	Scan time.Duration
	// Drain is how long shutdown waits for handed-out calls.
	Drain time.Duration
	// Debounce is how long changes take to reach another node.
	Debounce time.Duration
}

// realTiming is the timing of the contract.
var realTiming = Timing{LeaseGrace: 20 * time.Second, StaleFor: 60 * time.Second, ServeInterval: time.Second,
	PutInterval: time.Second, Scan: 3 * time.Second, Drain: 5 * time.Second, Debounce: time.Second}

// fastTiming is what `fast: true` in a dev config gives.
var fastTiming = Timing{LeaseGrace: dev.FastLeaseGrace, StaleFor: dev.FastStaleFor, ServeInterval: dev.FastServeInterval,
	PutInterval: dev.FastPutInterval, Scan: dev.FastScan, Drain: dev.FastDrainWait, Debounce: 50 * time.Millisecond}

// Instance is a running Warren (two nodes, n1 and n2) under test.
type Instance struct {
	// Dir holds <node>/services/<service>.sock and
	// <node>/attach/<child>/<service>.sock.
	Dir    string
	Timing Timing
	// Down makes a node unreachable for the others, or reachable again. Nil
	// when the launcher cannot (a binary): checks that need it are skipped.
	Down func(node string, down bool)
	// Shutdown starts the shutdown (SIGTERM for a binary) and returns at once;
	// Wait returns when it is complete. Stop does both.
	Shutdown func()
	Wait     func()
}

// Stop shuts the Warren down and waits for it.
func (in *Instance) Stop() {
	in.Shutdown()
	in.Wait()
}

// Launcher starts a Warren on a dev.yaml in dir.
type Launcher interface {
	Launch(t testing.TB, yaml, dir string, tm Timing) *Instance
}

// InProcess runs `warren dev` in the test's own process.
type InProcess struct{}

// Launch implements Launcher.
func (InProcess) Launch(t testing.TB, yamlText, dir string, tm Timing) *Instance {
	t.Helper()
	cfg, err := dev.Parse([]byte(yamlText))
	if err != nil {
		t.Fatal(err)
	}
	cl, err := dev.Start(cfg, dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(1)
	in := &Instance{Dir: cl.Dir, Timing: tm,
		Down:     func(node string, down bool) { cl.Net.SetDown(node, down) },
		Shutdown: func() { once.Do(func() { go func() { defer wg.Done(); cl.Stop() }() }) },
		Wait:     wg.Wait,
	}
	t.Cleanup(func() { in.Shutdown(); in.Wait() })
	return in
}

// Binary runs the warren binary at Path as `warren dev`.
type Binary struct{ Path string }

// Launch implements Launcher.
func (b Binary) Launch(t testing.TB, yamlText, dir string, tm Timing) *Instance {
	t.Helper()
	cfg := filepath.Join(dir + ".yaml")
	if err := os.WriteFile(cfg, []byte(yamlText), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(b.Path, "dev", "--config", cfg, "--dir", dir)
	// Both streams go to buffers that the process's own copying goroutines
	// fill, and cmd.Wait runs in one place only, so nothing reads a buffer
	// that is still being written.
	var stdout, stderr lockedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("cannot start %s: %v", b.Path, err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	// Ready when it has printed where everything is.
	deadline := time.After(30 * time.Second)
	for !strings.Contains(stdout.String(), "Stop with") {
		select {
		case <-done:
			t.Fatalf("%s dev exited before it was ready:\n%s", b.Path, stderr.String())
		case <-deadline:
			_ = cmd.Process.Kill()
			<-done
			t.Fatalf("%s dev was not ready in 30 s:\n%s", b.Path, stderr.String())
		case <-time.After(20 * time.Millisecond):
		}
	}
	var once sync.Once
	in := &Instance{Dir: dir, Timing: tm,
		Shutdown: func() { once.Do(func() { _ = cmd.Process.Signal(syscall.SIGTERM) }) },
		Wait: func() {
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				_ = cmd.Process.Kill()
				<-done
			}
		},
	}
	t.Cleanup(func() { in.Shutdown(); in.Wait() })
	return in
}

// lockedBuffer is a bytes.Buffer that may be read while the process writes.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// Topology is the dev.yaml the suite runs against. The names are the
// contract of the suite: node n1 with services echo (namespace chat, small
// limits), locked and nobody, flat children fa and fb, children kit, pat
// (entry names ok-*), only, small (3 entries) and wild, a template for
// directories made while it runs; node n2 with a child k2.
func Topology(fast bool) string {
	return fmt.Sprintf(`
fast: %t
nodes:
  - name: n1
    max_children: 70
    services:
      - {name: echo, allow: ["*"], namespaces: [chat], inflight: 3, per_root: 2, outflight: 2}
      - {name: locked, allow: ["n2"]}
      - {name: nobody}
    children:
      - {name: fa, flat: true, services: [{name: echo, allow: ["*"]}]}
      - {name: fb, flat: true, services: [{name: echo, allow: ["*"]}]}
      - {name: kit, services: [{name: echo, allow: ["*"]}, {name: locked, allow: ["kit.n1"]}]}
      - {name: pat, names: ["ok-*"], services: [{name: echo, allow: ["*"]}]}
      - {name: only, services: [{name: echo, allow: ["kit.n1"]}]}
      - {name: wild, services: [{name: echo, allow: ["*.n1"]}]}
      - {name: small, max_entries: 3, services: [{name: echo, allow: ["*"]}]}
    default:
      services: [{name: echo, allow: ["*"]}]
  - name: n2
    services:
      - {name: echo, allow: ["*"]}
      - {name: locked}
    children:
      - {name: k2, services: [{name: echo, allow: ["*"]}]}
`, fast)
}
