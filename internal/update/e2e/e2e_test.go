// Package e2e tests the update system against a real Warren: the daemon is
// started through daemon.Start with Options.Link, it manages a service whose
// binary is a copy of this test binary (or a script that crashes), and the
// updater installs new versions of it through its own socket. The real restart
// endpoint, counters and reload do the work.
package e2e

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"aead.dev/minisign"

	"github.com/PeterSR/warren-mesh/client"
	"github.com/PeterSR/warren-mesh/internal/daemon"
	"github.com/PeterSR/warren-mesh/internal/link"
	"github.com/PeterSR/warren-mesh/internal/service"
	"github.com/PeterSR/warren-mesh/internal/supervise"
	"github.com/PeterSR/warren-mesh/internal/update/engine"
	"github.com/PeterSR/warren-mesh/internal/update/manifest"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
)

// The test binary doubles as the component: Warren runs it with --role svc and
// it holds the service lease until it is stopped.
func TestMain(m *testing.M) {
	if len(os.Args) > 2 && os.Args[1] == "--role" && os.Args[2] == "svc" {
		os.Exit(roleService())
	}
	os.Exit(m.Run())
}

func roleService() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	c, err := client.Dial("")
	if err != nil {
		return 3
	}
	s := c.NewServer(client.ServeOptions{Version: 1, Take: true}, func(context.Context, *client.Call) ([]byte, error) { return []byte("ok"), nil })
	if err := s.Run(ctx); err != nil {
		return 4
	}
	return 0
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type world struct {
	t       *testing.T
	dir     string
	bin     string
	target  string
	cfgPath string
	wcfg    string
	wtext   string
	d       *daemon.Daemon
	ctl     *engine.Control
	cl      *engine.Client
	key     minisign.PrivateKey
	keyText string
	eng     *engine.Engine
	cancel  context.CancelFunc
	runDone chan error
	ownBin  []byte
}

func wait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	dl := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(dl) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// goodBinary is this test binary with a marker appended, so that each version
// has other bytes and still runs.
func (w *world) goodBinary(marker string) []byte {
	return append(append([]byte{}, w.ownBin...), []byte("\n"+marker)...)
}

var crasher = []byte("#!/bin/sh\nexit 1\n")

func newWorld(t *testing.T) *world {
	t.Helper()
	d, err := os.MkdirTemp("", "e2e")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	// Fast timing for Warren and the updater's limits.
	sc, lg, si := service.ScanEvery, service.LeaseGrace, service.ServeInterval
	bs, ka := supervise.BackoffStart, supervise.KillAfter
	service.ScanEvery, service.LeaseGrace, service.ServeInterval = 40*time.Millisecond, 2*time.Second, 0
	supervise.BackoffStart, supervise.KillAfter = 50*time.Millisecond, 2*time.Second
	ms, mc, mh := uconfig.MinSettle, uconfig.MinCanary, uconfig.MinHealthTimeout
	uconfig.MinSettle, uconfig.MinCanary, uconfig.MinHealthTimeout = 100*time.Millisecond, 100*time.Millisecond, time.Second
	t.Cleanup(func() {
		service.ScanEvery, service.LeaseGrace, service.ServeInterval = sc, lg, si
		supervise.BackoffStart, supervise.KillAfter = bs, ka
		uconfig.MinSettle, uconfig.MinCanary, uconfig.MinHealthTimeout = ms, mc, mh
	})
	w := &world{t: t, dir: d, bin: filepath.Join(d, "bin"), cfgPath: filepath.Join(d, "update.yaml"), wcfg: filepath.Join(d, "warren.yaml")}
	os.MkdirAll(w.bin, 0o755)
	own, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if w.ownBin, err = os.ReadFile(own); err != nil {
		t.Fatal(err)
	}
	w.target = filepath.Join(w.bin, "myapp")
	os.WriteFile(w.target, w.goodBinary("v0"), 0o755)

	pub, priv, err := minisign.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	w.key, w.keyText = priv, pub.String()

	w.wtext = strings.NewReplacer("DIR", d, "TARGET", w.target).Replace(`
node: n1
state_dir: DIR/state
services:
  dir: DIR/s
  list:
    - name: myapp
      allow: ["*"]
      run: [TARGET, --role, svc]
      env: {GENERATION: one}
`)
	os.WriteFile(w.wcfg, []byte(w.wtext), 0o600)
	net := link.NewNet()
	w.d, err = daemon.Start(daemon.Options{ConfigPath: w.wcfg, Log: quiet(), Link: net.Join("n1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer func() { _ = recover() }()
		w.d.Stop()
	})
	control := filepath.Join(d, "control.sock")
	w.ctl = engine.NewControl(control)

	os.WriteFile(w.cfgPath, []byte(strings.NewReplacer("DIR", d, "KEY", w.keyText, "TARGET", w.target).Replace(`
state_dir: DIR/updater
warren:
  control: DIR/control.sock
canary: 300ms
health_timeout: 20s
components:
  myapp:
    channel: dev
    keys: {dev: [KEY]}
    service: myapp
    targets: {myapp: TARGET}
    settle: 400ms
`)), 0o600)
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.runDone = make(chan error, 1)
	ready := make(chan *engine.Engine, 1)
	go func() {
		w.runDone <- engine.Run(ctx, engine.RunOptions{ConfigPath: w.cfgPath, Version: "test", Log: quiet(), PollEvery: 50 * time.Millisecond,
			ServiceSocket: "", Ready: func(e *engine.Engine) { ready <- e }})
	}()
	select {
	case w.eng = <-ready:
	case err := <-w.runDone:
		t.Fatalf("the updater did not start: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the updater did not start")
	}
	t.Cleanup(func() {
		cancel()
		<-w.runDone
	})
	w.cl = engine.NewClient(filepath.Join(d, "updater", engine.SocketName))
	wait(t, "the service holds its lease", func() bool {
		s, err := w.ctl.Service(context.Background(), "myapp")
		return err == nil && s.Lease.Held
	})
	return w
}

// release writes a signed source directory holding one version of the component.
func (w *world) release(version string, content []byte) string {
	w.t.Helper()
	src := filepath.Join(w.dir, "src-"+version)
	os.MkdirAll(src, 0o755)
	name := "myapp_" + version
	os.WriteFile(filepath.Join(src, name), content, 0o644)
	m := &manifest.Manifest{Format: manifest.Format, Component: "myapp", Version: version, Channel: manifest.Dev, Created: "2026-11-26T10:00:00Z",
		Platforms: map[string]manifest.Platform{engine.HostPlatform: {Files: map[string]manifest.File{"myapp": {Name: name, Size: int64(len(content)), SHA256: sha(content)}}}}}
	data, err := m.Marshal()
	if err != nil {
		w.t.Fatal(err)
	}
	sig := manifest.Sign(w.key, data, m)
	os.WriteFile(filepath.Join(src, manifest.FileName("myapp", "", version)), data, 0o644)
	os.WriteFile(filepath.Join(src, manifest.FileName("myapp", "", version)+".minisig"), sig, 0o644)
	return src
}

func (w *world) install(version string, content []byte) *engine.InstallResult {
	w.t.Helper()
	src := w.release(version, content)
	cfg, err := uconfig.Load(w.cfgPath)
	if err != nil {
		w.t.Fatal(err)
	}
	cand, err := engine.LoadFromDir(src, cfg.Components["myapp"], version)
	if err != nil {
		w.t.Fatal(err)
	}
	res, err := w.cl.Install(context.Background(), cand, engine.InstallRequest{})
	if err != nil {
		w.t.Fatalf("install %s: %v", version, err)
	}
	return res
}

func (w *world) component() engine.ComponentStatus {
	w.t.Helper()
	st, err := w.cl.Status(context.Background())
	if err != nil {
		w.t.Fatal(err)
	}
	return st.Components[0]
}

func (w *world) service() *engine.Service {
	w.t.Helper()
	s, err := w.ctl.Service(context.Background(), "myapp")
	if err != nil {
		w.t.Fatal(err)
	}
	return s
}

func (w *world) content() string {
	b, err := os.ReadFile(w.target)
	if err != nil {
		w.t.Fatal(err)
	}
	return string(b)
}

func TestInstallCrashLoopRevertAndReloadDuringProbation(t *testing.T) {
	w := newWorld(t)
	before := w.service()

	// 1. A real install of a working version: the real restart endpoint stops the
	// old process and starts the new file, and the lease settles.
	good1 := w.goodBinary("v1")
	res := w.install("1.0.0-dev.1", good1)
	if res.Phase != "probation" {
		t.Fatalf("phase %s", res.Phase)
	}
	wait(t, "v1 committed", func() bool { return w.component().Phase == "committed" })
	if w.content() != string(good1) {
		t.Fatal("the target does not hold v1")
	}
	after := w.service()
	if after.StartSeq <= before.StartSeq || after.RequestedExits < 1 || !after.Lease.Held || after.Lease.Epoch <= before.Lease.Epoch || after.State != "running" {
		t.Fatalf("service before %+v after %+v", before, after)
	}
	if c := w.component(); c.Version != "1.0.0-dev.1" || c.Prev != "" && c.Prev == "1.0.0-dev.1" {
		t.Errorf("status %+v", c)
	}

	// 2. A version that crashes in a loop: reverted by the updater, marked bad,
	// and the old version runs again.
	w.install("1.0.0-dev.2", crasher)
	wait(t, "v2 reverted and the old version back in place", func() bool {
		c := w.component()
		return c.Phase == "reverted" && c.Version == "1.0.0-dev.1"
	})
	if w.content() != string(good1) {
		t.Fatalf("after the crash loop the target holds %d bytes", len(w.content()))
	}
	c := w.component()
	if len(c.Bad) != 1 || c.Bad[0].Version != "1.0.0-dev.2" || c.Bad[0].Reason != "crash-loop" || c.Version != "1.0.0-dev.1" {
		t.Fatalf("status %+v", c)
	}
	wait(t, "the restored version holds the lease and is proven", func() bool {
		s := w.service()
		return s.State == "running" && s.Lease.Held && w.component().HealthySince != nil
	})
	// The counters saw the failures as unrequested ones.
	if s := w.service(); s.Exits-s.RequestedExits < 3 {
		t.Errorf("unrequested failures %d", s.Exits-s.RequestedExits)
	}

	// 3. A reload that changes run while a new version is on probation removes
	// and adds the service again; the counters and the lease epoch survive it,
	// and the install still settles and commits.
	w.install("1.0.0-dev.3", w.goodBinary("v3"))
	pre := w.service()
	os.WriteFile(w.wcfg, []byte(strings.Replace(w.wtext, "GENERATION: one", "GENERATION: two", 1)), 0o600)
	if err := w.d.Reload(); err != nil {
		t.Fatal(err)
	}
	if c := w.component(); c.Phase != "probation" {
		t.Fatalf("the install left probation by itself during the reload: %+v", c)
	}
	wait(t, "the re-added service holds a lease", func() bool {
		s, err := w.ctl.Service(context.Background(), "myapp")
		return err == nil && s.Lease.Held && s.Lease.Epoch > pre.Lease.Epoch
	})
	post := w.service()
	if post.StartSeq <= pre.StartSeq || post.Starts <= pre.Starts || post.Exits < pre.Exits || post.RequestedExits <= pre.RequestedExits || post.Boot != pre.Boot {
		t.Fatalf("counters did not survive the reload: before %+v after %+v", pre, post)
	}
	wait(t, "v3 committed", func() bool { return w.component().Phase == "committed" })
	if w.content() != string(w.goodBinary("v3")) {
		t.Error("the target does not hold v3")
	}
	if c := w.component(); len(c.Bad) != 1 || c.Version != "1.0.0-dev.3" {
		t.Errorf("status %+v", c)
	}
}

// A requested restart is real: the service can be restarted by the updater's
// manual revert even while it sits in Warren's backoff.
func TestManualRevertRestartsTheServiceThroughWarren(t *testing.T) {
	w := newWorld(t)
	w.install("1.0.0-dev.1", w.goodBinary("v1"))
	wait(t, "v1 committed", func() bool { return w.component().Phase == "committed" })
	before := w.service()
	res, err := w.cl.Revert(context.Background(), "myapp")
	if err != nil {
		t.Fatal(err)
	}
	if res.Phase != "reverted" {
		t.Fatalf("%+v", res)
	}
	wait(t, "the restored version runs", func() bool {
		s := w.service()
		return s.StartSeq > before.StartSeq && s.Lease.Held && w.component().HealthySince != nil
	})
	if w.content() != string(w.goodBinary("v0")) {
		t.Error("the target does not hold v0 again")
	}
}

func TestInstallRefusedWhenTheNodeIsPinned(t *testing.T) {
	w := newWorld(t)
	os.WriteFile(w.wcfg, []byte(w.wtext+"pinned: docker image x:y\n"), 0o600)
	if err := w.d.Reload(); err != nil {
		t.Fatal(err)
	}
	src := w.release("1.0.0-dev.1", w.goodBinary("v1"))
	cfg, _ := uconfig.Load(w.cfgPath)
	cand, err := engine.LoadFromDir(src, cfg.Components["myapp"], "1.0.0-dev.1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.cl.Install(context.Background(), cand, engine.InstallRequest{})
	var re *engine.RemoteError
	if !errors.As(err, &re) || !strings.Contains(err.Error(), "node is pinned: docker image x:y") {
		t.Fatalf("got %v", err)
	}
	if w.content() != string(w.goodBinary("v0")) {
		t.Error("the target changed")
	}
	if st, _ := w.cl.Status(context.Background()); st.Pinned != "docker image x:y" {
		t.Errorf("status pinned %q", st.Pinned)
	}
}

func sha(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
