package main

import (
	"bytes"
	"crypto/rand"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aead.dev/minisign"

	"github.com/PeterSR/warren-mesh/internal/update/engine"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
	"github.com/PeterSR/warren-mesh/internal/version"
)

func do(args ...string) (int, string, string) {
	var o, e bytes.Buffer
	c := run(args, &o, &e)
	return c, o.String(), e.String()
}

func TestVersionAndUsage(t *testing.T) {
	if c, out, _ := do("version"); c != 0 || strings.TrimSpace(out) != version.Version {
		t.Errorf("version: %d %q", c, out)
	}
	for _, args := range [][]string{{}, {"nope"}, {"install"}, {"install", "a", "b"}, {"revert"}} {
		if c, _, _ := do(args...); c != 2 {
			t.Errorf("%v: exit %d, want 2", args, c)
		}
	}
}

func TestInterspersedFlags(t *testing.T) {
	fs := flag.NewFlagSet("x", flag.ContinueOnError)
	force := fs.Bool("force", false, "")
	from := fs.String("from", "", "")
	pos, err := parse(fs, []string{"myapp", "--from", "file://d", "--force"})
	if err != nil || len(pos) != 1 || pos[0] != "myapp" || !*force || *from != "file://d" {
		t.Errorf("%v %v %v %v", pos, err, *force, *from)
	}
}

func TestFileSource(t *testing.T) {
	wd, _ := os.Getwd()
	for in, want := range map[string]string{
		"file:///tmp/dist":  "/tmp/dist",
		"file://./dist":     filepath.Join(wd, "dist"),
		"file://dist":       filepath.Join(wd, "dist"),
		"file://../up/dist": filepath.Join(filepath.Dir(wd), "up", "dist"),
	} {
		got, err := fileSource(in)
		if err != nil || got != want {
			t.Errorf("%s: %q %v, want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"https://example.org/x", "mesh", "/tmp/dist", ""} {
		if _, err := fileSource(in); err == nil {
			t.Errorf("%q accepted", in)
		}
	}
}

// The commands against a live updater socket.
func TestStatusAndReloadOverTheSocket(t *testing.T) {
	d, err := os.MkdirTemp("", "wu")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	os.MkdirAll(filepath.Join(d, "bin"), 0o755)
	os.MkdirAll(filepath.Join(d, "state"), 0o700)
	pub, _, _ := minisign.GenerateKey(rand.Reader)
	cfgPath := filepath.Join(d, "update.yaml")
	text := strings.NewReplacer("DIR", d, "KEY", pub.String()).Replace(`
state_dir: DIR/state
warren: {control: DIR/control.sock}
components:
  myapp:
    channel: dev
    keys: {dev: [KEY]}
    service: myapp
    targets: {myapp: DIR/bin/myapp}
    pinned: managed by apt
`)
	os.WriteFile(cfgPath, []byte(text), 0o600)
	cfg, err := uconfig.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	e := engine.New(engine.Options{Config: cfg, Version: "9.9"})
	srv, err := engine.Listen(filepath.Join(d, "state", engine.SocketName), e.Handler(func() error {
		next, err := uconfig.Load(cfgPath)
		if err != nil {
			return err
		}
		return e.Reload(next)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })

	c, out, errs := do("status", "--config", cfgPath)
	if c != 0 || !strings.Contains(out, "warren-update 9.9") || !strings.Contains(out, "Warren not reachable") ||
		!strings.Contains(out, "pinned: managed by apt") || !strings.Contains(out, "unknown") || !strings.Contains(out, "state-lost") {
		t.Fatalf("status: %d\n%s%s", c, out, errs)
	}
	if c, out, _ := do("status", "--json", "--config", cfgPath); c != 0 || !strings.Contains(out, `"name": "myapp"`) {
		t.Errorf("json: %d %s", c, out)
	}
	if c, out, _ := do("reload", "--config", cfgPath); c != 0 || !strings.Contains(out, "reloaded") {
		t.Errorf("reload: %d %s", c, out)
	}
	// A revert of a pinned component is refused with its reason.
	if c, _, errs := do("revert", "myapp", "--config", cfgPath); c != 1 || !strings.Contains(errs, "pinned: managed by apt") {
		t.Errorf("revert: %d %s", c, errs)
	}
	if c, _, errs := do("revert", "--offline", "myapp", "--config", cfgPath); c != 1 || !strings.Contains(errs, "pinned: managed by apt") {
		t.Errorf("offline revert: %d %s", c, errs)
	}
	// With no updater at the socket the commands say so.
	srv.Close()
	if c, _, errs := do("status", "--config", cfgPath); c != 1 || !strings.Contains(errs, "cannot reach the updater") {
		t.Errorf("no updater: %d %s", c, errs)
	}
	if c, _, errs := do("install", "nope", "--from", "file://x", "--config", cfgPath); c != 1 || !strings.Contains(errs, "not configured") {
		t.Errorf("unknown component: %d %s", c, errs)
	}
	// A pinned component is refused with its pin before the source is looked at.
	if c, _, errs := do("install", "myapp", "--from", "https://x/y", "--config", cfgPath); c != 1 || !strings.Contains(errs, "pinned: managed by apt") {
		t.Errorf("pinned install: %d %s", c, errs)
	}
	if c, _, errs := do("install", "myapp", "--from", "file:///nonexistent/dist", "--config", cfgPath); c != 1 || !strings.Contains(errs, "pinned: managed by apt") {
		t.Errorf("pinned install from a missing source: %d %s", c, errs)
	}
	if c, _, _ := do("status", "--config", filepath.Join(d, "missing.yaml")); c != 1 {
		t.Errorf("missing config: %d", c)
	}
}

// What can be refused from the config alone is refused before update.lock is
// taken, so a running updater does not turn it into a 10 s wait and a lock error.
func TestOfflineRevertRefusesFromTheConfigBeforeTheLock(t *testing.T) {
	d, err := os.MkdirTemp("", "wu")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	os.MkdirAll(filepath.Join(d, "bin"), 0o755)
	os.MkdirAll(filepath.Join(d, "state"), 0o700)
	pub, _, _ := minisign.GenerateKey(rand.Reader)
	write := func(extra string) string {
		p := filepath.Join(d, "update.yaml")
		os.WriteFile(p, []byte(strings.NewReplacer("DIR", d, "KEY", pub.String()).Replace(`
state_dir: DIR/state
warren: {control: DIR/control.sock}
components:
  myapp:
    channel: dev
    keys: {dev: [KEY]}
    service: myapp
`+extra)), 0o600)
		return p
	}
	lock, err := engine.AcquireLock(filepath.Join(d, "state"), 0) // a daemon runs
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	start := time.Now()
	if c, _, errs := do("revert", "--offline", "myapp", "--config", write("    pinned: distro\n    targets: {myapp: DIR/bin/myapp}\n")); c != 1 || !strings.Contains(errs, "pinned: distro") {
		t.Errorf("pinned: %d %s", c, errs)
	}
	if c, _, errs := do("revert", "--offline", "myapp", "--config", write("")); c != 1 || !strings.Contains(errs, "explicit targets") {
		t.Errorf("no targets: %d %s", c, errs)
	}
	if time.Since(start) > 5*time.Second {
		t.Error("the refusals waited for the lock")
	}
}

// The CLI may run as another user: it does not enforce the daemon's rules for
// the owner and mode of target directories.
func TestTheCLILoadsAConfigTheDaemonWouldRefuse(t *testing.T) {
	d, err := os.MkdirTemp("", "wu")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	loose := filepath.Join(d, "bin")
	os.MkdirAll(loose, 0o755)
	os.Chmod(loose, 0o775)
	pub, _, _ := minisign.GenerateKey(rand.Reader)
	p := filepath.Join(d, "update.yaml")
	os.WriteFile(p, []byte(strings.NewReplacer("DIR", d, "KEY", pub.String()).Replace(`
state_dir: DIR/state
warren: {control: DIR/control.sock}
components:
  myapp:
    channel: dev
    keys: {dev: [KEY]}
    service: myapp
    targets: {myapp: DIR/bin/myapp}
`)), 0o600)
	if _, err := uconfig.Load(p); err == nil {
		t.Fatal("the daemon accepted a group-writable directory")
	}
	_, _, errs := do("status", "--config", p)
	if strings.Contains(errs, "writable by group") || !strings.Contains(errs, "cannot reach the updater") {
		t.Errorf("the CLI enforced the daemon's rule: %s", errs)
	}
}

// The offline revert never creates the state directory and loads the config
// with the daemon's rules.
func TestOfflineRevertNeverCreatesTheStateDirAndKeepsTheDaemonsRules(t *testing.T) {
	d, err := os.MkdirTemp("", "wu")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	os.MkdirAll(filepath.Join(d, "bin"), 0o755)
	pub, _, _ := minisign.GenerateKey(rand.Reader)
	p := filepath.Join(d, "update.yaml")
	os.WriteFile(p, []byte(strings.NewReplacer("DIR", d, "KEY", pub.String()).Replace(`
state_dir: DIR/state
warren: {control: DIR/control.sock}
components:
  myapp:
    channel: dev
    keys: {dev: [KEY]}
    service: myapp
    targets: {myapp: DIR/bin/myapp}
`)), 0o600)
	if c, _, errs := do("revert", "--offline", "myapp", "--config", p); c != 1 || !strings.Contains(errs, "does not create it") {
		t.Errorf("a missing state_dir: %d %s", c, errs)
	}
	if _, err := os.Stat(filepath.Join(d, "state")); err == nil {
		t.Error("the state directory was created")
	}
	os.Mkdir(filepath.Join(d, "state"), 0o755) // others may enter it
	if c, _, errs := do("revert", "--offline", "myapp", "--config", p); c != 1 || !strings.Contains(errs, "0700") {
		t.Errorf("an open state_dir: %d %s", c, errs)
	}
	// The daemon's rules for target directories apply too.
	os.Chmod(filepath.Join(d, "bin"), 0o775)
	if c, _, errs := do("revert", "--offline", "myapp", "--config", p); c != 1 || !strings.Contains(errs, "writable by group") {
		t.Errorf("a group-writable target directory: %d %s", c, errs)
	}
}
