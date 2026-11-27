package uconfig

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"aead.dev/minisign"
)

func newKey(t *testing.T) string {
	t.Helper()
	pub, _, err := minisign.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub.String()
}

type env struct {
	dir, bin, state, stable, dev string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	d, err := os.MkdirTemp("", "uc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	e := &env{dir: d, bin: filepath.Join(d, "bin"), state: filepath.Join(d, "state"), stable: newKey(t), dev: newKey(t)}
	os.MkdirAll(e.bin, 0o755)
	os.MkdirAll(e.state, 0o700)
	return e
}

func (e *env) text(comp string) string {
	return strings.NewReplacer("STATE", e.state, "BIN", e.bin, "STABLE", e.stable, "DEV", e.dev, "DIR", e.dir).Replace(`
state_dir: STATE
warren:
  control: DIR/control.sock
components:
` + comp)
}

func (e *env) load(t *testing.T, comp string) (*Config, error) {
	t.Helper()
	p := filepath.Join(e.dir, "update.yaml")
	os.WriteFile(p, []byte(e.text(comp)), 0o600)
	return Load(p)
}

const myappStable = `  myapp:
    keys: {stable: [STABLE]}
    service: myapp
    targets: {myapp: BIN/myapp}
`

func TestLoadsAValidConfig(t *testing.T) {
	e := newEnv(t)
	c, err := e.load(t, myappStable+`  files:
    keys: {dev: [DEV]}
    channel: dev
    service: files
    settle: 10s
    max_size: 8MiB
    pinned: managed by apt
`)
	if err != nil {
		t.Fatal(err)
	}
	a, f := c.Components["myapp"], c.Components["files"]
	if a.Channel != "stable" || a.Settle != DefaultSettle || a.MaxSize != DefaultMaxSize || a.Targets["myapp"] != e.bin+"/myapp" {
		t.Errorf("myapp %+v", a)
	}
	if f.Channel != "dev" || f.Settle != 10*time.Second || f.MaxSize != 8<<20 || f.Pinned != "managed by apt" || len(f.Targets) != 0 {
		t.Errorf("files %+v", f)
	}
	if c.CanaryFor(a) != DefaultCanaryStable || c.CanaryFor(f) != DefaultCanaryDev || c.HealthTimeout != DefaultHealthTimeout {
		t.Errorf("defaults: %v %v %v", c.CanaryFor(a), c.CanaryFor(f), c.HealthTimeout)
	}
	if got := strings.Join(c.Names(), ","); got != "files,myapp" {
		t.Errorf("names %s", got)
	}
}

func TestRefusals(t *testing.T) {
	e := newEnv(t)
	os.WriteFile(filepath.Join(e.bin, "real"), nil, 0o755)
	os.Symlink(filepath.Join(e.bin, "real"), filepath.Join(e.bin, "link"))
	os.MkdirAll(filepath.Join(e.dir, "open"), 0o755)
	os.Chmod(filepath.Join(e.dir, "open"), 0o777)
	other := newKey(t)
	cases := []struct{ name, comp, want string }{
		{"warren", "  warren:\n    keys: {stable: [STABLE]}\n    targets: {warren: BIN/warren}\n", "in this release"},
		{"warren-update", "  warren-update:\n    keys: {stable: [STABLE]}\n    service: warren-update\n", "in this release"},
		{"auto", myappStable + "    auto: true\n", "in this release"},
		{"no service", "  myapp:\n    keys: {stable: [STABLE]}\n", "service is required"},
		{"bad name", "  Myapp:\n    keys: {stable: [STABLE]}\n    service: a\n", "label"},
		{"unknown key", myappStable + "    mirror: true\n", "unknown key"},
		{"channel", myappStable + "    channel: beta\n", "stable or dev"},
		{"stable needs a stable key", "  myapp:\n    keys: {dev: [DEV]}\n    service: a\n", "needs a stable key"},
		{"stable with a dev key", "  myapp:\n    keys: {stable: [STABLE], dev: [DEV]}\n    service: a\n", "may not list a dev key"},
		{"dev needs a dev key", "  myapp:\n    channel: dev\n    keys: {stable: [STABLE]}\n    service: a\n", "needs a dev key"},
		{"key shorthand on dev", "  myapp:\n    channel: dev\n    key: STABLE\n    service: a\n", "stable component"},
		{"same key in both channels", "  myapp:\n    channel: dev\n    keys: {stable: [STABLE], dev: [STABLE]}\n    service: a\n", "same public key"},
		{"bad key", "  myapp:\n    keys: {stable: [RWQnotakey]}\n    service: a\n", "42 bytes"},
		{"too many keys", "  myapp:\n    keys: {stable: [STABLE, " + other + ", " + other + ", " + other + ", " + other + "]}\n    service: a\n", "at most 4"},
		{"settle", myappStable + "    settle: 1s\n", "settle"},
		{"relative target", "  myapp:\n    keys: {stable: [STABLE]}\n    service: a\n    targets: {myapp: bin/myapp}\n", "absolute"},
		{"unclean target", "  myapp:\n    keys: {stable: [STABLE]}\n    service: a\n    targets: {myapp: BIN/../bin/myapp}\n", "clean"},
		{"symlink target", "  myapp:\n    keys: {stable: [STABLE]}\n    service: a\n    targets: {myapp: BIN/link}\n", "symlink"},
		{"target under state_dir", "  myapp:\n    keys: {stable: [STABLE]}\n    service: a\n    targets: {myapp: STATE/myapp}\n", "state_dir"},
		{"writable directory", "  myapp:\n    keys: {stable: [STABLE]}\n    service: a\n    targets: {myapp: DIR/open/myapp}\n", "writable by group or others"},
		{"missing directory", "  myapp:\n    keys: {stable: [STABLE]}\n    service: a\n    targets: {myapp: DIR/nope/myapp}\n", "directory"},
		{"duplicate file", myappStable + "  other:\n    keys: {stable: [STABLE]}\n    service: b\n    targets: {x: BIN/myapp}\n", "the same file"},
		{"duplicate through a symlinked directory", myappStable + "  other:\n    keys: {stable: [STABLE]}\n    service: b\n    targets: {x: DIR/viaLink/myapp}\n", "the same file"},
		{"pinned with a newline", myappStable + "    pinned: \"a\\nb\"\n", "one line"},
	}
	os.Symlink(e.bin, filepath.Join(e.dir, "viaLink"))
	for _, c := range cases {
		if _, err := e.load(t, c.comp); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", c.name, err, c.want)
		}
	}
}

func TestAPinnedComponentMayOmitKeysAutoAndTargets(t *testing.T) {
	e := newEnv(t)
	c, err := e.load(t, "  myapp:\n    service: myapp\n    pinned: distro\n")
	if err != nil {
		t.Fatal(err)
	}
	if a := c.Components["myapp"]; a.Pinned != "distro" || len(a.Keys) != 0 || len(a.Targets) != 0 {
		t.Errorf("%+v", a)
	}
	// Without the pin the same entry is refused, and a pinned one still needs its service.
	if _, err := e.load(t, "  myapp:\n    service: myapp\n"); err == nil {
		t.Error("an unpinned component without keys accepted")
	}
	if _, err := e.load(t, "  myapp:\n    pinned: distro\n"); err == nil {
		t.Error("a component without a service accepted")
	}
	// Keys it does list are still checked.
	if _, err := e.load(t, "  myapp:\n    service: myapp\n    pinned: distro\n    keys: {stable: [RWQbad]}\n"); err == nil {
		t.Error("a bad key on a pinned component accepted")
	}
}

func TestTargetUnderStateDirThroughASymlink(t *testing.T) {
	e := newEnv(t)
	// A directory that is a symlink into the state directory.
	os.MkdirAll(filepath.Join(e.state, "inner"), 0o700)
	os.Symlink(filepath.Join(e.state, "inner"), filepath.Join(e.dir, "alias"))
	_, err := e.load(t, "  myapp:\n    keys: {stable: [STABLE]}\n    service: a\n    targets: {myapp: DIR/alias/myapp}\n")
	if err == nil || !strings.Contains(err.Error(), "state_dir") {
		t.Errorf("a target under state_dir through a symlink: %v", err)
	}
	// And a state_dir that is itself reached through a link.
	link := filepath.Join(e.dir, "statelink")
	os.Symlink(e.state, link)
	if !UnderDir(filepath.Join(e.state, "x", "y"), link) || UnderDir(e.bin+"/myapp", link) {
		t.Error("UnderDir does not resolve symlinks")
	}
	if !UnderDir(filepath.Join(e.state, "not", "there", "yet"), e.state) {
		t.Error("a path that does not exist yet is not under its directory")
	}
}

func TestTwoComponentsMayNotShareAService(t *testing.T) {
	e := newEnv(t)
	_, err := e.load(t, myappStable+"  other:\n    keys: {stable: [STABLE]}\n    service: myapp\n")
	if err == nil || !strings.Contains(err.Error(), "also the service of component") {
		t.Errorf("got %v", err)
	}
}

// The CLI may run as another user (root): ownership and permission rules are
// the daemon's to enforce.
func TestLoadCLISkipsTheDirectoryOwnershipRules(t *testing.T) {
	e := newEnv(t)
	loose := filepath.Join(e.dir, "loose")
	os.MkdirAll(loose, 0o755)
	os.Chmod(loose, 0o775) // group writable: the daemon refuses it
	p := filepath.Join(e.dir, "update.yaml")
	os.WriteFile(p, []byte(e.text("  myapp:\n    keys: {stable: [STABLE]}\n    service: myapp\n    targets: {myapp: DIR/loose/myapp}\n")), 0o600)
	if _, err := Load(p); err == nil || !strings.Contains(err.Error(), "writable by group") {
		t.Fatalf("the daemon accepted a group-writable directory: %v", err)
	}
	if _, err := LoadCLI(p); err != nil {
		t.Errorf("the CLI refused it: %v", err)
	}
	// Everything else is still checked.
	os.WriteFile(p, []byte(e.text("  myapp:\n    service: myapp\n")), 0o600)
	if _, err := LoadCLI(p); err == nil {
		t.Error("the CLI accepted a component without keys")
	}
}

func TestStateDirAndControlAndLimits(t *testing.T) {
	e := newEnv(t)
	p := filepath.Join(e.dir, "u.yaml")
	for name, c := range map[string]struct{ text, want string }{
		"state_dir":      {"state_dir: rel\nwarren: {control: /x}\n", "state_dir"},
		"no control":     {"state_dir: /s\n", "warren.control"},
		"canary":         {"state_dir: /s\nwarren: {control: /x}\ncanary: 1s\n", "canary"},
		"health timeout": {"state_dir: /s\nwarren: {control: /x}\nhealth_timeout: 1s\n", "health_timeout"},
		"sources":        {"state_dir: /s\nwarren: {control: /x}\nsources: [mesh]\n", "unknown key"},
		"serve":          {"state_dir: /s\nwarren: {control: /x}\nserve: true\n", "unknown key"},
	} {
		os.WriteFile(p, []byte(strings.ReplaceAll(c.text, "\n", "\n")), 0o600)
		if _, err := Load(p); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestByteSize(t *testing.T) {
	e := newEnv(t)
	for text, want := range map[string]string{"1048576": "", "2KiB": "", "3GiB": "", "0": "size", "-1": "size", "12XB": "size"} {
		_, err := e.load(t, myappStable+"    max_size: "+text+"\n")
		if (want == "") != (err == nil) || (want != "" && !strings.Contains(err.Error(), want)) {
			t.Errorf("max_size %q: %v", text, err)
		}
	}
}

// The same rules as Warren's config: one shared check.
func TestPinnedFollowsTheSharedRules(t *testing.T) {
	e := newEnv(t)
	for name, v := range map[string]string{"bidi": `"a\u202eb"`, "zero width": `"a\u200bb"`, "only spaces": `"   "`, "tab": `"a\tb"`} {
		if _, err := e.load(t, myappStable+"    pinned: "+v+"\n"); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := e.load(t, myappStable+"    pinned: "+strings.Repeat("x", 201)+"\n"); err == nil {
		t.Error("an overlong reason accepted")
	}
}
