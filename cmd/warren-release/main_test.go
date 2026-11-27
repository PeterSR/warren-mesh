package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/PeterSR/warren-mesh/internal/fsutil"
	"github.com/PeterSR/warren-mesh/internal/update/engine"
	"github.com/PeterSR/warren-mesh/internal/update/manifest"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
	"github.com/PeterSR/warren-mesh/internal/version"
)

func do(t *testing.T, stdin string, args ...string) (code int, out, errs string) {
	t.Helper()
	var o, e bytes.Buffer
	code = run(args, strings.NewReader(stdin), &o, &e)
	return code, o.String(), e.String()
}

type fixture struct {
	dir, keyDir, key, pub, bin string
}

func newFixture(t *testing.T, channel string) *fixture {
	t.Helper()
	d := t.TempDir()
	f := &fixture{dir: d, keyDir: filepath.Join(d, "keys")}
	if code, _, e := do(t, "", "keygen", "--component", "myapp", "--channel", channel, "--dir", f.keyDir, "--no-password"); code != 0 {
		t.Fatalf("keygen: %s", e)
	}
	f.key, f.pub = filepath.Join(f.keyDir, "myapp-"+channel+".key"), filepath.Join(f.keyDir, "myapp-"+channel+".pub")
	f.bin = filepath.Join(d, "myapp-bin")
	os.WriteFile(f.bin, []byte("the myapp binary"), 0o755)
	return f
}

func TestVersionSubcommand(t *testing.T) {
	if code, out, _ := do(t, "", "version"); code != 0 || strings.TrimSpace(out) != version.Version {
		t.Errorf("%d %q", code, out)
	}
}

func TestKeygenWritesProtectedFilesAndRefusesToOverwrite(t *testing.T) {
	f := newFixture(t, "dev")
	fi, err := os.Stat(f.key)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("key file: %v %v", err, fi)
	}
	code, _, errs := do(t, "", "keygen", "--component", "myapp", "--channel", "dev", "--dir", f.keyDir, "--no-password")
	if code == 0 || !strings.Contains(errs, "not overwriting") {
		t.Errorf("overwrite: %d %s", code, errs)
	}
	for _, args := range [][]string{
		{"keygen", "--component", "Bad", "--channel", "dev", "--dir", f.keyDir, "--no-password"},
		{"keygen", "--component", "myapp", "--channel", "beta", "--dir", f.keyDir, "--no-password"},
		{"keygen", "--component", "x", "--channel", "dev", "--dir", f.keyDir}, // an empty password from stdin
	} {
		if code, _, _ := do(t, "", args...); code == 0 {
			t.Errorf("%v accepted", args)
		}
	}
	code, out, _ := do(t, "", "keygen", "--component", "y", "--channel", "dev", "--dir", f.keyDir, "--no-password")
	if code != 0 || !strings.Contains(out, "keys: {dev: [RW") {
		t.Errorf("the config line: %s", out)
	}
}

// The full chain: keygen, publish, verify, then the updater's own reader.
func TestPublishVerifyAndTheUpdaterAcceptTheOutput(t *testing.T) {
	f := newFixture(t, "dev")
	dist := filepath.Join(f.dir, "dist")
	code, _, e := do(t, "", "publish", "--component", "myapp", "--channel", "dev", "--version", "1.4.0-dev.7",
		"--file", "myapp=linux/amd64="+f.bin, "--file", "myapp=linux/arm64="+f.bin, "--key", f.key, "--out", dist)
	if code != 0 {
		t.Fatalf("publish: %s", e)
	}
	for _, n := range []string{"myapp-1.4.0-dev.7.manifest.json", "myapp-1.4.0-dev.7.manifest.json.minisig", "myapp.dev.manifest.json", "myapp.dev.manifest.json.minisig", "myapp_1.4.0-dev.7_linux_amd64", "myapp_1.4.0-dev.7_linux_arm64"} {
		if _, err := os.Stat(filepath.Join(dist, n)); err != nil {
			t.Errorf("missing %s", n)
		}
	}
	code, out, e := do(t, "", "verify", "--key", f.pub, dist)
	if code != 0 || strings.Count(out, "ok ") != 2 {
		t.Fatalf("verify: %d %s %s", code, out, e)
	}
	// The same key as bare text.
	pk, _ := os.ReadFile(f.pub)
	bare := strings.Fields(string(pk))[len(strings.Fields(string(pk)))-1]
	if code, _, e := do(t, "", "verify", "--key", bare, "--component", "myapp", dist); code != 0 {
		t.Errorf("verify with a bare key: %s", e)
	}
	// What the updater itself reads from the directory.
	k, err := manifest.ParseKey(bare)
	if err != nil {
		t.Fatal(err)
	}
	comp := &uconfig.Comp{Name: "myapp", Channel: "dev", Keys: manifest.Keys{"dev": {k}}, MaxSize: 1 << 20}
	cand, err := engine.LoadFromDir(dist, comp, "")
	if err != nil || cand.Manifest.Version != "1.4.0-dev.7" {
		t.Fatalf("updater read: %v", err)
	}
	if _, err := engine.LoadFromDir(dist, comp, "1.4.0-dev.7"); err != nil {
		t.Errorf("exact version: %v", err)
	}
	if _, err := engine.LoadFromDir(dist, comp, "1.4.0-dev.8"); err == nil {
		t.Error("a version that is not there was found")
	}
	if _, err := engine.LoadFromDir(dist, comp, "../x"); err == nil {
		t.Error("a path as version accepted")
	}
	// A manual release is signed manual.
	if code, _, e := do(t, "", "publish", "--component", "myapp", "--channel", "dev", "--version", "1.4.0-dev.8", "--manual",
		"--file", "myapp=linux/amd64="+f.bin, "--key", f.key, "--out", dist); code != 0 {
		t.Fatal(e)
	}
	if c, err := engine.LoadFromDir(dist, comp, "1.4.0-dev.8"); err != nil || !c.Manifest.Manual {
		t.Errorf("manual flag: %v", err)
	}
	// Tampering is found by verify.
	os.WriteFile(filepath.Join(dist, "myapp_1.4.0-dev.7_linux_amd64"), []byte("the myapp binarY"), 0o644)
	if code, out, _ := do(t, "", "verify", "--key", f.pub, dist); code == 0 || !strings.Contains(out, "digest does not match") {
		t.Errorf("tampered file: %d %s", code, out)
	}
	os.WriteFile(filepath.Join(dist, "myapp_1.4.0-dev.7_linux_amd64"), []byte("short"), 0o644)
	if code, out, _ := do(t, "", "verify", "--key", f.pub, dist); code == 0 || !strings.Contains(out, "bytes, the manifest signs") {
		t.Errorf("truncated file: %d %s", code, out)
	}
	// Another key fails.
	g := newFixture(t, "dev")
	if code, _, _ := do(t, "", "verify", "--key", g.pub, dist); code == 0 {
		t.Error("verified with another key")
	}
}

func TestPublishChecks(t *testing.T) {
	f := newFixture(t, "dev")
	dist := filepath.Join(f.dir, "dist")
	base := []string{"publish", "--component", "myapp", "--channel", "dev", "--key", f.key, "--out", dist}
	cases := map[string][]string{
		"a dev release with a release version": {"--version", "1.4.0", "--file", "myapp=linux/amd64=" + f.bin},
		"different targets per platform":       {"--version", "1.4.0-dev.1", "--file", "myapp=linux/amd64=" + f.bin, "--file", "other=linux/arm64=" + f.bin},
		"a duplicate target":                   {"--version", "1.4.0-dev.1", "--file", "myapp=linux/amd64=" + f.bin, "--file", "myapp=linux/amd64=" + f.bin},
		"a missing file":                       {"--version", "1.4.0-dev.1", "--file", "myapp=linux/amd64=" + f.bin + ".nope"},
		"a bad file spec":                      {"--version", "1.4.0-dev.1", "--file", "myapp"},
		"no version":                           {"--file", "myapp=linux/amd64=" + f.bin},
		"build metadata":                       {"--version", "1.4.0-dev.1+x", "--file", "myapp=linux/amd64=" + f.bin},
	}
	for name, extra := range cases {
		if code, _, _ := do(t, "", append(append([]string{}, base...), extra...)...); code == 0 {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := os.Stat(dist); err == nil {
		t.Error("a refused publish wrote output")
	}
	// A size over --max-size.
	if code, _, _ := do(t, "", append(append([]string{}, base...), "--version", "1.4.0-dev.1", "--max-size", "3", "--file", "myapp=linux/amd64="+f.bin)...); code == 0 {
		t.Error("an oversize file accepted")
	}
	// A key readable by others is refused.
	os.Chmod(f.key, 0o644)
	if code, _, e := do(t, "", append(append([]string{}, base...), "--version", "1.4.0-dev.1", "--file", "myapp=linux/amd64="+f.bin)...); code == 0 || !strings.Contains(e, "accessible by group") {
		t.Errorf("a world-readable key accepted: %s", e)
	}
}

func TestVersionGuardForWarrenComponents(t *testing.T) {
	d := t.TempDir()
	if code, _, e := do(t, "", "keygen", "--component", "warren-update", "--channel", "stable", "--dir", d, "--no-password"); code != 0 {
		t.Fatal(e)
	}
	key := filepath.Join(d, "warren-update-stable.key")
	script := filepath.Join(d, "warren-update")
	os.WriteFile(script, []byte("#!/bin/sh\necho 1.2.3\n"), 0o755)
	if _, err := exec.LookPath("/bin/sh"); err != nil {
		t.Skip("no shell")
	}
	host := runtime.GOOS + "/" + runtime.GOARCH
	args := func(v string) []string {
		return []string{"publish", "--component", "warren-update", "--channel", "stable", "--version", v, "--file", "warren-update=" + host + "=" + script, "--key", key, "--out", filepath.Join(d, "out"+v)}
	}
	if code, _, e := do(t, "", args("1.2.4")...); code == 0 || !strings.Contains(e, "version guard") {
		t.Errorf("a binary that reports another version was published: %s", e)
	}
	if code, _, e := do(t, "", args("1.2.3")...); code != 0 {
		t.Errorf("the right version refused: %s", e)
	}
	other := []string{"publish", "--component", "warren-update", "--channel", "stable", "--version", "1.2.3", "--file", "warren-update=plan9/mips=" + script, "--key", key, "--out", filepath.Join(d, "o3")}
	if code, _, e := do(t, "", other...); code == 0 || !strings.Contains(e, "host platform") {
		t.Errorf("no host binary: %s", e)
	}
}

// The signature is the stock minisign format: the stock tool verifies it, when installed.
func TestStockMinisignVerifiesTheManifest(t *testing.T) {
	bin, err := exec.LookPath("minisign")
	if err != nil {
		t.Skip("minisign is not installed")
	}
	f := newFixture(t, "dev")
	dist := filepath.Join(f.dir, "dist")
	if code, _, e := do(t, "", "publish", "--component", "myapp", "--channel", "dev", "--version", "1.4.0-dev.7", "--file", "myapp=linux/amd64="+f.bin, "--key", f.key, "--out", dist); code != 0 {
		t.Fatal(e)
	}
	out, err := exec.Command(bin, "-V", "-p", f.pub, "-m", filepath.Join(dist, "myapp.dev.manifest.json")).CombinedOutput()
	if err != nil || !strings.Contains(string(out), "warren-manifest/1 myapp 1.4.0-dev.7 dev") {
		t.Errorf("minisign -V: %v %s", err, out)
	}
}

// The files are hashed again while copied: a file that changes after signing
// is refused and no manifest is written.
func TestPublishRefusesAFileThatChangedAfterSigning(t *testing.T) {
	f := newFixture(t, "dev")
	dist := filepath.Join(f.dir, "dist")
	beforeCopy = func() { os.WriteFile(f.bin, []byte("the myapp binarY"), 0o755) }
	defer func() { beforeCopy = nil }()
	code, _, e := do(t, "", "publish", "--component", "myapp", "--channel", "dev", "--version", "1.4.0-dev.7",
		"--file", "myapp=linux/amd64="+f.bin, "--key", f.key, "--out", dist)
	if code == 0 || !strings.Contains(e, "changed after it was signed") {
		t.Fatalf("%d %s", code, e)
	}
	matches, _ := filepath.Glob(filepath.Join(dist, "*"))
	if len(matches) != 0 {
		t.Errorf("a refused publish left %v", matches)
	}
}

func TestPublishWillNotMoveAPointerBackwards(t *testing.T) {
	f := newFixture(t, "dev")
	dist := filepath.Join(f.dir, "dist")
	pub := func(v string, extra ...string) (int, string) {
		args := append([]string{"publish", "--component", "myapp", "--channel", "dev", "--version", v, "--file", "myapp=linux/amd64=" + f.bin, "--key", f.key, "--out", dist}, extra...)
		c, _, e := do(t, "", args...)
		return c, e
	}
	if c, e := pub("1.4.0-dev.9"); c != 0 {
		t.Fatal(e)
	}
	if c, e := pub("1.4.0-dev.7"); c == 0 || !strings.Contains(e, "--allow-downgrade") {
		t.Fatalf("a lower version moved the pointer: %d %s", c, e)
	}
	// Nothing of the refused release was written.
	if _, err := os.Stat(filepath.Join(dist, "myapp-1.4.0-dev.7.manifest.json")); err == nil {
		t.Error("a refused publish wrote the release")
	}
	if c, e := pub("1.4.0-dev.7", "--allow-downgrade"); c != 0 {
		t.Fatalf("with the flag: %s", e)
	}
	if c, e := pub("1.4.0-dev.12"); c != 0 { // a higher one is fine (and 12 > 9 as numbers)
		t.Fatal(e)
	}
}

// A key is never left without its public half.
func TestKeygenRemovesTheKeyWhenThePublicHalfCannotBeWritten(t *testing.T) {
	d := t.TempDir()
	// Something that appears at the public key's path after the private key is
	// written makes the write of the public half fail.
	afterKey = func(base string) { os.MkdirAll(base+".pub", 0o755) }
	defer func() { afterKey = nil }()
	code, _, _ := do(t, "", "keygen", "--component", "myapp", "--channel", "dev", "--dir", d, "--no-password")
	if code == 0 {
		t.Fatal("keygen succeeded")
	}
	if _, err := os.Stat(filepath.Join(d, "myapp-dev.key")); err == nil {
		t.Error("the key file was left behind")
	}
}

// A failed write leaves nothing behind and never replaces a file.
func TestWriteNoClobberLeavesNothingOnFailure(t *testing.T) {
	d := t.TempDir()
	path := filepath.Join(d, "k.key")
	if err := writeNoClobber(path, []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeNoClobber(path, []byte("two"), 0o600); err == nil {
		t.Fatal("an existing file was replaced")
	}
	if b, _ := os.ReadFile(path); string(b) != "one" {
		t.Errorf("content %q", b)
	}
	entries, _ := os.ReadDir(d)
	if len(entries) != 1 {
		t.Errorf("leftovers: %v", entries)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v", fi.Mode().Perm())
	}
}

func TestWriteNoClobberRemovesTheFileWhenTheDirectorySyncFails(t *testing.T) {
	d := t.TempDir()
	path := filepath.Join(d, "k.key")
	syncDirFn = func(string) error { return os.ErrInvalid }
	defer func() { syncDirFn = fsutil.SyncDir }()
	if err := writeNoClobber(path, []byte("one"), 0o600); err == nil {
		t.Fatal("no error")
	}
	if entries, _ := os.ReadDir(d); len(entries) != 0 {
		t.Errorf("left behind: %v", entries)
	}
}
