package main

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/daemon"
	"github.com/PeterSR/warren-mesh/internal/link"
	"github.com/PeterSR/warren-mesh/internal/platform"
)

func TestUsage(t *testing.T) {
	var out, errb bytes.Buffer
	if run(nil, &out, &errb) != 2 || run([]string{"bogus"}, &out, &errb) != 2 {
		t.Error("usage errors should exit 2")
	}
	errb.Reset()
	if run([]string{"dev", "--config", "/nonexistent/dev.yaml"}, &out, &errb) != 1 || !strings.Contains(errb.String(), "warren dev:") {
		t.Errorf("dev without a config: %q", errb.String())
	}
	out.Reset()
	if run([]string{"version"}, &out, &errb) != 0 || strings.TrimSpace(out.String()) == "" {
		t.Error("version")
	}
	// status without a running Warren fails.
	d, _ := os.MkdirTemp("", "m")
	defer os.RemoveAll(d)
	cfg := filepath.Join(d, "c.yaml")
	os.WriteFile(cfg, []byte("node: n1\nstate_dir: "+d+"/st\n"), 0o600)
	errb.Reset()
	if run([]string{"status", "--config", cfg}, &out, &errb) != 1 || !strings.Contains(errb.String(), "cannot reach") {
		t.Errorf("status of nothing: %q", errb.String())
	}
	if run([]string{"status", "--health", "--config", cfg}, &out, &errb) != 2 {
		t.Error("--health of nothing should exit 2")
	}
	if run([]string{"status", "--config", d + "/missing.yaml"}, &out, &errb) != 1 {
		t.Error("missing config")
	}
}

// TestEndToEnd builds the binary and runs `warren run`, `status` and
// `reload` against it, then stops it with SIGTERM.
func TestEndToEnd(t *testing.T) {
	d, _ := os.MkdirTemp("", "e")
	defer os.RemoveAll(d)
	bin := filepath.Join(d, "warren")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	cfg := filepath.Join(d, "c.yaml")
	text := "node: n1\nstate_dir: " + d + "/st\nservices:\n  dir: " + d + "/s\n  list:\n    - name: echo\n      allow: [\"*\"]\n      run: [/bin/sleep, \"300\"]\nattach:\n  dir: " + d + "/a\n"
	os.WriteFile(cfg, []byte(text), 0o600)

	cmd := exec.Command(bin, "run", "--config", cfg)
	var logs bytes.Buffer
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	defer cmd.Process.Kill()

	var status string
	dl := time.Now().Add(10 * time.Second)
	for {
		out, err := exec.Command(bin, "status", "--config", cfg).CombinedOutput()
		if err == nil {
			status = string(out)
			break
		}
		if time.Now().After(dl) {
			t.Fatalf("status never worked: %s\nlogs:\n%s", out, logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The process may still be starting when the control socket first answers.
	for end := time.Now().Add(10 * time.Second); !strings.Contains(status, "process service:echo: running") && time.Now().Before(end); {
		time.Sleep(50 * time.Millisecond)
		if out, err := exec.Command(bin, "status", "--config", cfg).CombinedOutput(); err == nil {
			status = string(out)
		}
	}
	for _, want := range []string{"node n1", "warren 0.", "service echo at n1", "process service:echo: running", "memory bound"} {
		if !strings.Contains(status, want) {
			t.Errorf("status lacks %q:\n%s", want, status)
		}
	}
	if out, err := exec.Command(bin, "status", "--health", "--config", cfg).CombinedOutput(); err != nil || !strings.Contains(string(out), "healthy") {
		t.Errorf("health: %v %s", err, out)
	}
	if out, err := exec.Command(bin, "reload", "--config", cfg).CombinedOutput(); err != nil || !strings.Contains(string(out), "reloaded") {
		t.Errorf("reload: %v %s", err, out)
	}
	cmd.Process.Signal(syscall.SIGTERM)
	_ = syscall.SIGTERM
	select {
	case err := <-exited:
		if err != nil {
			t.Errorf("warren exited with %v\n%s", err, logs.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("warren did not stop on SIGTERM")
	}
}

func TestHealthIsTwoWhenTheConfigCannotBeLoaded(t *testing.T) {
	var out, errw bytes.Buffer
	if code := run([]string{"status", "--health", "--config", "/nonexistent/warren.yaml"}, &out, &errw); code != 2 {
		t.Errorf("status --health with an unloadable config returned %d, want 2", code)
	}
	if code := run([]string{"status", "--config", "/nonexistent/warren.yaml"}, &out, &errw); code != 1 {
		t.Errorf("status with an unloadable config returned %d, want 1", code)
	}
}

func TestStatusDoesNotNeedTheRestOfTheConfigExpanded(t *testing.T) {
	d := t.TempDir()
	cfg := filepath.Join(d, "c.yaml")
	text := "node: n1\nstate_dir: " + d + "/st\nservices:\n  dir: " + d + "/s\n  list:\n    - name: echo\n      allow: [\"*\"]\n      env: {TOKEN: \"${WARREN_TEST_SURELY_UNSET}\"}\n"
	os.WriteFile(cfg, []byte(text), 0o600)
	var out, errw bytes.Buffer
	code := run([]string{"status", "--health", "--config", cfg}, &out, &errw)
	// Nothing runs, so it is unreachable (2), but the error is about that,
	// not about the variable the rest of the file needs.
	if code != 2 || !strings.Contains(errw.String(), "cannot reach Warren") || strings.Contains(errw.String(), "SURELY_UNSET") {
		t.Errorf("code %d, stderr %q", code, errw.String())
	}
	errw.Reset()
	code = run([]string{"status", "--health", "--control", filepath.Join(d, "none.sock")}, &out, &errw)
	if code != 2 || !strings.Contains(errw.String(), "cannot reach Warren") {
		t.Errorf("with --control: code %d, stderr %q", code, errw.String())
	}
}

func TestPrintStatusShowsPinnedAndNotUpdatable(t *testing.T) {
	var b strings.Builder
	printStatus(&b, []byte(`{"node":"n1","boot":"abcdef0123","warren":{"version":"1.0.0","updatable":false,"not_updatable":"container"},"pinned":"docker image x:y",
		"processes":[{"name":"service:a","state":"running","updatable":false,"not_updatable":"read-only"},{"name":"service:b","state":"running","updatable":true},{"name":"service:c","state":"running"}]}`))
	out := b.String()
	for _, want := range []string{"pinned: docker image x:y\n", "warren binary: not updatable: container\n", "process service:a: running [not updatable: read-only]\n", "process service:b: running\n", "process service:c: running\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	b.Reset()
	printStatus(&b, []byte(`{"node":"n1","boot":"abcdef0123","warren":{"version":"1.0.0"}}`))
	if strings.Contains(b.String(), "pinned") || strings.Contains(b.String(), "updatable") {
		t.Errorf("status shows what is not there:\n%s", b.String())
	}
}

func TestPrintStatusEscapesWhatIsNotPlainText(t *testing.T) {
	var b strings.Builder
	printStatus(&b, []byte(`{"node":"n1","boot":"abcdef0123","warren":{"version":"1.0.0"},"pinned":"a\u202eb\u001b[31m"}`))
	if strings.ContainsAny(b.String(), "\u202e\x1b") || !strings.Contains(b.String(), `pinned: a\u202eb\u001b[31m`) {
		t.Errorf("status:\n%q", b.String())
	}
}

// The marker comes through the real daemon: processes in /status carry
// updatable and not_updatable.
func TestStatusShowsNotUpdatableFromARealDaemon(t *testing.T) {
	dir, err := os.MkdirTemp("", "ws")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	cfgPath := filepath.Join(dir, "warren.yaml")
	os.WriteFile(cfgPath, []byte(strings.ReplaceAll(`
node: n1
state_dir: DIR/state
services:
  dir: DIR/s
  list:
    - name: sleeper
      allow: ["*"]
      run: [/bin/sleep, "30"]
`, "DIR", dir)), 0o600)
	env := &platform.UpdatableEnv{
		Access: func(p string, mode uint32) error {
			if p == "/bin/sleep" || p == "/usr/bin/sleep" {
				return syscall.EROFS
			}
			return nil
		},
		Dev:  func(string) (uint64, error) { return 1, nil },
		Root: "/",
	}
	d, err := daemon.Start(daemon.Options{ConfigPath: cfgPath, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Link: link.NewNet().Join("n1"), Updatable: env})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Stop)
	var out, errw bytes.Buffer
	deadline := time.Now().Add(10 * time.Second)
	for {
		out.Reset()
		code := run([]string{"status", "--control", filepath.Join(dir, "control.sock")}, &out, &errw)
		if code == 0 && strings.Contains(out.String(), "process service:sleeper: running") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no process line:\n%s%s", out.String(), errw.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(out.String(), "[not updatable: read-only]") {
		b, _ := daemon.Control(filepath.Join(dir, "control.sock"), "GET", "/status")
		t.Errorf("no marker in:\n%s\n%s", out.String(), b)
	}
}
