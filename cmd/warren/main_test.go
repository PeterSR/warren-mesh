package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
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
