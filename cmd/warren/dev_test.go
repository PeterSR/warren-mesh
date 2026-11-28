package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/daemon"
)

// TestDevBinary builds the binary and runs `warren dev` on the example
// config: it prints its sockets, they answer /v1/service/self, SIGTERM stops
// it cleanly, and the temporary directory is gone afterwards.
func TestDevBinary(t *testing.T) {
	d := t.TempDir()
	bin := filepath.Join(d, "warren")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	cmd := exec.Command(bin, "dev", "--config", "../../examples/dev.yaml")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()

	var sockets []string
	var all strings.Builder
	re := regexp.MustCompile(`service \S+\s+(/\S+\.sock)`)
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		all.WriteString(sc.Text() + "\n")
		if m := re.FindStringSubmatch(sc.Text()); m != nil {
			sockets = append(sockets, m[1])
		}
		if strings.HasPrefix(sc.Text(), "Stop with") {
			break
		}
	}
	if len(sockets) != 5 {
		t.Fatalf("want 5 sockets, got %v\n%s\n%s", sockets, all.String(), errb.String())
	}
	dir := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(sockets[0]))))
	if !strings.HasPrefix(filepath.Base(dir), "warren-dev-") {
		t.Fatalf("unexpected directory %s", dir)
	}
	hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sockets[0])
		}}}
	req, _ := http.NewRequest("GET", "http://warren/v1/service/self", nil)
	req.Header.Set("X-Warren", "1")
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"relay"`) {
		t.Errorf("self: %d %s", resp.StatusCode, body)
	}
	control := filepath.Join(dir, "laptop", "control.sock")
	status, err := daemon.Control(control, "GET", "/status")
	if err != nil || strings.Contains(string(status), "drain-exit-code") {
		t.Errorf("dev status advertised a restart drain: %s (%v)", status, err)
	}
	if _, err := daemon.Control(control, "POST", "/drain-and-exit?code=75"); err == nil || !strings.Contains(err.Error(), "unknown query parameter") {
		t.Errorf("dev restart drain should reject the unknown parameter: %v", err)
	}
	if _, err := daemon.Control(control, "POST", "/drain-and-exit"); err != nil {
		t.Errorf("dev rejected the legacy drain request: %v", err)
	}
	cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("warren dev exited with %v\n%s", err, errb.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("warren dev did not stop on SIGTERM")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the temporary directory %s was not removed (%v)", dir, err)
	}
}

func TestDevRefusesProcesses(t *testing.T) {
	d := t.TempDir()
	cfg := filepath.Join(d, "dev.yaml")
	os.WriteFile(cfg, []byte("nodes:\n  - name: a\n    services:\n      - name: x\n        allow: ['*']\n        run: [sleep, '1']\n"), 0o600)
	var out, errb bytes.Buffer
	if run([]string{"dev", "--config", cfg}, &out, &errb) != 1 || !strings.Contains(errb.String(), "starts no processes") {
		t.Errorf("run: %q", errb.String())
	}
}
