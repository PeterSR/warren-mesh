package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/link"
	"github.com/PeterSR/warren-mesh/internal/platform"
)

func TestBadInvocations(t *testing.T) {
	defer platform.SetDumpable()
	for name, c := range map[string]struct {
		args, env []string
	}{
		"bad settings": {nil, []string{"WARREN_TRANSPORT_SETTINGS={nope"}},
		"no tag":       {nil, []string{`WARREN_TRANSPORT_SETTINGS={"port":1}`}},
		"one link":     {[]string{"--link-a", "/x"}, []string{`WARREN_TRANSPORT_SETTINGS={"tag":"t"}`}},
		"no links":     {nil, []string{`WARREN_TRANSPORT_SETTINGS={"tag":"t"}`}},
	} {
		var errw bytes.Buffer
		if code := run(c.args, c.env, &errw); code != 2 {
			t.Errorf("%s: exit %d (%s)", name, code, errw.String())
		}
	}
	// The very first thing is to stop other processes from attaching.
	if d, err := platform.Dumpable(); err != nil || d {
		t.Errorf("still dumpable after run: %v %v", d, err)
	}
}

// TestInheritedDescriptors builds the binary and runs it the way Warren
// does: two socket pairs as descriptors 3 and 4, settings in the environment,
// against a fake tailscaled.
func TestInheritedDescriptors(t *testing.T) {
	dir, err := os.MkdirTemp("", "wt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	exe := filepath.Join(dir, "warren-tailscale")
	if out, err := exec.Command("go", "build", "-o", exe, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	sock := filepath.Join(dir, "tailscaled.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/localapi/v0/status" || r.Header.Get("Sec-Tailscale") != "localapi" {
			http.Error(w, "no", 404)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"BackendState": "Running", "MagicDNSSuffix": "t.ts.net",
			"Self": map[string]any{"DNSName": "solo.t.ts.net.", "TailscaleIPs": []string{"127.0.0.1"}}})
	})}
	go srv.Serve(ln)
	defer srv.Close()

	m := link.NewManaged("solo")
	files, release, err := m.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe)
	cmd.ExtraFiles = files // descriptors 3 and 4
	settings, _ := json.Marshal(map[string]any{"tag": "warren-test", "port": 0, "socket": sock, "listen": "127.0.0.1:0"})
	cmd.Env = []string{"WARREN_TRANSPORT_SETTINGS=" + string(settings), "PATH=" + os.Getenv("PATH")}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	release()
	defer func() { cmd.Process.Kill(); cmd.Wait(); m.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var h link.Hello
	for {
		if h, err = m.Hello(ctx); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("no hello: %v\n%s", err, stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if h.Name != "tailscale" || h.Node != "solo" || link.CheckHello(h, "solo") != nil {
		t.Errorf("hello %+v", h)
	}
	// The process made itself non-dumpable: the files in its /proc entry belong to root.
	if os.Geteuid() != 0 {
		var st syscall.Stat_t
		if err := syscall.Stat(fmt.Sprintf("/proc/%d/environ", cmd.Process.Pid), &st); err != nil || st.Uid == uint32(os.Geteuid()) {
			t.Errorf("the transport is dumpable: /proc/pid/environ owned by %d (%v)", st.Uid, err)
		}
	}
	// A broken link ends the process, so that Warren starts it again.
	m.Down()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(stderr.String(), "link") {
			t.Errorf("exit %v: %s", err, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the transport kept running after its links closed")
	}
}
