package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/config"
	"github.com/PeterSR/warren-mesh/internal/link"
	"github.com/PeterSR/warren-mesh/internal/platform"
	"github.com/PeterSR/warren-mesh/internal/service"
	"github.com/PeterSR/warren-mesh/internal/supervise"
)

// The test binary doubles as the managed transport and the managed echo
// service: Warren runs it with --role.
func TestMain(m *testing.M) {
	if len(os.Args) > 2 && os.Args[1] == "--role" {
		switch os.Args[2] {
		case "transport":
			roleTransport(os.Args[3])
		case "transport-once":
			// The first start closes both links and stays alive; later starts work.
			if _, err := os.Stat(os.Args[4]); err != nil {
				os.WriteFile(os.Args[4], nil, 0o600)
				os.NewFile(3, "a").Close()
				os.NewFile(4, "b").Close()
				select {}
			}
			roleTransport(os.Args[3])
		case "transport-flaky":
			// Answers hello, then exits a moment later, at every start.
			go roleTransport(os.Args[3])
			time.Sleep(150 * time.Millisecond)
			os.Exit(1)
		case "echo":
			roleEcho()
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type roleBackend struct{ node string }

func (b roleBackend) Hello(context.Context) link.Hello {
	return link.Hello{Version: 1, Node: b.node, MaxBody: link.MinMaxBody, Name: "roletransport"}
}
func (b roleBackend) Peers(done <-chan struct{}, since string, wait time.Duration) link.PeerList {
	select {
	case <-done:
	case <-time.After(wait):
	}
	return link.PeerList{Version: "1", Peers: []string{}}
}
func (b roleBackend) Carry(*http.Request, string, *link.Request) (*link.Response, error) {
	return nil, &link.CarryError{Kind: link.NotSent, Detail: "no network"}
}

func roleTransport(node string) {
	supervise.Harden()
	c, err := net.FileConn(os.NewFile(3, "link-a"))
	if err != nil {
		os.Exit(3)
	}
	_ = link.ServeConn(c, link.TransportHandler(roleBackend{node}))
}

// roleEcho leases the socket Warren named and answers every call with what
// it can see of its own environment.
func roleEcho() {
	sock := os.Getenv("WARREN_SERVICE_SOCKET")
	hc := &http.Client{Transport: &http.Transport{DialContext: unixDialer(sock)}, Timeout: 10 * time.Second}
	do := func(method, pq string, body string) (*http.Response, []byte) {
		req, _ := http.NewRequest(method, "http://w"+pq, strings.NewReader(body))
		req.Header.Set("X-Warren", "1")
		r, err := hc.Do(req)
		if err != nil {
			os.Exit(4)
		}
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		return r, b
	}
	_, b := do("POST", "/v1/service/serve?version=1&take=1", "")
	var l struct{ Lease string }
	json.Unmarshal(b, &l)
	for {
		r, body := do("GET", "/v1/service/calls?lease="+l.Lease+"&wait=1", "")
		switch r.StatusCode {
		case 200:
			reply := fmt.Sprintf("%s sock=%s leak=%q pass=%q pid=%d", body, sock, os.Getenv("LEAK_ME"), os.Getenv("PASS_ME"), os.Getpid())
			if string(body) == "die" {
				os.Exit(9)
			}
			do("POST", "/v1/service/answer?lease="+l.Lease+"&id="+r.Header.Get("X-Warren-Call"), reply)
		case 204:
		default:
			os.Exit(5)
		}
	}
}

// ---- helpers ----

func short(t *testing.T) string {
	d, err := os.MkdirTemp("", "d")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func fast(t *testing.T) {
	sc, lg, si := service.ScanEvery, service.LeaseGrace, service.ServeInterval
	bs, ka := supervise.BackoffStart, supervise.KillAfter
	service.ScanEvery, service.LeaseGrace, service.ServeInterval = 40*time.Millisecond, 2*time.Second, 0
	supervise.BackoffStart, supervise.KillAfter = 50*time.Millisecond, 500*time.Millisecond
	t.Cleanup(func() {
		service.ScanEvery, service.LeaseGrace, service.ServeInterval = sc, lg, si
		supervise.BackoffStart, supervise.KillAfter = bs, ka
	})
}

func cfgText(dir, transport, extra string) string {
	exe, _ := os.Executable()
	return strings.NewReplacer("DIR", dir, "EXE", exe, "TRANSPORT", transport, "EXTRA", extra).Replace(`
node: n1
state_dir: DIR/state
TRANSPORT
services:
  dir: DIR/s
  list:
    - name: echo
      allow: ["*"]
      run: [EXE, --role, echo]
      env: {PASS_ME: from-config}
EXTRA
attach:
  dir: DIR/a
  default:
    services:
      - name: echo
        allow: ["*"]
  children:
    - name: host
      flat: true
      services:
        - name: echo
          allow: ["*"]
`)
}

// manyStreams makes a config whose template children, at their largest
// pools, need more streams than a transport link allows.
func manyStreams(text string) string {
	return strings.Replace(text, "  default:\n    services:\n      - name: echo\n        allow: [\"*\"]\n",
		"  max_children: 100\n  default:\n    services:\n      - name: echo\n        allow: [\"*\"]\n        inflight: 32\n        outflight: 28\n", 1)
}

type harness struct {
	t    *testing.T
	dir  string
	path string
	d    *Daemon
	text string
}

func start(t *testing.T, transport, extra string, opts ...func(*Options)) (*harness, error) {
	t.Helper()
	fast(t)
	h := &harness{t: t, dir: short(t)}
	os.MkdirAll(filepath.Join(h.dir, "a", "host"), 0o755)
	h.text = cfgText(h.dir, transport, extra)
	h.path = filepath.Join(h.dir, "warren.yaml")
	os.WriteFile(h.path, []byte(h.text), 0o600)
	o := Options{ConfigPath: h.path, Log: quiet()}
	for _, f := range opts {
		f(&o)
	}
	d, err := Start(o)
	if err != nil {
		return nil, err
	}
	h.d = d
	t.Cleanup(func() {
		defer func() { recover() }()
		d.Stop()
	})
	return h, nil
}

func unixClient(path string) *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: unixDialer(path), DisableKeepAlives: true}, Timeout: 10 * time.Second}
}

// callFrom makes a call from the host child to the node's echo service.
func callFrom(t *testing.T, dir, body string) (int, string) {
	hc := unixClient(filepath.Join(dir, "a", "host", "echo.sock"))
	req, _ := http.NewRequest("POST", "http://w/v1/service/call?to=n1&method=m&version=1&timeout_ms=5000", strings.NewReader(body))
	req.Header.Set("X-Warren", "1")
	r, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return r.StatusCode, string(b)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	dl := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(dl) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func transportYAML() string {
	exe, _ := os.Executable()
	return "transport: {run: [" + exe + ", --role, transport, n1]}"
}

// ---- tests ----

func TestManagedServiceAndTransport(t *testing.T) {
	t.Setenv("LEAK_ME", "secret")
	h, err := start(t, transportYAML(), "")
	if err != nil {
		t.Fatal(err)
	}
	// The managed echo service answers calls from a child.
	eventually(t, "echo answers", func() bool {
		s, b := callFrom(t, h.dir, "hi")
		return s == 200 && strings.HasPrefix(b, "hi ")
	})
	_, b := callFrom(t, h.dir, "hi")
	if !strings.Contains(b, "sock="+filepath.Join(h.dir, "s", "echo.sock")) {
		t.Errorf("WARREN_SERVICE_SOCKET missing: %s", b)
	}
	if !strings.Contains(b, `leak=""`) || !strings.Contains(b, `pass="from-config"`) {
		t.Errorf("environment: %s", b)
	}
	// The transport's hello was accepted and shows in status.
	st := h.d.Status()
	if st["transport"].(map[string]any)["name"] != "roletransport" {
		t.Errorf("transport %v", st["transport"])
	}
	procs := st["processes"].([]supervise.Status)
	if len(procs) != 2 || procs[0].State != supervise.Running {
		t.Errorf("processes %+v", procs)
	}
	if st["memory_bound"].(int64) <= 0 {
		t.Error("memory bound missing")
	}
	w := st["warren"].(map[string]any)
	if w["version"] == "" {
		t.Error("version missing")
	}
}

func TestEnvPassGivesOnlyNamedVariables(t *testing.T) {
	t.Setenv("LEAK_ME", "secret")
	t.Setenv("PASS_ME", "from-env")
	h, err := start(t, "", "      env_pass: [PASS_ME]\n")
	if err != nil {
		t.Fatal(err)
	}
	_ = h
	var b string
	eventually(t, "echo answers", func() bool {
		var s int
		s, b = callFrom(t, h.dir, "x")
		return s == 200
	})
	// env: wins over env_pass for the same name; LEAK_ME is not named.
	if !strings.Contains(b, `pass="from-config"`) || !strings.Contains(b, `leak=""`) {
		t.Errorf("environment: %s", b)
	}
}

func TestCrashedServiceLosesItsLeaseAtOnceAndIsRestarted(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "echo answers", func() bool { s, _ := callFrom(t, h.dir, "x"); return s == 200 })
	pid := func() int {
		for _, p := range h.d.Status()["processes"].([]supervise.Status) {
			if p.Name == "service:echo" {
				return p.PID
			}
		}
		return 0
	}
	first := pid()
	// The service dies mid-call ("die" exits without answering).
	hc := unixClient(filepath.Join(h.dir, "a", "host", "echo.sock"))
	req, _ := http.NewRequest("POST", "http://w/v1/service/call?to=n1&method=m&version=1&timeout_ms=5000", strings.NewReader("die"))
	req.Header.Set("X-Warren", "1")
	start := time.Now()
	r, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	// The handed-out call fails unknown at once, not at its 5 s deadline
	// and not after the 2 s grace.
	if r.Header.Get("X-Warren-Error") != "unknown" || time.Since(start) > time.Second {
		t.Errorf("call after crash: %s in %v", r.Header.Get("X-Warren-Error"), time.Since(start))
	}
	eventually(t, "restarted and serving", func() bool {
		s, b := callFrom(t, h.dir, "again")
		return s == 200 && strings.HasPrefix(b, "again ")
	})
	if pid() == first {
		t.Error("same process after a crash")
	}
}

func TestShutdownOrderAndProcessesStopped(t *testing.T) {
	var mu sync.Mutex
	var steps []string
	h, err := start(t, transportYAML(), "", func(o *Options) {
		o.Step = func(s string) { mu.Lock(); steps = append(steps, s); mu.Unlock() }
	})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "echo answers", func() bool { s, _ := callFrom(t, h.dir, "x"); return s == 200 })
	var pids []int
	for _, p := range h.d.Status()["processes"].([]supervise.Status) {
		pids = append(pids, p.PID)
	}
	h.d.Stop()
	mu.Lock()
	got := strings.Join(steps, ",")
	mu.Unlock()
	if got != "drain,stop-services,stop-transport,close-node" {
		t.Errorf("shutdown steps: %s", got)
	}
	for _, pid := range pids {
		if err := syscall.Kill(pid, 0); err == nil {
			t.Errorf("process %d still alive", pid)
		}
	}
	if _, err := os.Lstat(filepath.Join(h.dir, "s", "echo.sock")); err == nil {
		t.Error("service socket left behind")
	}
	if _, err := os.Lstat(ControlPath(h.d.cfg)); err == nil {
		t.Error("control socket left behind")
	}
	// Stop twice is harmless.
	h.d.Stop()
}

func TestTransportHelloMismatchRefusesToStart(t *testing.T) {
	exe, _ := os.Executable()
	// The transport believes it serves node "other".
	_, err := start(t, "transport: {run: ["+exe+", --role, transport, other]}", "")
	if err == nil || !strings.Contains(err.Error(), `"other"`) {
		t.Fatalf("start with a mismatched transport: %v", err)
	}
}

func TestTransportThatNeverAnswersIsNotARefusal(t *testing.T) {
	old := HelloTimeout
	HelloTimeout = 700 * time.Millisecond
	defer func() { HelloTimeout = old }()
	h, err := start(t, "transport: {run: [/bin/sleep, \"30\"]}", "")
	if err != nil {
		t.Fatalf("an unanswered hello stopped Warren: %v", err)
	}
	// Warren runs with the link down: unhealthy, but the control socket
	// answers and the node serves.
	st := h.d.Status()
	if st["link_up"] != false || st["healthy"] != false {
		t.Errorf("status with no transport answering: link_up=%v healthy=%v", st["link_up"], st["healthy"])
	}
	if _, err := Control(ControlPath(h.d.cfg), "GET", "/status"); err != nil {
		t.Errorf("control socket: %v", err)
	}
}

func TestControlStatusAndReload(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	ctl := ControlPath(h.d.cfg)
	b, err := Control(ctl, "GET", "/status")
	if err != nil {
		t.Fatal(err)
	}
	var st map[string]any
	json.Unmarshal(b, &st)
	if st["node"] != "n1" || st["memory_bound"] == nil || st["processes"] == nil {
		t.Errorf("status %s", b)
	}
	if fi, _ := os.Stat(ctl); fi.Mode().Perm() != 0o600 {
		t.Errorf("control socket mode %v", fi.Mode().Perm())
	}
	boot := st["boot"]

	// Reload: add a template service name; boot stays.
	os.WriteFile(h.path, []byte(strings.Replace(h.text, "  default:\n    services:\n", "  default:\n    services:\n      - name: extra\n        allow: [\"*\"]\n", 1)), 0o600)
	if _, err := Control(ctl, "POST", "/reload"); err != nil {
		t.Fatal(err)
	}
	b, _ = Control(ctl, "GET", "/status")
	json.Unmarshal(b, &st)
	if st["boot"] != boot {
		t.Error("boot changed")
	}

	// A refused reload reports why and changes nothing.
	os.WriteFile(h.path, []byte(strings.Replace(h.text, "  children:\n    - name: host\n      flat: true\n      services:\n        - name: echo\n          allow: [\"*\"]\n", "  children: []\n", 1)), 0o600)
	_, err = Control(ctl, "POST", "/reload")
	if err == nil || !strings.Contains(err.Error(), "still exists") {
		t.Errorf("removal of host not refused: %v", err)
	}
	// A broken file is refused too, and Warren keeps running.
	os.WriteFile(h.path, []byte("node: n1\nbogus: 1\n"), 0o600)
	if _, err := Control(ctl, "POST", "/reload"); err == nil {
		t.Error("broken config accepted")
	}
	if s, _ := callFrom(t, h.dir, "x"); s != 200 && s != 404 && s != 504 {
		t.Errorf("Warren stopped answering: %d", s)
	}
}

func TestReloadChangesManagedServices(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "echo answers", func() bool { s, _ := callFrom(t, h.dir, "x"); return s == 200 })
	// Changing run's environment restarts the process.
	os.WriteFile(h.path, []byte(strings.Replace(h.text, "PASS_ME: from-config", "PASS_ME: changed", 1)), 0o600)
	if err := h.d.Reload(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "restarted with the new environment", func() bool {
		s, b := callFrom(t, h.dir, "x")
		return s == 200 && strings.Contains(b, `pass="changed"`)
	})
	// Removing run stops the process.
	os.WriteFile(h.path, []byte(strings.Replace(h.text, "      run: [", "      xrun: [", 1)), 0o600)
	if err := h.d.Reload(); err == nil {
		t.Log("xrun is an unknown key, refused as it should be")
	}
	os.WriteFile(h.path, []byte(strings.NewReplacer("      run: [", "      #run: [", "      env: {PASS_ME: from-config}\n", "").Replace(h.text)), 0o600)
	if err := h.d.Reload(); err != nil {
		t.Fatal(err)
	}
	if len(h.d.Status()["processes"].([]supervise.Status)) != 0 {
		t.Errorf("service process still managed: %+v", h.d.Status()["processes"])
	}
}

func TestRunStopsOnContextAndReloadsOnSIGHUP(t *testing.T) {
	fast(t)
	dir := short(t)
	os.MkdirAll(filepath.Join(dir, "a", "host"), 0o755)
	text := cfgText(dir, "", "")
	path := filepath.Join(dir, "warren.yaml")
	os.WriteFile(path, []byte(text), 0o600)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		code, err := Run(ctx, Options{ConfigPath: path, Log: quiet()})
		if code != 0 {
			t.Errorf("exit code %d, want 0", code)
		}
		done <- err
	}()
	cfg, _ := config.Load(path)
	ctl := ControlPath(cfg)
	eventually(t, "control socket", func() bool { _, err := Control(ctl, "GET", "/status"); return err == nil })
	// SIGHUP applies a config change without a restart.
	os.WriteFile(path, []byte(strings.Replace(text, "  default:\n    services:\n", "  default:\n    services:\n      - name: extra\n        allow: [\"*\"]\n", 1)), 0o600)
	syscall.Kill(os.Getpid(), syscall.SIGHUP)
	os.MkdirAll(filepath.Join(dir, "a", "tmpl"), 0o755)
	eventually(t, "SIGHUP applied", func() bool {
		_, err := os.Stat(filepath.Join(dir, "a", "tmpl", "extra.sock"))
		return err == nil
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
	if _, err := exec.LookPath("true"); err != nil {
		t.Skip()
	}
}

func TestDrainAndExitAndHealth(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		code  int
	}{
		{name: "stop"},
		{name: "restart", query: "?code=75", code: 75},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fast(t)
			dir := short(t)
			os.MkdirAll(filepath.Join(dir, "a", "host"), 0o755)
			path := filepath.Join(dir, "warren.yaml")
			os.WriteFile(path, []byte(cfgText(dir, "", "")), 0o600)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan int, 1)
			go func() {
				code, err := Run(ctx, Options{ConfigPath: path, Log: quiet()})
				if err != nil {
					t.Error(err)
				}
				done <- code
			}()
			cfg, _ := config.Load(path)
			ctl := ControlPath(cfg)
			if ctl != filepath.Join(dir, "control.sock") {
				t.Errorf("control default %s", ctl)
			}
			eventually(t, "control socket", func() bool { _, err := Control(ctl, "GET", "/status"); return err == nil })
			b, _ := Control(ctl, "GET", "/status")
			var st map[string]any
			if err := json.Unmarshal(b, &st); err != nil || st["healthy"] != true {
				t.Errorf("not healthy: %s", b)
			}
			if _, err := Control(ctl, "POST", "/drain-and-exit"+tc.query); err != nil {
				t.Fatal(err)
			}
			select {
			case code := <-done:
				if code != tc.code {
					t.Errorf("exit code %d, want %d", code, tc.code)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("did not exit")
			}
		})
	}
}

func TestDrainAndExitKeepsFirstAcceptedCode(t *testing.T) {
	h, err := start(t, "", "", func(o *Options) { o.restartDrain = true })
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"?code=0", "?code=1", "?code=075", "?code=75&code=75", "?code=75&x=1", "?code", "?=75", "?code=75;"} {
		if code, b := ctlDo(t, h, "POST", "/drain-and-exit"+query); code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", query, code, b)
		}
	}
	select {
	case <-h.d.exit:
		t.Fatal("an invalid request started a drain")
	default:
	}
	for _, tc := range []struct {
		query string
		code  int
	}{
		{query: "?code=75", code: http.StatusNoContent},
		{code: http.StatusConflict},
		{query: "?code=75", code: http.StatusNoContent},
	} {
		if code, b := ctlDo(t, h, "POST", "/drain-and-exit"+tc.query); code != tc.code {
			t.Errorf("%s: %d %s, want %d", tc.query, code, b, tc.code)
		}
	}
	select {
	case <-h.d.exit:
	default:
		t.Fatal("the accepted request did not start a drain")
	}
	if h.d.exitCode != RestartExitCode {
		t.Errorf("exit code %d, want %d", h.d.exitCode, RestartExitCode)
	}
}

func TestTooManyStreamsRefusesToStart(t *testing.T) {
	_, err := start(t, "", "", func(o *Options) {
		os.WriteFile(o.ConfigPath, []byte(manyStreams(cfgText(filepath.Dir(o.ConfigPath), "", ""))), 0o600)
	})
	if err == nil || !strings.Contains(err.Error(), "streams") {
		t.Fatalf("got %v", err)
	}
}

func TestStreamCheckComesBeforeTheTransportStarts(t *testing.T) {
	// A transport that cannot start: were it started first, the hello wait
	// would be what fails.
	_, err := start(t, "transport: {run: [/nonexistent/transport]}", "", func(o *Options) {
		dir := filepath.Dir(o.ConfigPath)
		text := manyStreams(cfgText(dir, "transport: {run: [/nonexistent/transport]}", ""))
		os.WriteFile(o.ConfigPath, []byte(text), 0o600)
	})
	if err == nil || !strings.Contains(err.Error(), "streams") {
		t.Fatalf("got %v", err)
	}
}

func TestTransportSettingsUseLowerCaseKeys(t *testing.T) {
	d := &Daemon{}
	cfg := &config.Config{Tailscale: &config.Tailscale{Tag: "tag:warren", Listen: "0.0.0.0", Port: 7000, Socket: "/s"}}
	got := d.transportSettings(cfg)["WARREN_TRANSPORT_SETTINGS"]
	var m map[string]any
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatal(err)
	}
	if m["tag"] != "tag:warren" || m["port"] != float64(7000) || m["listen"] != "0.0.0.0" || m["socket"] != "/s" {
		t.Errorf("settings %s", got)
	}
}

// A reload that stops a slow process must not stop Status from answering.
func TestStatusAnswersWhileAReloadStopsAProcess(t *testing.T) {
	slow := "    - name: slow\n      allow: [\"*\"]\n      run: [/bin/sh, -c, \"trap '' TERM; while :; do sleep 1; done\"]\n"
	h, err := start(t, "", slow)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "the slow service runs", func() bool {
		n := 0
		for _, p := range h.d.Status()["processes"].([]supervise.Status) {
			if p.PID > 0 {
				n++
			}
		}
		return n == 2
	})
	time.Sleep(200 * time.Millisecond) // let the shell set its trap
	os.WriteFile(h.path, []byte(strings.Replace(h.text, slow, "", 1)), 0o600)
	done := make(chan error, 1)
	go func() { done <- h.d.Reload() }()
	var worst time.Duration
	for running := true; running; {
		start := time.Now()
		h.d.Status()
		if d := time.Since(start); d > worst {
			worst = d
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			running = false
		case <-time.After(10 * time.Millisecond):
		}
	}
	if worst > 250*time.Millisecond {
		t.Errorf("Status was blocked for %v during a reload", worst)
	}
}

// A transport that drops its link but stays alive is restarted, so that new
// socket pairs are made. Without that, the hello would never be answered.
func TestTransportThatDropsItsLinkIsRestarted(t *testing.T) {
	exe, _ := os.Executable()
	marker := filepath.Join(short(t), "marker")
	tr := "transport: {run: [" + exe + ", --role, transport-once, n1, " + marker + "]}"
	h, err := start(t, tr, "")
	if err != nil {
		t.Fatalf("the transport was not restarted: %v", err)
	}
	for _, p := range h.d.Status()["processes"].([]supervise.Status) {
		if p.Name == "transport" && p.Restarts < 1 {
			t.Errorf("the transport was never restarted: %+v", p)
		}
	}
}

func TestSecondWarrenOnTheSameStateDirIsRefused(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = Start(Options{ConfigPath: h.path, Log: quiet()})
	if err == nil || !strings.Contains(err.Error(), "already runs") {
		t.Fatalf("a second Warren started: %v", err)
	}
	// The first one is unharmed, and its stop frees the lock.
	eventually(t, "echo answers", func() bool { s, _ := callFrom(t, h.dir, "x"); return s == 200 })
	h.d.Stop()
	d2, err := Start(Options{ConfigPath: h.path, Log: quiet()})
	if err != nil {
		t.Fatalf("start after the first stopped: %v", err)
	}
	d2.Stop()
}

func TestReloadRefusesMoreStreamsThanTheLinkAllows(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	big := manyStreams(h.text)
	os.WriteFile(h.path, []byte(big), 0o600)
	if err := h.d.Reload(); err == nil || !strings.Contains(err.Error(), "streams") {
		t.Fatalf("reload: %v", err)
	}
	if got := service.StreamNeed(h.d.Node().Config()); got > link.MinStreams {
		t.Errorf("the refused config was applied (%d streams)", got)
	}
}

func TestControlSocketIsMadeAgainWhenItDisappears(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	ctl := ControlPath(h.d.cfg)
	if err := os.Remove(ctl); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the control socket to come back", func() bool {
		_, err := Control(ctl, "GET", "/status")
		return err == nil
	})
}

type logCap struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logCap) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *logCap) count(s string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Count(l.b.String(), s)
}

func TestShutdownDoesNotLogALinkBreak(t *testing.T) {
	var lc logCap
	h, err := start(t, transportYAML(), "", func(o *Options) { o.Log = slog.New(slog.NewTextHandler(&lc, nil)) })
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "echo answers", func() bool { s, _ := callFrom(t, h.dir, "x"); return s == 200 })
	h.d.Stop()
	if n := lc.count("transport link broke"); n != 0 {
		t.Errorf("shutdown logged a link break %d times", n)
	}
}

func TestARealLinkBreakIsLoggedOnce(t *testing.T) {
	exe, _ := os.Executable()
	marker := filepath.Join(short(t), "marker")
	var lc logCap
	tr := "transport: {run: [" + exe + ", --role, transport-once, n1, " + marker + "]}"
	_, err := start(t, tr, "", func(o *Options) { o.Log = slog.New(slog.NewTextHandler(&lc, nil)) })
	if err != nil {
		t.Fatal(err)
	}
	if n := lc.count("transport link broke"); n != 1 {
		t.Errorf("a break was logged %d times, want once", n)
	}
}

func TestReloadAndStopDoNotInterleave(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	h.d.Stop()
	if err := h.d.Reload(); err == nil || !strings.Contains(err.Error(), "shutting down") {
		t.Errorf("a reload after Stop: %v", err)
	}
}

// A SIGTERM while Warren waits for the transport ends the startup and
// shuts down cleanly, instead of killing the process mid-start.
func TestSignalDuringStartupStopsCleanly(t *testing.T) {
	old := HelloTimeout
	HelloTimeout = 20 * time.Second
	defer func() { HelloTimeout = old }()
	dir := short(t)
	path := filepath.Join(dir, "warren.yaml")
	os.WriteFile(path, []byte(cfgText(dir, "transport: {run: [/bin/sleep, \"30\"]}", "")), 0o600)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		code, err := Run(ctx, Options{ConfigPath: path, Log: quiet()})
		if code != 0 {
			t.Errorf("exit code %d, want 0", code)
		}
		done <- err
	}()
	time.Sleep(500 * time.Millisecond) // Start is now waiting for the hello
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after an early stop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not end on the stop request")
	}
}

// A SIGHUP while Warren is still starting must not kill it: it waits, and
// is applied once Start has returned.
func TestSIGHUPDuringStartupDoesNotKill(t *testing.T) {
	fast(t)
	dir := short(t)
	path := filepath.Join(dir, "warren.yaml")
	os.WriteFile(path, []byte(cfgText(dir, "", "")), 0o600)
	var first atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		code, err := Run(ctx, Options{ConfigPath: path, Log: quiet(), Load: func() (*config.Config, error) {
			if first.CompareAndSwap(false, true) {
				time.Sleep(500 * time.Millisecond) // startup is slow
			}
			return config.Load(path)
		}})
		if code != 0 {
			t.Errorf("exit code %d, want 0", code)
		}
		done <- err
	}()
	time.Sleep(150 * time.Millisecond)
	syscall.Kill(os.Getpid(), syscall.SIGHUP) // would terminate the process before the fix
	cfg, _ := config.Load(path)
	eventually(t, "control socket", func() bool { _, err := Control(ControlPath(cfg), "GET", "/status"); return err == nil })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
}

// ---- the services endpoints of the control API ----

func ctlDo(t *testing.T, h *harness, method, uri string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, "http://warren"+uri, nil)
	r, err := unixClient(ControlPath(h.d.cfg)).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return r.StatusCode, b
}

func getService(t *testing.T, h *harness, name string) (serviceObject, int) {
	t.Helper()
	code, b := ctlDo(t, h, "GET", "/services/"+name)
	var s serviceObject
	if code == 200 {
		if err := json.Unmarshal(b, &s); err != nil {
			t.Fatalf("%s: %v", b, err)
		}
	}
	return s, code
}

func TestControlServicesListAndGet(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	exe, _ := os.Executable()
	eventually(t, "echo holds a lease", func() bool { s, c := getService(t, h, "echo"); return c == 200 && s.Lease.Held })
	s, _ := getService(t, h, "echo")
	if s.Exe != exe || s.State != "running" || s.StartSeq != 1 || s.Starts != 1 || s.Lease.Epoch != 1 || s.Lease.Since.IsZero() || s.Boot == "" || s.PID == 0 {
		t.Errorf("service: %+v", s)
	}
	code, b := ctlDo(t, h, "GET", "/services")
	var all []serviceObject
	if code != 200 || json.Unmarshal(b, &all) != nil || len(all) != 1 || all[0].Name != "echo" {
		t.Errorf("list: %d %s", code, b)
	}
	if _, code := getService(t, h, "host"); code != 404 { // a child's service is not managed
		t.Errorf("unmanaged name: %d", code)
	}
	if code, _ := ctlDo(t, h, "POST", "/services/nobody/restart"); code != 404 {
		t.Errorf("restart of an unmanaged name: %d", code)
	}
	if code, _ := ctlDo(t, h, "GET", "/services/echo/restart"); code != 405 {
		t.Errorf("GET restart: %d", code)
	}
}

func TestControlRefusesUnknownQueryParameters(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range [][2]string{{"GET", "/status?x=1"}, {"GET", "/services?code=75"}, {"GET", "/services/echo?a"}, {"POST", "/services/echo/restart?x=1"}, {"POST", "/reload?x=1"}} {
		if code, b := ctlDo(t, h, c[0], c[1]); code != 400 || !strings.Contains(string(b), "unknown query parameter") {
			t.Errorf("%s %s: %d %s", c[0], c[1], code, b)
		}
	}
	if s, _ := getService(t, h, "echo"); s.StartSeq != 1 {
		t.Errorf("a refused restart restarted: %+v", s)
	}
}

func TestControlRestartRestartsAndCountsItAsRequested(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "echo holds a lease", func() bool { s, c := getService(t, h, "echo"); return c == 200 && s.Lease.Held })
	code, b := ctlDo(t, h, "POST", "/services/echo/restart")
	var ans struct {
		Accepted    bool `json:"accepted"`
		MinStartSeq int  `json:"min_start_seq"`
	}
	if code != 200 || json.Unmarshal(b, &ans) != nil || !ans.Accepted || ans.MinStartSeq != 2 {
		t.Fatalf("restart: %d %s", code, b)
	}
	eventually(t, "the new process holds a lease", func() bool {
		s, _ := getService(t, h, "echo")
		return s.StartSeq >= 2 && s.Lease.Held && s.Lease.Epoch == 2
	})
	s, _ := getService(t, h, "echo")
	if s.Exits != 1 || s.RequestedExits != 1 || !s.LastExitRequested || s.StartFailures != 0 {
		t.Errorf("counters: %+v", s)
	}
}

// A reload that changes a service's run removes and adds it again: its
// counters and the lease epoch carry on, and 404 is only a window.
func TestControlCountersAndEpochSurviveAReloadThatReAddsTheService(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "echo holds a lease", func() bool { s, c := getService(t, h, "echo"); return c == 200 && s.Lease.Held })
	before, _ := getService(t, h, "echo")
	os.WriteFile(h.path, []byte(strings.Replace(h.text, "PASS_ME: from-config", "PASS_ME: changed", 1)), 0o600)
	if err := h.d.Reload(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the re-added process holds a lease", func() bool {
		s, c := getService(t, h, "echo")
		return c == 200 && s.Lease.Held && s.Lease.Epoch > before.Lease.Epoch
	})
	after, _ := getService(t, h, "echo")
	if after.StartSeq != before.StartSeq+1 || after.Starts != before.Starts+1 || after.Exits != 1 || after.RequestedExits != 1 || after.Boot != before.Boot {
		t.Errorf("before %+v after %+v", before, after)
	}
}

func TestControlStatusReportsRunningExecutable(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "warren")
	if err := os.Symlink(exe, alias); err != nil {
		t.Fatal(err)
	}
	h, err := start(t, "", "", func(o *Options) {
		o.Executable = func() (string, error) { return alias, nil }
		o.restartDrain = true
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	code, b := ctlDo(t, h, "GET", "/status")
	var st struct {
		Warren struct {
			Features []string
			Exe      string
			Dev      *uint64 `json:"exe_dev"`
			Ino      *uint64 `json:"exe_ino"`
		}
	}
	if err := json.Unmarshal(b, &st); code != http.StatusOK || err != nil {
		t.Fatalf("status: %d %s: %v", code, b, err)
	}
	if st.Warren.Exe != resolved || !slices.Contains(st.Warren.Features, "drain-exit-code") {
		t.Errorf("running executable or feature missing: %s", b)
	}
	if dev, ino, ok := platform.ExecutableIdentity(); ok {
		if st.Warren.Dev == nil || st.Warren.Ino == nil || *st.Warren.Dev != dev || *st.Warren.Ino != ino {
			t.Errorf("running executable identity missing or wrong: %s", b)
		}
	} else if st.Warren.Dev != nil || st.Warren.Ino != nil {
		t.Errorf("unavailable executable identity was reported: %s", b)
	}
	if slices.Contains(h.d.Node().Status()["warren"].(map[string]any)["features"].([]string), "drain-exit-code") {
		t.Error("the control feature leaked into the service status")
	}
}

func TestStatusReportsPinned(t *testing.T) {
	h, err := start(t, "", "pinned: docker image x:y\n")
	if err != nil {
		t.Fatal(err)
	}
	if st := h.d.Status(); st["pinned"] != "docker image x:y" {
		t.Errorf("pinned: %v", st["pinned"])
	}
}

func TestStatusOmitsPinnedWhenUnset(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := h.d.Status()["pinned"]; ok {
		t.Error("pinned reported when unset")
	}
}

// ---- updatable ----

// fakeEnv is an updatable environment with chosen answers: every path has
// device 1 unless devs says otherwise, and the container files /c1 and /c2 exist
// only when devs names them.
func fakeEnv(denied map[string]bool, devs map[string]uint64) *platform.UpdatableEnv {
	return &platform.UpdatableEnv{
		ContainerFiles: []string{"/c1", "/c2"},
		Root:           "/",
		Access: func(path string, mode uint32) error {
			if denied[path] {
				return syscall.EACCES
			}
			return nil
		},
		Dev: func(path string) (uint64, error) {
			if d, ok := devs[path]; ok {
				return d, nil
			}
			if path == "/c1" || path == "/c2" {
				return 0, syscall.ENOENT
			}
			return 1, nil
		},
	}
}

func TestUpdatableReportsServiceAndWarren(t *testing.T) {
	exe, _ := os.Executable()
	dir := filepath.Dir(exe)
	for _, c := range []struct {
		name       string
		env        *platform.UpdatableEnv
		want       *bool
		wantReason string
	}{
		{"updatable", fakeEnv(nil, nil), ptr(true), ""},
		{"read-only file", fakeEnv(map[string]bool{exe: true}, nil), ptr(false), "read-only"},
		{"read-only directory", fakeEnv(map[string]bool{dir: true}, nil), ptr(false), "read-only"},
		{"container layer", fakeEnv(nil, map[string]uint64{"/c2": 1, dir: 7, exe: 7, "/": 7}), ptr(false), "container"},
		{"container with a volume", fakeEnv(nil, map[string]uint64{"/c1": 1, dir: 8, exe: 8, "/": 7}), ptr(true), ""},
		{"a bind-mounted file", fakeEnv(nil, map[string]uint64{dir: 8, exe: 9, "/": 7}), ptr(false), "mount-point"},
		{"read-only wins over container", fakeEnv(map[string]bool{exe: true}, map[string]uint64{"/c1": 1, dir: 7, exe: 7, "/": 7}), ptr(false), "read-only"},
		{"unknown on this system", &platform.UpdatableEnv{}, nil, ""},
	} {
		h, err := start(t, "", "", func(o *Options) { o.Updatable = c.env })
		if err != nil {
			t.Fatal(err)
		}
		eventually(t, c.name+": the service is listed", func() bool { _, code := getService(t, h, "echo"); return code == 200 })
		s, _ := getService(t, h, "echo")
		if (s.Updatable == nil) != (c.want == nil) || (c.want != nil && *s.Updatable != *c.want) || s.NotUpdatable != c.wantReason {
			t.Errorf("%s: service updatable %v reason %q", c.name, s.Updatable, s.NotUpdatable)
		}
		w := h.d.Status()["warren"].(map[string]any)
		got, has := w["updatable"]
		if has != (c.want != nil) || (has && got != *c.want) || w["not_updatable"] != nilIfEmpty(c.wantReason) {
			t.Errorf("%s: warren %v", c.name, w)
		}
		// The processes of /status carry the same, for `warren status`.
		for _, p := range h.d.Status()["processes"].([]supervise.Status) {
			if p.Name == "service:echo" && ((p.Updatable == nil) != (c.want == nil) || p.NotUpdatable != c.wantReason) {
				t.Errorf("%s: process entry updatable %v reason %q", c.name, p.Updatable, p.NotUpdatable)
			}
		}
		h.d.Stop()
	}
}

func ptr(b bool) *bool { return &b }

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func TestControlRoutesAndStatusCodes(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		method, uri string
		code        int
	}{
		{"GET", "/nope", 404},
		{"GET", "/nope?x=1", 404}, // not a route: 404, not 400
		{"GET", "/services/echo/restart", 405},
		{"GET", "/status?x=1", 400},
		{"POST", "/services/nobody/restart", 404},
		{"DELETE", "/status", 405},
	} {
		if code, b := ctlDo(t, h, c.method, c.uri); code != c.code {
			t.Errorf("%s %s: %d %s, want %d", c.method, c.uri, code, b, c.code)
		}
	}
}

// A restart that arrives while Warren stops is refused, not accepted: nothing
// would serve it.
func TestRestartDuringStopIsRefusedWith503(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "echo runs", func() bool { s, c := getService(t, h, "echo"); return c == 200 && s.State == "running" })
	h.d.Stop()
	rec := httptest.NewRecorder()
	h.d.controlHandler().ServeHTTP(rec, httptest.NewRequest("POST", "/services/echo/restart", nil))
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "shutting down") {
		t.Errorf("restart during stop: %d %s", rec.Code, rec.Body.String())
	}
}

// The status omits what is not known: no zero timestamps.
func TestServiceObjectOmitsUnsetTimes(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "echo holds a lease", func() bool { s, c := getService(t, h, "echo"); return c == 200 && s.Lease.Held })
	_, b := ctlDo(t, h, "GET", "/services/echo")
	if strings.Contains(string(b), "0001-01-01") {
		t.Errorf("a zero time in %s", b)
	}
	// A service that has not started has no started_at and no lease since.
	slow := "    - name: late\n      allow: [\"*\"]\n      run: [/nonexistent/binary]\n"
	os.WriteFile(h.path, []byte(strings.Replace(h.text, "attach:", slow+"attach:", 1)), 0o600)
	if err := h.d.Reload(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "late is listed", func() bool { _, c := getService(t, h, "late"); return c == 200 })
	_, b = ctlDo(t, h, "GET", "/services/late")
	if strings.Contains(string(b), "0001-01-01") || strings.Contains(string(b), "started_at") || strings.Contains(string(b), `"since"`) {
		t.Errorf("unset times shown in %s", b)
	}
}

// A transport that breaks its link at every start must back off, as a crash
// does: a link break stops the process but is not a requested restart, which
// would skip and reset the backoff.
func TestCrashLoopingTransportStillBacksOff(t *testing.T) {
	exe, _ := os.Executable()
	tr := "transport: {run: [" + exe + ", --role, transport-flaky, n1]}"
	// start() runs fast() first; a longer first delay, set before the daemon
	// starts, makes the difference plain.
	h, err := start(t, tr, "", func(*Options) { supervise.BackoffStart = 500 * time.Millisecond })
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	var seq, requested int
	for _, p := range h.d.sup.Status() {
		if p.Name == "transport" {
			seq, requested = p.StartSeq, p.RequestedExits
		}
	}
	if seq > 4 {
		t.Errorf("%d transport starts in 2.5 s: the backoff was skipped", seq)
	}
	if requested != 0 {
		t.Errorf("%d link breaks counted as requested exits", requested)
	}
}

func TestRestartIsRefusedOnceDrainBegan(t *testing.T) {
	h, err := start(t, "", "")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "echo runs", func() bool { s, c := getService(t, h, "echo"); return c == 200 && s.State == "running" })
	if code, _ := ctlDo(t, h, "POST", "/drain-and-exit"); code != 204 {
		t.Fatalf("drain %d", code)
	}
	if code, b := ctlDo(t, h, "POST", "/services/echo/restart"); code != 503 {
		t.Errorf("restart during drain: %d %s", code, b)
	}
}

func TestStatusProcessesOmitAZeroStartedAt(t *testing.T) {
	slow := "    - name: late\n      allow: [\"*\"]\n      run: [/nonexistent/binary]\n"
	h, err := start(t, "", slow)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "late is a process", func() bool {
		for _, p := range h.d.Status()["processes"].([]supervise.Status) {
			if p.Name == "service:late" {
				return true
			}
		}
		return false
	})
	b, _ := json.Marshal(h.d.Status()["processes"])
	for _, p := range h.d.Status()["processes"].([]supervise.Status) {
		if p.Name == "service:late" && p.Starts == 0 {
			if strings.Contains(string(b), `"started_at":"0001`) {
				t.Errorf("a zero started_at in %s", b)
			}
		}
	}
}
