package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"aead.dev/minisign"

	"github.com/PeterSR/warren-mesh/internal/update/manifest"
	"github.com/PeterSR/warren-mesh/internal/update/state"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
)

// ---- a fake Warren: the control API of docs/control-api.md on a unix socket ----

type fakeWarren struct {
	mu       sync.Mutex
	path     string
	ln       net.Listener
	svcs     map[string]*Service
	managed  map[string]bool
	boot     string
	pinned   string
	version  string
	restarts int
	// onRestart plays what Warren does when a restart is accepted.
	onRestart func(s *Service)
	// refuseRestart answers 404 to a restart while true; refuseFor does so for
	// one service.
	refuseRestart bool
	refuseFor     map[string]bool
}

func newFakeWarren(t *testing.T, dir string) *fakeWarren {
	t.Helper()
	f := &fakeWarren{path: filepath.Join(dir, "control.sock"), svcs: map[string]*Service{}, managed: map[string]bool{}, boot: "boot-1", version: "1.0.0"}
	f.onRestart = restartWorks
	ln, err := net.Listen("unix", f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.ln = ln
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		st := map[string]any{"boot": f.boot, "warren": map[string]any{"version": f.version}, "healthy": true}
		if f.pinned != "" {
			st["pinned"] = f.pinned
		}
		json.NewEncoder(w).Encode(st)
	})
	mux.HandleFunc("GET /services", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		out := []Service{}
		for n, s := range f.svcs {
			if f.managed[n] {
				out = append(out, f.snapshot(s))
			}
		}
		json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("GET /services/{name}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		s := f.svcs[r.PathValue("name")]
		if s == nil || !f.managed[s.Name] {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(f.snapshot(s))
	})
	mux.HandleFunc("POST /services/{name}/restart", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		s := f.svcs[r.PathValue("name")]
		if s == nil || !f.managed[s.Name] || f.refuseRestart || f.refuseFor[s.Name] {
			http.NotFound(w, r)
			return
		}
		f.restarts++
		min := s.StartSeq + 1
		f.onRestart(s)
		json.NewEncoder(w).Encode(map[string]any{"accepted": true, "min_start_seq": min})
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return f
}

func (f *fakeWarren) snapshot(s *Service) Service {
	c := *s
	c.Boot = f.boot
	return c
}

// add a managed service running exe, with a lease held, started once.
func (f *fakeWarren) add(name, exe string) *Service {
	f.mu.Lock()
	defer f.mu.Unlock()
	yes := true
	s := &Service{Name: name, Exe: exe, Updatable: &yes, State: "running", PID: 100, StartSeq: 1, Starts: 1, Lease: Lease{Held: true, Epoch: 1, Since: time.Now()}}
	f.svcs[name] = s
	f.managed[name] = true
	return s
}

func (f *fakeWarren) with(name string, fn func(s *Service)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f.svcs[name])
}

func (f *fakeWarren) setManaged(name string, v bool) {
	f.mu.Lock()
	f.managed[name] = v
	f.mu.Unlock()
}

func (f *fakeWarren) restartCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.restarts
}

// restartWorks is a service that stops on request, starts again at once and
// takes its lease.
func restartWorks(s *Service) {
	s.StartSeq++
	s.Starts++
	s.Exits++
	s.RequestedExits++
	s.LastExitRequested = true
	s.State = "running"
	s.Lease = Lease{Held: true, Epoch: s.Lease.Epoch + 1, Since: time.Now()}
}

// restartHangs starts but never takes a lease.
func restartHangs(s *Service) {
	restartWorks(s)
	s.Lease.Held = false
}

// crash is one unrequested failure: the process exits and is started again.
func crash(s *Service) {
	s.StartSeq++
	s.Starts++
	s.Exits++
	s.LastExit = "exit status 1"
	s.Lease = Lease{Held: true, Epoch: s.Lease.Epoch + 1, Since: time.Now()}
}

// ---- a rig: a component, its target, keys and an engine ----

type kp struct {
	priv minisign.PrivateKey
	text string
}

func newKey(t *testing.T) kp {
	t.Helper()
	pub, priv, err := minisign.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return kp{priv, pub.String()}
}

type rig struct {
	t      *testing.T
	dir    string
	bin    string
	target string
	state  string
	key    kp
	fw     *fakeWarren
	cfg    *uconfig.Config
	comp   *uconfig.Comp
	e      *Engine
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type rigOpt func(*rig)

func newRig(t *testing.T, opts ...rigOpt) *rig {
	t.Helper()
	d, err := os.MkdirTemp("", "eng")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	r := &rig{t: t, dir: d, bin: filepath.Join(d, "bin"), state: filepath.Join(d, "state"), key: newKey(t)}
	os.MkdirAll(r.bin, 0o755)
	os.MkdirAll(r.state, 0o700)
	r.target = filepath.Join(r.bin, "myapp")
	os.WriteFile(r.target, []byte("old binary"), 0o755)
	r.fw = newFakeWarren(t, d)
	r.fw.add("myapp", r.target)
	k, err := manifest.ParseKey(r.key.text)
	if err != nil {
		t.Fatal(err)
	}
	r.comp = &uconfig.Comp{Name: "myapp", Channel: manifest.Dev, Service: "myapp", Keys: manifest.Keys{manifest.Dev: {k}},
		Targets: map[string]string{"myapp": r.target}, Settle: 120 * time.Millisecond, MaxSize: 1 << 20}
	r.cfg = &uconfig.Config{StateDir: r.state, Control: r.fw.path, HealthTimeout: 2 * time.Second, Canary: 200 * time.Millisecond,
		Components: map[string]*uconfig.Comp{"myapp": r.comp}}
	for _, o := range opts {
		o(r)
	}
	r.e = r.engine(nil)
	return r
}

// engine makes a new engine over the same config and state: a restarted updater.
func (r *rig) engine(hook func(string) error) *Engine {
	return New(Options{Config: r.cfg, Control: NewControl(r.fw.path), Log: quiet(), Version: "test", PollEvery: 10 * time.Millisecond, Hook: hook})
}

func (r *rig) store() state.Store { return state.Store{Dir: r.state} }

func (r *rig) record() *state.Record {
	r.t.Helper()
	rec, err := r.store().Load("myapp")
	if err != nil {
		r.t.Fatal(err)
	}
	return rec
}

func (r *rig) content() string {
	b, err := os.ReadFile(r.target)
	if err != nil {
		r.t.Fatal(err)
	}
	return string(b)
}

// publish signs a manifest for the component and returns it with the bytes of
// the one file of this platform.
func (r *rig) publish(version, content string, mods ...func(*manifest.Manifest)) (data, sig []byte, m *manifest.Manifest) {
	r.t.Helper()
	sum := sha256hex([]byte(content))
	f := manifest.File{Name: "myapp_" + version, Size: int64(len(content)), SHA256: sum}
	m = &manifest.Manifest{Format: manifest.Format, Component: "myapp", Version: version, Channel: manifest.Dev, Created: "2026-11-26T10:00:00Z",
		Platforms: map[string]manifest.Platform{Platform: {Files: map[string]manifest.File{"myapp": f}}}}
	for _, mod := range mods {
		mod(m)
	}
	data, err := m.Marshal()
	if err != nil {
		r.t.Fatal(err)
	}
	return data, manifest.Sign(r.key.priv, data, m), m
}

// stage publishes and stages a version through the engine's own Stage and ReceiveFile.
func (r *rig) stage(version, content string) {
	r.t.Helper()
	data, sig, _ := r.publish(version, content)
	res, err := r.e.Stage("myapp", data, sig)
	if err != nil {
		r.t.Fatal(err)
	}
	for _, n := range res.Need {
		if err := r.e.ReceiveFile("myapp", version, n.SHA256, strings.NewReader(content)); err != nil {
			r.t.Fatal(err)
		}
	}
}

func (r *rig) install(version, content string, mods ...func(*InstallRequest)) (*InstallResult, error) {
	r.t.Helper()
	r.stage(version, content)
	req := InstallRequest{Component: "myapp", Version: version}
	for _, m := range mods {
		if m != nil {
			m(&req)
		}
	}
	return r.e.Install(context.Background(), req)
}

// tick runs the watcher until cond holds on the record, or fails.
func (r *rig) until(what string, cond func(*state.Record) bool) {
	r.t.Helper()
	r.untilWith(r.e, what, cond)
}

func (r *rig) untilWith(e *Engine, what string, cond func(*state.Record) bool) {
	r.t.Helper()
	dl := time.Now().Add(10 * time.Second)
	for {
		e.Tick(context.Background())
		if rec, err := r.store().Load("myapp"); err == nil && cond(rec) {
			return
		}
		if time.Now().After(dl) {
			rec, _ := r.store().Load("myapp")
			j, _ := json.Marshal(rec)
			r.t.Fatalf("timed out waiting for %s; record %s", what, j)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func phaseIs(p string) func(*state.Record) bool {
	return func(r *state.Record) bool { return r.Phase == p }
}

var _ = bytes.NewReader
