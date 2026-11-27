// Package daemon is `warren run`: it loads the config, starts the managed
// transport and services, serves the node, answers the control socket
// (status and reload), and shuts everything down in order.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/PeterSR/warren-mesh/internal/config"
	"github.com/PeterSR/warren-mesh/internal/fsutil"
	"github.com/PeterSR/warren-mesh/internal/link"
	"github.com/PeterSR/warren-mesh/internal/platform"
	"github.com/PeterSR/warren-mesh/internal/service"
	"github.com/PeterSR/warren-mesh/internal/socketdir"
	"github.com/PeterSR/warren-mesh/internal/supervise"
)

// HelloTimeout is how long startup waits for the transport to answer hello.
var HelloTimeout = 3 * time.Second

// Options start a daemon.
type Options struct {
	// Load reads the configuration; it is called at start and at every
	// reload. Defaults to config.Load(ConfigPath).
	Load       func() (*config.Config, error)
	ConfigPath string
	Log        *slog.Logger
	// Step, if set, is told each shutdown step as it begins (for tests).
	Step func(string)
	// Link replaces the transport link (tests use the in-memory fake).
	Link link.Link
	// Updatable is what the updatable check looks at (container files, the
	// root path, the access check); it defaults to the real system.
	Updatable *platform.UpdatableEnv
	// Executable gives the path of Warren's own binary; it defaults to
	// os.Executable. Tests replace it.
	Executable func() (string, error)
	// Context, if set, ends startup early: when it is done, Start stops what
	// it started and returns the context's error.
	Context context.Context
}

// Daemon is a running Warren.
type Daemon struct {
	o    Options
	log  *slog.Logger
	node *service.Node
	sup  *supervise.Supervisor
	lnk  link.Link
	ctl  *socketdir.Group

	// reloadMu serializes reloads (SIGHUP and the control socket). mu guards
	// what Status and the service endpoints read, managed included (written
	// only by syncServices, under both), and is never held while a process is
	// stopped.
	reloadMu  sync.Mutex
	mu        sync.Mutex
	cfg       *config.Config
	transport string
	managed   map[string]config.Service // services we run, as started
	exit      chan struct{}             // closed by drain-and-exit
	exitOnce  sync.Once
	lockFile  *os.File
	stopCh    chan struct{}
	stopOnce  sync.Once
}

// ControlPath is the control socket for a config.
func ControlPath(cfg *config.Config) string { return cfg.Control }

// Start brings Warren up. It fails, loudly, if the transport's hello does
// not match this node.
func Start(o Options) (*Daemon, error) {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Context == nil {
		o.Context = context.Background()
	}
	if o.Load == nil {
		path := o.ConfigPath
		o.Load = func() (*config.Config, error) { return config.Load(path) }
	}
	cfg, err := o.Load()
	if err != nil {
		return nil, err
	}
	d := &Daemon{o: o, log: o.Log, cfg: cfg, sup: supervise.New(o.Log), managed: map[string]config.Service{}, exit: make(chan struct{}), stopCh: make(chan struct{})}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return nil, fmt.Errorf("state_dir: %w", err)
	}
	// Before anything is kept in or read from it.
	if owner, err := service.CheckStateDir(cfg.StateDir); err != nil {
		return nil, err
	} else if owner >= 0 {
		d.log.Warn("state_dir is not owned by Warren's user", "dir", cfg.StateDir, "owner", owner)
	}
	if err := d.lockState(cfg); err != nil {
		return nil, err
	}
	if need := service.StreamNeed(cfg); need > link.MinStreams {
		d.unlockState()
		return nil, fmt.Errorf("the config can use up to %d streams on a transport link, which allows %d; lower inflight and outflight, or max_children", need, link.MinStreams)
	}
	if err := d.startTransport(cfg); err != nil {
		d.abort()
		return nil, err
	}
	if err := o.Context.Err(); err != nil {
		d.abort()
		return nil, err
	}
	d.node, err = service.New(service.Options{Config: cfg, Link: d.lnk, Log: o.Log, TransportName: d.transport})
	if err != nil {
		d.abort()
		return nil, err
	}
	if err := d.node.Start(); err != nil {
		d.abort()
		return nil, err
	}
	if err := o.Context.Err(); err != nil {
		d.abort()
		return nil, err
	}
	if cfg.Pinned != "" {
		d.log.Info("this node is pinned: its binaries are replaced from outside Warren", "reason", cfg.Pinned)
	}
	d.syncServices(cfg)
	d.ctl = socketdir.NewGroup(o.Log)
	ctlWants := []socketdir.Want{{Key: "control", Path: ControlPath(cfg), Mode: 0o600, Handler: d.controlHandler(),
		// Only Warren's own user, or root, may use the control socket.
		Admit: func(uid int, known bool) bool { return known && (uid == os.Geteuid() || uid == 0) }}}
	if f := d.ctl.Sync(ctlWants); f["control"] != nil {
		d.abort()
		return nil, fmt.Errorf("control socket: %w", f["control"])
	}
	// The control socket is made again if its file disappears.
	every := service.ScanEvery
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-d.stopCh:
				return
			case <-t.C:
				d.ctl.Sync(ctlWants)
			}
		}
	}()
	return d, nil
}

// Node returns the node, for tests.
func (d *Daemon) Node() *service.Node { return d.node }

// lockState takes the exclusive lock that keeps two Warrens off one config:
// they would fight over every socket. It is held until the process ends or
// the daemon stops.
func (d *Daemon) lockState(cfg *config.Config) error {
	p := filepath.Join(cfg.StateDir, "warren.lock")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("state_dir lock: %w", err)
	}
	if err := fsutil.TryLock(f); err != nil {
		f.Close()
		if errors.Is(err, fsutil.ErrLocked) {
			return fmt.Errorf("another Warren already runs on this state_dir (%s is locked); two Warrens on one config would fight over every socket", p)
		}
		return fmt.Errorf("state_dir lock: %w", err)
	}
	d.lockFile = f
	return nil
}

func (d *Daemon) unlockState() {
	if d.lockFile != nil {
		d.lockFile.Close() // closing releases the lock
		d.lockFile = nil
	}
}

func (d *Daemon) abort() {
	defer d.unlockState()
	d.stopOnce.Do(func() { close(d.stopCh) })
	d.sup.StopAll()
	if d.node != nil {
		d.node.Close()
	}
	if d.lnk != nil {
		d.lnk.Close()
	}
}

func (d *Daemon) step(name string) {
	d.log.Info("shutdown", "step", name)
	if d.o.Step != nil {
		d.o.Step(name)
	}
}

// Stop shuts down in order: stop handing out calls and wait for answers;
// stop the managed services; stop the transport; end leases and close the
// sockets; close the link.
func (d *Daemon) Stop() {
	defer d.unlockState()
	d.stopOnce.Do(func() { close(d.stopCh) })
	// No reload runs during shutdown: one in progress finishes first, and
	// any later one sees stopCh closed.
	d.reloadMu.Lock()
	//lint:ignore SA2001 a barrier: waits for a running reload; later ones see stopCh closed
	d.reloadMu.Unlock() //nolint:staticcheck // see the lint:ignore above
	d.step("drain")
	d.node.Drain()
	d.step("stop-services")
	d.sup.StopGroup(supervise.GroupService)
	d.step("stop-transport")
	d.sup.StopGroup(supervise.GroupTransport)
	d.step("close-node")
	d.ctl.Close() // no more control requests while the node goes down
	d.node.Close()
	if d.lnk != nil {
		d.lnk.Close()
	}
	d.sup.StopAll()
}

// Run starts Warren, reloads on SIGHUP, and stops on ctx end or SIGTERM/SIGINT.
//
// The signals are taken before Start, which can take seconds (it waits for
// the transport): a SIGHUP then waits until Start has returned, and a SIGTERM
// ends the startup and shuts down cleanly instead of killing the process.
func Run(ctx context.Context, o Options) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	// Once the first stop signal has come, the default handling is back, so a
	// second one ends the process at once if the clean shutdown hangs.
	go func() { <-ctx.Done(); stop() }()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	o.Context = ctx
	d, err := Start(o)
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil // asked to stop while starting: stopped cleanly
		}
		return err
	}
	for {
		select {
		case <-hup:
			if err := d.Reload(); err != nil {
				d.log.Error("reload refused", "err", err)
			}
		case <-d.exit:
			d.Stop()
			return nil
		case <-ctx.Done():
			d.Stop()
			return nil
		}
	}
}

// Reload reads the config again and applies it. Secrets that are files are
// read again too; an environment variable that changed needs a restart.
func (d *Daemon) Reload() error {
	d.reloadMu.Lock()
	defer d.reloadMu.Unlock()
	select {
	case <-d.stopCh:
		return errors.New("warren is shutting down")
	default:
	}
	next, err := d.o.Load()
	if err != nil {
		return err
	}
	if need := service.StreamNeed(next); need > link.MinStreams {
		return fmt.Errorf("the config can use up to %d streams on a transport link, which allows %d; lower inflight and outflight, or max_children", need, link.MinStreams)
	}
	if err := d.node.Reload(next); err != nil {
		return err
	}
	d.mu.Lock()
	d.cfg = next
	d.mu.Unlock()
	d.syncServices(next)
	d.log.Info("reloaded")
	return nil
}

// ---- transport ----

func (d *Daemon) startTransport(cfg *config.Config) error {
	t := cfg.Transport
	if d.o.Link != nil {
		d.lnk = d.o.Link
		d.transport = "test"
		return d.checkHello(cfg)
	}
	if !t.Set() {
		return nil
	}
	switch {
	case t.External():
		l, err := link.NewExternal(cfg.Node, t.LinkA, t.LinkB, *t.UID, d.log)
		if err != nil {
			return err
		}
		d.lnk, d.transport = l, "external"
	default:
		argv := t.Run
		d.transport = "custom"
		if t.Name != "" {
			exe, err := os.Executable()
			if err != nil {
				return err
			}
			argv = []string{filepath.Join(filepath.Dir(exe), "warren-"+t.Name)}
			d.transport = t.Name
		}
		m := link.NewManaged(cfg.Node)
		// A link that breaks while the process lives is only mended by new
		// socket pairs, which a restart makes.
		m.OnBroken(func() {
			select {
			case <-d.stopCh:
				return // shutting down: the link is meant to end
			default:
			}
			d.log.Warn("the transport link broke; restarting the transport")
			// Not a requested restart: a transport that breaks its link at every
			// start keeps the doubling backoff.
			d.sup.Bounce("transport")
		})
		d.lnk = m
		env := supervise.MinimalEnv(os.LookupEnv, nil, nil, d.transportSettings(cfg))
		err := d.sup.Add(supervise.Spec{
			Name: "transport", Group: supervise.GroupTransport, Argv: argv, Env: env,
			Prepare: m.Prepare, OnExit: func(bool, error) { m.Down() },
		})
		if err != nil {
			return err
		}
	}
	return d.checkHello(cfg)
}

// transportSettings hands a shipped transport its block of the config.
func (d *Daemon) transportSettings(cfg *config.Config) map[string]string {
	var block any
	switch {
	case cfg.Tailscale != nil:
		block = cfg.Tailscale
	default:
		return nil
	}
	b, _ := json.Marshal(block)
	return map[string]string{"WARREN_TRANSPORT_SETTINGS": string(b)}
}

// checkHello waits for the transport to answer and refuses a mismatch.
func (d *Daemon) checkHello(cfg *config.Config) error {
	deadline := time.Now().Add(HelloTimeout)
	var last error
	for {
		ctx, cancel := context.WithTimeout(d.o.Context, 3*time.Second)
		h, err := d.lnk.Hello(ctx)
		cancel()
		if cerr := d.o.Context.Err(); cerr != nil {
			return cerr
		}
		if err == nil {
			if err := link.CheckHello(h, cfg.Node); err != nil {
				return fmt.Errorf("transport refused: %w", err)
			}
			if h.Name != "" {
				d.transport = h.Name
			}
			return nil
		}
		// A transport that answered, and not with a hello (an error status, a
		// body that does not parse), is refused. Only failures to reach it, and
		// timeouts, are an unanswered hello.
		if link.IsAnswerError(err) {
			return fmt.Errorf("transport refused: %w", err)
		}
		last = err
		if time.Now().After(deadline) {
			// An unanswered hello (tailscaled down or logged out) is not a
			// refusal: Warren starts with the link down, its local services
			// work, and the node keeps asking until the transport answers.
			d.log.Warn("the transport did not answer hello; starting with the link down", "wait", HelloTimeout, "err", last)
			return nil
		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-d.o.Context.Done():
			return d.o.Context.Err()
		}
	}
}

// ---- managed services ----

// syncServices starts, restarts and stops the node's managed services to
// match cfg. The caller holds reloadMu (or is Start, before anything else
// can reload); removing a process can take seconds and must not block Status.
func (d *Daemon) syncServices(cfg *config.Config) {
	want := map[string]config.Service{}
	for _, s := range cfg.Services.List {
		if len(s.Run) > 0 {
			want[s.Name] = s
		}
	}
	// Each removal can take a process's whole stop time (SIGTERM, then
	// SIGKILL), so the changed or removed services are stopped in parallel.
	var wg sync.WaitGroup
	for name, old := range d.managed {
		if w, ok := want[name]; !ok || !sameRun(old, w) {
			wg.Add(1)
			go func() { defer wg.Done(); d.sup.Remove("service:" + name) }()
			d.mu.Lock()
			delete(d.managed, name)
			d.mu.Unlock()
		}
	}
	wg.Wait()
	for name, s := range want {
		if _, running := d.managed[name]; running { // only syncServices writes it
			continue
		}
		name, s := name, s
		sock := cfg.ServicePath(name)
		env := supervise.MinimalEnv(os.LookupEnv, s.EnvPass, s.Env, map[string]string{"WARREN_SERVICE_SOCKET": sock})
		err := d.sup.Add(supervise.Spec{
			Name: "service:" + name, Group: supervise.GroupService, Argv: s.Run, Env: env,
			// The process is gone, so its lease ends at once.
			OnExit: func(bool, error) { d.node.ProcessExited(cfg.Node, name) },
		})
		if err != nil {
			d.log.Error("cannot manage a service", "service", name, "err", err)
			continue
		}
		d.mu.Lock()
		d.managed[name] = s
		d.mu.Unlock()
	}
}

func sameRun(a, b config.Service) bool {
	return reflect.DeepEqual(a.Run, b.Run) && reflect.DeepEqual(a.Env, b.Env) && reflect.DeepEqual(a.EnvPass, b.EnvPass)
}

// ---- control socket ----

// Status is what `warren status` shows.
func (d *Daemon) Status() map[string]any {
	st := d.node.Status()
	d.mu.Lock()
	cfg := d.cfg
	d.mu.Unlock()
	procs := d.sup.Status()
	for i := range procs {
		procs[i].Updatable, procs[i].NotUpdatable = d.updatable(procs[i].Exe)
	}
	st["processes"] = procs
	if cfg.Pinned != "" {
		st["pinned"] = cfg.Pinned
	}
	if w, ok := st["warren"].(map[string]any); ok {
		if exe, err := d.executable(); err == nil {
			if updatable, reason, known := d.updatableEnv().Check(exe); known {
				w["updatable"] = updatable
				if reason != "" {
					w["not_updatable"] = reason
				}
			}
		}
	}
	st["memory_bound"] = service.MemoryBound(cfg)
	st["memory_bound_peers"] = service.AssumedPeers
	st["config"] = d.o.ConfigPath
	// Healthy: the config is loaded (it is, or Warren would not run) and the
	// transport link is up (trivially so without a transport).
	st["healthy"] = st["link_up"] == true
	return st
}

// serviceObject is the control API's view of one managed service (docs/control-api.md).
type serviceObject struct {
	Name     string `json:"name"`
	Exe      string `json:"exe"`
	ExeError string `json:"exe_error,omitempty"`
	// Updatable and NotUpdatable are left out where the system cannot say.
	Updatable         *bool      `json:"updatable,omitempty"`
	NotUpdatable      string     `json:"not_updatable,omitempty"`
	State             string     `json:"state"`
	PID               int        `json:"pid,omitempty"`
	StartSeq          int        `json:"start_seq"`
	StartedAt         *time.Time `json:"started_at,omitempty"`
	Starts            int        `json:"starts"`
	Exits             int        `json:"exits"`
	RequestedExits    int        `json:"requested_exits"`
	StartFailures     int        `json:"start_failures"`
	LastExit          string     `json:"last_exit"`
	LastExitRequested bool       `json:"last_exit_requested"`
	LastStartError    string     `json:"last_start_error"`
	Lease             leaseInfo  `json:"lease"`
	Boot              string     `json:"boot"`
}

type leaseInfo struct {
	Held  bool       `json:"held"`
	Since *time.Time `json:"since,omitempty"`
	Epoch int        `json:"epoch"`
}

// managedNames is the sorted names of the managed services, a snapshot taken
// under mu (never reloadMu, so a slow reload cannot stall a health poll). The
// objects are then read one by one, so a list can show a reload half way:
// a service of the old config next to one of the new.
func (d *Daemon) managedNames() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	names := make([]string, 0, len(d.managed))
	for n := range d.managed {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (d *Daemon) isManaged(name string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.managed[name]
	return ok
}

// service describes one managed service; false when it is not managed now.
func (d *Daemon) service(name string) (serviceObject, bool) {
	if !d.isManaged(name) {
		return serviceObject{}, false
	}
	p, ok := d.sup.Process("service:" + name)
	if !ok {
		return serviceObject{}, false
	}
	held, since, epoch := d.node.LeaseInfo(name)
	updatable, notUpdatable := d.updatable(p.Exe)
	var startedAt, leaseSince *time.Time
	if !p.StartedAt.IsZero() {
		startedAt = &p.StartedAt
	}
	if !since.IsZero() {
		leaseSince = &since
	}
	return serviceObject{
		Name: name, Exe: p.Exe, ExeError: p.ExeError, Updatable: updatable, NotUpdatable: notUpdatable, State: p.State, PID: p.PID,
		StartSeq: p.StartSeq, StartedAt: startedAt, Starts: p.Starts, Exits: p.Exits,
		RequestedExits: p.RequestedExits, StartFailures: p.StartFailures,
		LastExit: p.LastExit, LastExitRequested: p.LastExitRequested, LastStartError: p.LastStartError,
		Lease: leaseInfo{Held: held, Since: leaseSince, Epoch: epoch}, Boot: d.node.Boot(),
	}, true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// noQuery answers 400 to any query parameter on a route that exists: no
// control endpoint has one yet, so a caller that asks for something an older
// Warren does not know is refused instead of silently ignored. A path that is
// no route at all is left to the mux, which answers 404 (or 405 for another
// method).
func noQuery(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" {
			if _, pattern := mux.Handler(r); pattern != "" {
				name := r.URL.RawQuery
				if q, err := url.ParseQuery(r.URL.RawQuery); err == nil {
					for k := range q {
						name = k
						break
					}
				}
				http.Error(w, "unknown query parameter "+strconv.Quote(name), http.StatusBadRequest)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func (d *Daemon) controlHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, d.Status())
	})
	mux.HandleFunc("GET /services", func(w http.ResponseWriter, r *http.Request) {
		out := []serviceObject{}
		for _, name := range d.managedNames() {
			if s, ok := d.service(name); ok {
				out = append(out, s)
			}
		}
		writeJSON(w, out)
	})
	mux.HandleFunc("GET /services/{name}", func(w http.ResponseWriter, r *http.Request) {
		s, ok := d.service(r.PathValue("name"))
		if !ok {
			http.Error(w, "no such managed service", http.StatusNotFound)
			return
		}
		writeJSON(w, s)
	})
	mux.HandleFunc("POST /services/{name}/restart", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		for _, ch := range []chan struct{}{d.stopCh, d.exit} { // stopping, or draining to exit
			select {
			case <-ch:
				http.Error(w, "warren is shutting down", http.StatusServiceUnavailable)
				return
			default:
			}
		}
		if !d.isManaged(name) {
			http.Error(w, "no such managed service", http.StatusNotFound)
			return
		}
		minSeq, ok := d.sup.Restart("service:" + name)
		if !ok {
			select {
			case <-d.stopCh:
				http.Error(w, "warren is shutting down", http.StatusServiceUnavailable)
			default:
				http.Error(w, "no such managed service", http.StatusNotFound)
			}
			return
		}
		writeJSON(w, map[string]any{"accepted": true, "min_start_seq": minSeq})
	})
	mux.HandleFunc("POST /drain-and-exit", func(w http.ResponseWriter, r *http.Request) {
		d.exitOnce.Do(func() { close(d.exit) })
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /reload", func(w http.ResponseWriter, r *http.Request) {
		if err := d.Reload(); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return noQuery(mux)
}

// ---- control client (warren status, warren reload) ----

// Control calls the control socket at path.
func Control(path, method, uri string) ([]byte, error) {
	c := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{DialContext: unixDialer(path), DisableKeepAlives: true}}
	req, err := http.NewRequest(method, "http://warren"+uri, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot reach Warren at %s: %w", path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return nil, errors.New(string(b))
	}
	return b, nil
}

// updatable is the updatable check of a binary: nil, "" where it cannot be told.
func (d *Daemon) updatable(exe string) (*bool, string) {
	if exe == "" {
		return nil, ""
	}
	if u, reason, known := d.updatableEnv().Check(exe); known {
		return &u, reason
	}
	return nil, ""
}

func (d *Daemon) updatableEnv() platform.UpdatableEnv {
	if d.o.Updatable != nil {
		return *d.o.Updatable
	}
	return platform.DefaultUpdatableEnv()
}

func (d *Daemon) executable() (string, error) {
	if d.o.Executable != nil {
		return d.o.Executable()
	}
	return os.Executable()
}
