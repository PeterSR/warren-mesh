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
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/PeterSR/warren-mesh/internal/config"
	"github.com/PeterSR/warren-mesh/internal/link"
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

	// reloadMu serializes reloads (SIGHUP and the control socket); it also
	// guards managed. mu guards only what Status reads, and is never held
	// while a process is stopped.
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
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
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
			d.sup.Restart("transport")
		})
		d.lnk = m
		env := supervise.MinimalEnv(os.LookupEnv, nil, nil, d.transportSettings(cfg))
		err := d.sup.Add(supervise.Spec{
			Name: "transport", Group: supervise.GroupTransport, Argv: argv, Env: env,
			Prepare: m.Prepare, OnExit: m.Down,
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
			delete(d.managed, name)
		}
	}
	wg.Wait()
	for name, s := range want {
		if _, running := d.managed[name]; running {
			continue
		}
		name, s := name, s
		sock := cfg.ServicePath(name)
		env := supervise.MinimalEnv(os.LookupEnv, s.EnvPass, s.Env, map[string]string{"WARREN_SERVICE_SOCKET": sock})
		err := d.sup.Add(supervise.Spec{
			Name: "service:" + name, Group: supervise.GroupService, Argv: s.Run, Env: env,
			// The process is gone, so its lease ends at once.
			OnExit: func() { d.node.ProcessExited(cfg.Node, name) },
		})
		if err != nil {
			d.log.Error("cannot manage a service", "service", name, "err", err)
			continue
		}
		d.managed[name] = s
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
	st["processes"] = d.sup.Status()
	st["memory_bound"] = service.MemoryBound(cfg)
	st["memory_bound_peers"] = service.AssumedPeers
	st["config"] = d.o.ConfigPath
	// Healthy: the config is loaded (it is, or Warren would not run) and the
	// transport link is up (trivially so without a transport).
	st["healthy"] = st["link_up"] == true
	return st
}

func (d *Daemon) controlHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(d.Status())
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
	return mux
}

// ---- control client (warren status, warren reload) ----

// Control calls the control socket at path.
func Control(path, method, uri string) ([]byte, error) {
	c := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{DialContext: unixDialer(path), DisableKeepAlives: true}}
	req, _ := http.NewRequest(method, "http://warren"+uri, nil)
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
