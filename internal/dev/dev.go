// Package dev is `warren dev`: several Warren nodes in one process, joined by
// the in-memory transport, each serving real service sockets in a directory
// (a temporary one unless given). Service authors, in any language, test
// against the real /v1/service contract with it, and the service conformance
// suite uses it.
//
// Nodes are the same daemon `warren run` is (control socket, state lock and
// all), with the in-memory link in place of a transport. Services are
// always external here: dev starts no processes.
package dev

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PeterSR/warren-mesh/confload"
	"github.com/PeterSR/warren-mesh/internal/config"
	"github.com/PeterSR/warren-mesh/internal/daemon"
	"github.com/PeterSR/warren-mesh/internal/link"
	"github.com/PeterSR/warren-mesh/internal/service"
)

// Config is a dev.yaml.
type Config struct {
	// Dir is where the nodes keep their sockets and state. Empty means a
	// temporary directory that is removed at stop.
	Dir string `yaml:"dir"`
	// Fast shortens Warren's timers (lease grace, stale window, scan,
	// debounce, serve and PUT intervals), so tests need not wait. The
	// contract's real values are in the service API document.
	Fast  bool   `yaml:"fast"`
	Nodes []Node `yaml:"nodes"`
}

// Node is one Warren node.
type Node struct {
	Name string `yaml:"name"`
	// Services are the services on the node itself (not run by dev).
	Services []config.Service `yaml:"services"`
	Children []config.Child   `yaml:"children"`
	// Default is the template for directories made under the node's attach
	// directory while dev runs.
	Default     *config.Default `yaml:"default"`
	MaxChildren int             `yaml:"max_children"`
}

// Load reads and checks a dev.yaml (strict keys, ${VAR} expansion).
func Load(path string) (*Config, error) {
	var c Config
	if err := confload.Load(path, &c); err != nil {
		return nil, err
	}
	return &c, c.check()
}

// Parse reads a dev.yaml from memory.
func Parse(data []byte) (*Config, error) {
	var c Config
	if err := confload.Parse(data, "", &c); err != nil {
		return nil, err
	}
	return &c, c.check()
}

func (c *Config) check() error {
	if len(c.Nodes) == 0 {
		return errors.New("dev config: no nodes")
	}
	seen := map[string]bool{}
	for i, n := range c.Nodes {
		if n.Name == "" {
			return fmt.Errorf("dev config: nodes[%d] has no name", i)
		}
		if seen[n.Name] {
			return fmt.Errorf("dev config: node %q listed twice", n.Name)
		}
		seen[n.Name] = true
		for _, s := range n.Services {
			if len(s.Run) > 0 || len(s.Env) > 0 || len(s.EnvPass) > 0 {
				return fmt.Errorf("dev config: node %q service %q: dev starts no processes, so run, env and env_pass are not allowed", n.Name, s.Name)
			}
		}
	}
	return nil
}

// Socket is one service socket of a running cluster.
type Socket struct {
	Node    string
	Relay   string // relay id: the node or <label>.<node>
	Service string
	Path    string
	Allow   []string
	Flat    bool
	Child   bool
}

// Instance is one running node.
type Instance struct {
	Name    string
	Dir     string // services, attach, state and control live under it
	Control string
	Config  *config.Config
	Daemon  *daemon.Daemon
}

// Cluster is the running nodes.
type Cluster struct {
	Dir    string
	Nodes  []*Instance
	Net    *link.Net
	timers bool // this mesh holds the process-wide timers
	remove bool
}

// Fast timers, as `fast: true` sets them.
const (
	FastScan          = 100 * time.Millisecond
	FastLeaseGrace    = 1500 * time.Millisecond
	FastStaleFor      = 3 * time.Second
	FastBootHold      = 500 * time.Millisecond
	FastServeInterval = 300 * time.Millisecond
	FastPutInterval   = 300 * time.Millisecond
	FastDrainWait     = 1500 * time.Millisecond
)

// The timers are package variables of Warren, read by every running node, so
// they are only ever written while no mesh of this process runs: the first
// mesh sets them for its mode and the last one to stop puts them back. Meshes
// that run at the same time must therefore agree on the mode, and a mesh of
// the other mode is refused.
var timers struct {
	mu    sync.Mutex
	users int
	fast  bool
	saved func() // puts the real values back
}

// acquireTimers registers a mesh that wants fast (or real) timers.
func acquireTimers(fast bool) error {
	timers.mu.Lock()
	defer timers.mu.Unlock()
	if timers.users > 0 {
		if timers.fast != fast {
			return fmt.Errorf("a mesh with %s timers already runs in this process, and the timers are process-wide: start meshes of one kind only at a time (fast: %t)", map[bool]string{true: "fast", false: "real"}[timers.fast], timers.fast)
		}
		timers.users++
		return nil
	}
	timers.users, timers.fast, timers.saved = 1, fast, nil
	if fast {
		sc, lg, sf, bh, si, pi, dw := service.ScanEvery, service.LeaseGrace, service.StaleFor, service.BootHold, service.ServeInterval, service.PutInterval, service.DrainWait
		pd, pg := service.PeerDebounce, service.PollGap
		service.ScanEvery, service.LeaseGrace, service.StaleFor, service.BootHold = FastScan, FastLeaseGrace, FastStaleFor, FastBootHold
		service.ServeInterval, service.PutInterval, service.DrainWait = FastServeInterval, FastPutInterval, FastDrainWait
		service.PeerDebounce, service.PollGap = 50*time.Millisecond, 20*time.Millisecond
		timers.saved = func() {
			service.ScanEvery, service.LeaseGrace, service.StaleFor, service.BootHold = sc, lg, sf, bh
			service.ServeInterval, service.PutInterval, service.DrainWait = si, pi, dw
			service.PeerDebounce, service.PollGap = pd, pg
		}
	}
	return nil
}

// releaseTimers is called when a mesh has stopped; the last one restores.
func releaseTimers() {
	timers.mu.Lock()
	defer timers.mu.Unlock()
	if timers.users--; timers.users == 0 && timers.saved != nil {
		timers.saved()
		timers.saved = nil
	}
}

// Start brings every node up. dir, if not empty, replaces the config's Dir.
func Start(c *Config, dir string, log *slog.Logger) (*Cluster, error) {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	cl := &Cluster{Net: link.NewNet()}
	if dir == "" {
		dir = c.Dir
	}
	if dir == "" {
		// Short, so that socket paths fit sun_path.
		d, err := os.MkdirTemp("", "warren-dev-")
		if err != nil {
			return nil, err
		}
		dir, cl.remove = d, true
	} else {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return nil, err
		}
		dir = abs
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	cl.Dir = dir
	if err := acquireTimers(c.Fast); err != nil {
		if cl.remove {
			os.RemoveAll(dir)
		}
		return nil, err
	}
	cl.timers = true
	for _, n := range c.Nodes {
		in, err := cl.startNode(n, log)
		if err != nil {
			cl.Stop()
			return nil, fmt.Errorf("node %s: %w", n.Name, err)
		}
		cl.Nodes = append(cl.Nodes, in)
	}
	return cl, nil
}

func (cl *Cluster) startNode(n Node, log *slog.Logger) (*Instance, error) {
	nd := filepath.Join(cl.Dir, n.Name)
	cfg := &config.Config{
		Node:     n.Name,
		StateDir: filepath.Join(nd, "state"),
		Services: config.Services{Dir: filepath.Join(nd, "services"), List: n.Services},
		Attach: config.Attach{Dir: filepath.Join(nd, "attach"), MaxChildren: n.MaxChildren,
			Default: n.Default, Children: n.Children},
	}
	if len(n.Services) == 0 {
		cfg.Services.Dir = ""
	}
	if len(n.Children) == 0 && n.Default == nil {
		cfg.Attach.Dir = ""
	}
	// Normalise checks and fills in everything, as for a real config. The
	// control socket sits next to the services directory, as by default.
	cfg.Control = filepath.Join(nd, "control.sock")
	if err := cfg.Normalise(); err != nil {
		return nil, err
	}
	// Private directories: dev is for one user, and Warren would warn about
	// directories others can enter.
	for _, d := range []string{cfg.Services.Dir, cfg.Attach.Dir} {
		if d != "" {
			if err := os.MkdirAll(d, 0o700); err != nil {
				return nil, err
			}
		}
	}
	// A child directory is made for every configured child; the node never
	// makes directories in a child's directory, only in its own.
	for _, c := range cfg.Attach.Children {
		if err := os.MkdirAll(filepath.Join(cfg.Attach.Dir, c.Name), 0o755); err != nil {
			return nil, err
		}
	}
	// Other nodes see this one only once it answers.
	f := cl.Net.JoinDown(n.Name)
	f.HelloValue.Name = "memory"
	f.HelloValue.SoftwareVersion = service.Version
	d, err := daemon.Start(daemon.Options{Load: func() (*config.Config, error) { return cfg, nil }, Log: log, Link: f})
	if err != nil {
		return nil, err
	}
	cl.Net.SetDown(n.Name, false)
	return &Instance{Name: n.Name, Dir: nd, Control: cfg.Control, Config: cfg, Daemon: d}, nil
}

// Stop shuts every node down and removes a temporary directory.
func (cl *Cluster) Stop() {
	for _, in := range cl.Nodes {
		in.Daemon.Stop()
	}
	cl.Nodes = nil
	if cl.timers {
		cl.timers = false
		releaseTimers()
	}
	if cl.remove {
		os.RemoveAll(cl.Dir)
		cl.remove = false
	}
}

// Sockets lists the service sockets of the config, node by node: the node's
// own, then each configured child's. Children made from the `default`
// template while dev runs are not listed (their directories are made by
// whoever wants them, and their sockets are <attach dir>/<label>/<service>.sock).
func (cl *Cluster) Sockets() []Socket {
	var out []Socket
	for _, in := range cl.Nodes {
		cfg := in.Config
		for _, s := range cfg.Services.List {
			out = append(out, Socket{Node: in.Name, Relay: in.Name, Service: s.Name, Path: cfg.ServicePath(s.Name), Allow: s.Allow})
		}
		for _, c := range cfg.Attach.Children {
			for _, s := range c.Services {
				out = append(out, Socket{Node: in.Name, Relay: c.Name + "." + in.Name, Service: s.Name,
					Path: cfg.ChildPath(c.Name, s.Name), Allow: s.Allow, Flat: c.Flat, Child: true})
			}
		}
	}
	return out
}

// Print writes what a service author needs: where each socket is, who may
// call it, and how to reach it.
func (cl *Cluster) Print(w io.Writer) {
	fmt.Fprintf(w, "warren dev: %d node(s) in %s\n", len(cl.Nodes), cl.Dir)
	for _, in := range cl.Nodes {
		cfg := in.Config
		fmt.Fprintf(w, "\nnode %s (control socket %s)\n", in.Name, in.Control)
		var rows []Socket
		for _, s := range cl.Sockets() {
			if s.Node == in.Name {
				rows = append(rows, s)
			}
		}
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].Relay < rows[j].Relay })
		last := ""
		for _, s := range rows {
			if s.Relay != last {
				last = s.Relay
				kind := "node"
				switch {
				case s.Child && s.Flat:
					kind = "flat child"
				case s.Child:
					kind = "child"
				}
				fmt.Fprintf(w, "  relay %s (%s)\n", s.Relay, kind)
			}
			allow := strings.Join(s.Allow, ", ")
			if allow == "" {
				allow = "nobody"
			}
			fmt.Fprintf(w, "    service %-12s %s   (allow: %s)\n", s.Service, s.Path, allow)
		}
		if d := cfg.Attach.Default; d != nil {
			fmt.Fprintf(w, "  a directory made under %s gets the default services within a few seconds\n", cfg.Attach.Dir)
		}
	}
	fmt.Fprintf(w, "\nA service connects to its socket, for example:\n")
	fmt.Fprintf(w, "  export WARREN_SERVICE_SOCKET=<a socket path above>\n")
	fmt.Fprintf(w, "  curl --unix-socket \"$WARREN_SERVICE_SOCKET\" -H 'X-Warren: 1' http://warren/v1/service/self\n")
	fmt.Fprintf(w, "Stop with Ctrl-C.\n")
}
