// Package config reads Warren's YAML configuration: strictly (unknown keys
// are refused), with defaults filled in and every name, path and limit
// checked, so a bad file fails at load or reload and never half-applies.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/PeterSR/warren-mesh/confload"
	"github.com/PeterSR/warren-mesh/internal/grammar"
	"github.com/PeterSR/warren-mesh/internal/pinned"
	"github.com/PeterSR/warren-mesh/internal/platform"
)

// Defaults (docs/architecture.md, "Limits and fairness").
const (
	// MaxServices is the most distinct service names a node has.
	MaxServices = 32

	DefaultInflight  = 16
	DefaultPerRoot   = 8
	DefaultOutflight = 16
	// Services of template children default to smaller pools, since anyone
	// who can make a directory gets them.
	// MaxInflight is the most inflight a socket may be given: a lease may have
	// that many polls open, and a socket takes 64 connections in all.
	MaxInflight = 32
	// SocketConns is how many connections a service socket takes, and
	// ViewPollConns how many of them the view polls may hold; inflight +
	// outflight + ViewPollConns must fit in SocketConns.
	SocketConns        = 64
	ViewPollConns      = 4
	TemplateInflight   = 4
	TemplatePerRoot    = 2
	TemplateOutflight  = 4
	DefaultMaxChildren = 16
	// DefaultTemplateEntries is max_entries for children made by the
	// template, DefaultChildEntries for configured children.
	DefaultTemplateEntries = 100
	DefaultChildEntries    = 1000
	MaxEntriesPerService   = 1000
)

// Config is the whole file.
type Config struct {
	Node     string `yaml:"node"`
	StateDir string `yaml:"state_dir"`
	// Control is the control socket (internal: status, reload, drain and
	// exit). Default: control.sock next to services.dir.
	Control   string     `yaml:"control"`
	Transport Transport  `yaml:"transport"`
	Tailscale *Tailscale `yaml:"tailscale"`
	// NATS is read so a file written for a later release still loads;
	// that transport is not built yet, so naming it is refused.
	NATS     map[string]any `yaml:"nats"`
	Services Services       `yaml:"services"`
	Attach   Attach         `yaml:"attach"`
	// Pinned, when set, says why this node's binaries are replaced from
	// outside Warren (a container image, a package manager). It is free
	// text, shown in the status, and tells an updater to stay away.
	Pinned string `yaml:"pinned"`
}

// Transport is how this node reaches others: a shipped one by name
// ("tailscale"), or a third party's, run by Warren (Run) or reached
// over two sockets (LinkA, LinkB). The zero value means no transport.
type Transport struct {
	Name  string
	Run   []string
	LinkA string
	LinkB string
	UID   *int
}

// UnmarshalYAML accepts "tailscale" or a mapping.
func (t *Transport) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		return n.Decode(&t.Name)
	}
	if n.Kind != yaml.MappingNode {
		return confload.Public(errors.New("transport: a name or a mapping with run, or link_a and link_b"))
	}
	var raw struct {
		Run   []string `yaml:"run"`
		LinkA string   `yaml:"link_a"`
		LinkB string   `yaml:"link_b"`
		UID   *int     `yaml:"uid"`
	}
	// Node.Decode is not strict, so check the keys by hand.
	for i := 0; i+1 < len(n.Content); i += 2 {
		switch k := n.Content[i].Value; k {
		case "run", "link_a", "link_b", "uid":
		default:
			return confload.Public(fmt.Errorf("transport: unknown key %q", k))
		}
	}
	if err := n.Decode(&raw); err != nil {
		return err
	}
	t.Run, t.LinkA, t.LinkB, t.UID = raw.Run, raw.LinkA, raw.LinkB, raw.UID
	return nil
}

// Set reports whether a transport is configured.
func (t Transport) Set() bool {
	return t.Name != "" || len(t.Run) > 0 || t.LinkA != "" || t.LinkB != ""
}

// External reports whether the transport is reached over sockets.
func (t Transport) External() bool { return t.LinkA != "" || t.LinkB != "" }

// Tailscale holds warren-tailscale's settings.
type Tailscale struct {
	Tag    string `yaml:"tag" json:"tag"`
	Listen string `yaml:"listen" json:"listen"`
	Port   int    `yaml:"port" json:"port"`
	Socket string `yaml:"socket" json:"socket"`
}

// Services are the services on the node itself.
type Services struct {
	Dir  string    `yaml:"dir"`
	List []Service `yaml:"list"`
}

// Service is one service at a relay. Namespaces, Run, Env and EnvPass only
// make sense on the node itself.
type Service struct {
	Name       string            `yaml:"name"`
	Allow      []string          `yaml:"allow"`
	Namespaces []string          `yaml:"namespaces"`
	Run        []string          `yaml:"run"`
	Env        map[string]string `yaml:"env"`
	EnvPass    []string          `yaml:"env_pass"`
	UID        *int              `yaml:"uid"`
	Inflight   int               `yaml:"inflight"`
	PerRoot    int               `yaml:"per_root"`
	Outflight  int               `yaml:"outflight"`
}

// Attach is where children live.
type Attach struct {
	Dir         string   `yaml:"dir"`
	MaxChildren int      `yaml:"max_children"`
	Default     *Default `yaml:"default"`
	Children    []Child  `yaml:"children"`
}

// Default is the template for directories not named under children.
type Default struct {
	// Flat is read only to be refused with a clear message.
	Flat       bool      `yaml:"flat"`
	MaxEntries int       `yaml:"max_entries"`
	Services   []Service `yaml:"services"`
}

// Child is a configured child.
type Child struct {
	Name       string    `yaml:"name"`
	Flat       bool      `yaml:"flat"`
	UID        *int      `yaml:"uid"`
	Names      []string  `yaml:"names"`
	MaxEntries int       `yaml:"max_entries"`
	Services   []Service `yaml:"services"`
}

// Parse reads and checks a configuration from data, through confload:
// unknown keys are errors, string values are expanded ($VAR, ${VAR:-d}),
// and everything is validated. Relative paths in secrets (there are none in
// Warren's own config) would be relative to the working directory.
func Parse(data []byte) (*Config, error) {
	var c Config
	if err := confload.Parse(data, "", &c); err != nil {
		return nil, err
	}
	if err := c.Normalise(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Load reads the file at path. Every load, including a reload, resolves
// variables and secret files again.
func Load(path string) (*Config, error) {
	var c Config
	if err := confload.Load(path, &c); err != nil {
		return nil, err
	}
	if err := c.Normalise(); err != nil {
		return nil, err
	}
	return &c, nil
}

func resolveControl(control, stateDir, servicesDir string) string {
	if control != "" {
		return control
	}
	base := stateDir
	if servicesDir != "" {
		base = filepath.Dir(servicesDir)
	}
	return filepath.Join(base, "control.sock")
}

// ControlPath finds the control socket of the config at path while expanding
// only the three values it depends on (control, state_dir, services.dir), so
// a variable or secret elsewhere in the file that is not set here does not
// stop `warren status` from reaching a running Warren.
func ControlPath(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var raw struct {
		Control  string `yaml:"control"`
		StateDir string `yaml:"state_dir"`
		Services struct {
			Dir string `yaml:"dir"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return "", fmt.Errorf("config: %w", err)
	}
	// A fixed order, so the same config always names the same key first.
	for _, k := range []struct {
		name string
		v    *string
	}{{"control", &raw.Control}, {"state_dir", &raw.StateDir}, {"services.dir", &raw.Services.Dir}} {
		x, err := confload.ExpandString(*k.v)
		if err != nil {
			return "", fmt.Errorf("config key %q: %w", k.name, err)
		}
		*k.v = x
	}
	if raw.StateDir == "" {
		return "", errors.New("config: state_dir is missing")
	}
	p := resolveControl(raw.Control, raw.StateDir, raw.Services.Dir)
	if !filepath.IsAbs(p) {
		return "", fmt.Errorf("control %q must be an absolute path", p)
	}
	return p, nil
}

// Normalise fills in defaults and validates.
func (c *Config) Normalise() error {
	if !grammar.Label(c.Node) {
		return fmt.Errorf("node %q must be a label (lower case letters, digits, dash, underscore)", c.Node)
	}
	if !filepath.IsAbs(c.StateDir) {
		return fmt.Errorf("state_dir %q must be an absolute path", c.StateDir)
	}
	c.Control = resolveControl(c.Control, c.StateDir, c.Services.Dir)
	if !filepath.IsAbs(c.Control) {
		return fmt.Errorf("control %q must be an absolute path", c.Control)
	}
	if err := c.checkSun(c.Control); err != nil {
		return fmt.Errorf("control: %w", err)
	}
	if err := pinned.Check(c.Pinned); err != nil {
		return err
	}
	if err := c.checkTransport(); err != nil {
		return err
	}
	if len(c.Services.List) > 0 && !filepath.IsAbs(c.Services.Dir) {
		return fmt.Errorf("services.dir %q must be an absolute path", c.Services.Dir)
	}
	if c.Attach.Dir != "" && !filepath.IsAbs(c.Attach.Dir) {
		return fmt.Errorf("attach.dir %q must be an absolute path", c.Attach.Dir)
	}
	if (len(c.Attach.Children) > 0 || c.Attach.Default != nil) && c.Attach.Dir == "" {
		return errors.New("attach: set dir")
	}
	if c.Services.Dir != "" {
		// services.dir is closed to other users (0700) unless a service names a
		// uid, so nothing Warren serves to another user may live in it.
		for _, x := range []struct{ what, path string }{
			{"attach.dir", c.Attach.Dir}, {"control", c.Control},
			{"transport.link_a", c.Transport.LinkA}, {"transport.link_b", c.Transport.LinkB},
		} {
			if x.path != "" && within(c.Services.Dir, x.path) {
				return fmt.Errorf("%s %q must not be services.dir or inside it", x.what, x.path)
			}
		}
	}
	if c.Attach.MaxChildren == 0 {
		c.Attach.MaxChildren = DefaultMaxChildren
	}
	if c.Attach.MaxChildren < 1 {
		return errors.New("attach.max_children must be positive")
	}

	childLabels := map[string]bool{}
	for i := range c.Attach.Children {
		ch := &c.Attach.Children[i]
		where := fmt.Sprintf("attach.children[%d] (%s)", i, ch.Name)
		if !grammar.Label(ch.Name) {
			return fmt.Errorf("%s: name must be a label", where)
		}
		if !grammar.RelayID(grammar.Child(ch.Name, c.Node)) {
			return fmt.Errorf("%s: %s.%s is longer than %d bytes", where, ch.Name, c.Node, grammar.MaxRelayID)
		}
		if childLabels[ch.Name] {
			return fmt.Errorf("%s: listed twice", where)
		}
		childLabels[ch.Name] = true
		if ch.UID != nil && *ch.UID < 0 {
			return fmt.Errorf("%s: uid must not be negative", where)
		}
		for _, p := range ch.Names {
			if !grammar.NamePattern(p) {
				return fmt.Errorf("%s: bad name pattern %q", where, p)
			}
		}
		if ch.MaxEntries == 0 {
			ch.MaxEntries = DefaultChildEntries
		}
		if ch.MaxEntries < 0 || ch.MaxEntries > MaxEntriesPerService {
			return fmt.Errorf("%s: max_entries must be 1 to %d", where, MaxEntriesPerService)
		}
		if err := checkServices(where, ch.Services, false, false); err != nil {
			return err
		}
		for _, s := range ch.Services {
			if err := c.checkSun(filepath.Join(c.Attach.Dir, ch.Name, s.Name+".sock")); err != nil {
				return fmt.Errorf("%s: %w", where, err)
			}
		}
	}
	if len(c.Attach.Children) > c.Attach.MaxChildren {
		return fmt.Errorf("attach.children lists %d children, more than attach.max_children (%d)", len(c.Attach.Children), c.Attach.MaxChildren)
	}
	if d := c.Attach.Default; d != nil {
		if d.Flat {
			return errors.New("attach.default cannot be flat: anyone who can create a directory would get the node's namespace")
		}
		if d.MaxEntries == 0 {
			d.MaxEntries = DefaultTemplateEntries
		}
		if d.MaxEntries < 0 || d.MaxEntries > MaxEntriesPerService {
			return fmt.Errorf("attach.default: max_entries must be 1 to %d", MaxEntriesPerService)
		}
		if err := checkServices("attach.default", d.Services, false, true); err != nil {
			return err
		}
	}

	if err := checkServices("services.list", c.Services.List, true, false); err != nil {
		return err
	}
	// Peers poll one list per service name, and a node has at most 32.
	names := map[string]bool{}
	for _, s := range c.Services.List {
		names[s.Name] = true
	}
	for _, ch := range c.Attach.Children {
		for _, s := range ch.Services {
			names[s.Name] = true
		}
	}
	if d := c.Attach.Default; d != nil {
		for _, s := range d.Services {
			names[s.Name] = true
		}
	}
	if len(names) > MaxServices {
		return fmt.Errorf("the config names %d different services; a node has at most %d", len(names), MaxServices)
	}
	for i := range c.Services.List {
		s := &c.Services.List[i]
		if err := c.checkSun(filepath.Join(c.Services.Dir, s.Name+".sock")); err != nil {
			return fmt.Errorf("services.list[%d] (%s): %w", i, s.Name, err)
		}
		// A namespace label may not equal a child label, nor repeat within
		// the service: the address would mean two things.
		seen := map[string]bool{}
		for _, ns := range s.Namespaces {
			switch {
			case !grammar.Label(ns) || !grammar.RelayID(grammar.Child(ns, c.Node)):
				return fmt.Errorf("services.list[%d] (%s): namespace %q must be a label that fits %s.%s in %d bytes", i, s.Name, ns, ns, c.Node, grammar.MaxRelayID)
			case childLabels[ns]:
				return fmt.Errorf("services.list[%d] (%s): namespace %q is also a child label", i, s.Name, ns)
			case seen[ns]:
				return fmt.Errorf("services.list[%d] (%s): namespace %q listed twice", i, s.Name, ns)
			}
			seen[ns] = true
		}
	}
	return nil
}

func (c *Config) checkTransport() error {
	t := c.Transport
	forms := 0
	if t.Name != "" {
		forms++
	}
	if len(t.Run) > 0 {
		forms++
	}
	if t.External() {
		forms++
	}
	if forms > 1 {
		return errors.New("transport: use one of a name, run, or link_a with link_b")
	}
	switch {
	case t.Name == "nats":
		return errors.New("transport: nats is not available in this release")
	case t.Name != "" && t.Name != "tailscale":
		return fmt.Errorf("transport %q is not known; use tailscale or {run: [...]}", t.Name)
	case t.External() && (t.LinkA == "" || t.LinkB == ""):
		return errors.New("transport: link_a and link_b go together")
	case t.External() && (!filepath.IsAbs(t.LinkA) || !filepath.IsAbs(t.LinkB)):
		return errors.New("transport: link_a and link_b must be absolute paths")
	case t.External() && t.UID == nil:
		return errors.New("transport: uid is required with link_a and link_b (the only user allowed to be the transport)")
	case t.UID != nil && (!t.External() || *t.UID < 0):
		return errors.New("transport: uid is a non-negative user id and only goes with link_a and link_b")
	case t.External():
		if err := c.checkSun(t.LinkA); err != nil {
			return fmt.Errorf("transport.link_a: %w", err)
		}
		if err := c.checkSun(t.LinkB); err != nil {
			return fmt.Errorf("transport.link_b: %w", err)
		}
	}
	if (t.Name != "tailscale") && c.Tailscale != nil {
		return errors.New("tailscale: block given, but transport is not tailscale")
	}
	if c.NATS != nil {
		return errors.New("nats: the nats block is not available in this release; remove it")
	}
	if t.Name == "tailscale" && (c.Tailscale == nil || c.Tailscale.Tag == "") {
		return errors.New("tailscale: transport is tailscale, so the tailscale: block with a tag is required")
	}
	if ts := c.Tailscale; ts != nil {
		if ts.Port < 0 || ts.Port > 65535 {
			return fmt.Errorf("tailscale: port %d is out of range", ts.Port)
		}
	}
	return nil
}

func checkServices(where string, list []Service, node, template bool) error {
	seen := map[string]bool{}
	for i := range list {
		s := &list[i]
		w := fmt.Sprintf("%s[%d] (%s)", where, i, s.Name)
		if where == "attach.default" {
			w = fmt.Sprintf("attach.default.services[%d] (%s)", i, s.Name)
		}
		if !grammar.Service(s.Name) {
			return fmt.Errorf("%s: service name must be a label of at most %d bytes", w, grammar.MaxServiceName)
		}
		if seen[s.Name] {
			return fmt.Errorf("%s: listed twice", w)
		}
		seen[s.Name] = true
		for _, p := range s.Allow {
			if !grammar.AllowPattern(p) {
				return fmt.Errorf("%s: bad allow pattern %q (use a relay id, *.<node> or *)", w, p)
			}
		}
		if !node && (len(s.Namespaces) > 0 || len(s.Run) > 0 || len(s.Env) > 0 || len(s.EnvPass) > 0) {
			return fmt.Errorf("%s: namespaces, run, env and env_pass are only for services on the node itself", w)
		}
		if s.UID != nil && *s.UID < 0 {
			return fmt.Errorf("%s: uid must not be negative", w)
		}
		for _, v := range []*int{&s.Inflight, &s.PerRoot, &s.Outflight} {
			if *v < 0 {
				return fmt.Errorf("%s: inflight, per_root and outflight must not be negative", w)
			}
		}
		if len(s.Run) > 0 && s.UID != nil && *s.UID != os.Geteuid() {
			// A managed service runs as Warren's own user, so a socket that only
			// another user may use would lock the service out of its own socket.
			return fmt.Errorf("%s: run starts the service as Warren's user (%d), so uid must be that user or left out", w, os.Geteuid())
		}
		if s.Inflight > MaxInflight {
			return fmt.Errorf("%s: inflight is at most %d (a lease has that many polls open, and a socket takes 64 connections)", w, MaxInflight)
		}
		in, per, out := DefaultInflight, DefaultPerRoot, DefaultOutflight
		if template {
			in, per, out = TemplateInflight, TemplatePerRoot, TemplateOutflight
		}
		if s.Inflight == 0 {
			s.Inflight = in
		}
		if s.Outflight == 0 {
			s.Outflight = out
		}
		// A socket takes at most 64 connections: one for each open poll, one
		// for each call it has in flight, and the view polls.
		if s.Inflight+s.Outflight+ViewPollConns > SocketConns {
			return fmt.Errorf("%s: inflight (%d) + outflight (%d) + %d view polls must be at most %d, the connections a socket takes", w, s.Inflight, s.Outflight, ViewPollConns, SocketConns)
		}
		if s.PerRoot == 0 {
			s.PerRoot = per
		}
		if len(s.Run) > 0 && s.Run[0] == "" {
			return fmt.Errorf("%s: run needs a program", w)
		}
		for k := range s.Env {
			if k == "" || k == "WARREN_SERVICE_SOCKET" {
				return fmt.Errorf("%s: env name %q is not allowed", w, k)
			}
		}
	}
	return nil
}

// within is whether p is dir itself or below it.
func within(dir, p string) bool {
	rel, err := filepath.Rel(filepath.Clean(dir), filepath.Clean(p))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func (c *Config) checkSun(p string) error {
	if len(p) > platform.MaxSunPath {
		return fmt.Errorf("socket path %q is %d bytes, over the %d byte limit of a unix socket path", p, len(p), platform.MaxSunPath)
	}
	return nil
}

// Child returns the configured child with this label.
func (c *Config) Child(label string) (*Child, int) {
	for i := range c.Attach.Children {
		if c.Attach.Children[i].Name == label {
			return &c.Attach.Children[i], i
		}
	}
	return nil, -1
}

// ChildLabels are the configured labels, in config order.
func (c *Config) ChildLabels() []string {
	var l []string
	for _, ch := range c.Attach.Children {
		l = append(l, ch.Name)
	}
	return l
}

// NamespaceTaken reports whether label is a service namespace of any
// service on the node; a child directory with that name gets nothing.
func (c *Config) NamespaceTaken(label string) bool {
	for _, s := range c.Services.List {
		for _, ns := range s.Namespaces {
			if ns == label {
				return true
			}
		}
	}
	return false
}

// ServicePath is where the node's service socket for name lives.
func (c *Config) ServicePath(name string) string {
	return filepath.Join(c.Services.Dir, name+".sock")
}

// ChildPath is where a child's service socket lives.
func (c *Config) ChildPath(label, name string) string {
	return filepath.Join(c.Attach.Dir, label, name+".sock")
}

// ChildSocketPathFits reports whether a child's path fits sun_path.
func (c *Config) ChildSocketPathFits(label, name string) bool {
	return len(c.ChildPath(label, name)) <= platform.MaxSunPath
}

// CheckReload says whether next may replace c while running. Settings that
// need a restart are refused, and so is removing a configured child whose
// directory still exists, so the host's agents can never silently fall back
// to the template's namespace. (A changed flat flag is a removal and an
// addition at once, which is fine: the child stays configured.)
func CheckReload(old, next *Config) error {
	switch {
	case old.Node != next.Node:
		return errors.New("reload: node cannot change without a restart")
	case old.Control != next.Control:
		return errors.New("reload: control cannot change without a restart")
	case old.StateDir != next.StateDir:
		return errors.New("reload: state_dir cannot change without a restart")
	case old.Services.Dir != next.Services.Dir:
		return errors.New("reload: services.dir cannot change without a restart")
	case old.Attach.Dir != next.Attach.Dir:
		return errors.New("reload: attach.dir cannot change without a restart")
	case !reflect.DeepEqual(old.Transport, next.Transport) ||
		!reflect.DeepEqual(old.Tailscale, next.Tailscale):
		return errors.New("reload: the transport and its settings cannot change without a restart")
	}
	for _, ch := range old.Attach.Children {
		if n, _ := next.Child(ch.Name); n != nil {
			continue
		}
		dir := filepath.Join(old.Attach.Dir, ch.Name)
		if _, err := os.Lstat(dir); err == nil {
			return fmt.Errorf("reload: child %q is removed from the config but its directory %s still exists; remove the directory first, or keep the child", ch.Name, dir)
		}
	}
	return nil
}

// SortedKeys is a helper for stable output.
func SortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
