// Package uconfig loads and checks the configuration of warren-update: its own
// file (not Warren's), loaded with the public confload package, which holds the
// keys, the components and the maps from logical targets to absolute paths.
//
// This release accepts service components only, installed from a directory: the
// components warren and warren-update, sources, auto, mesh and the timers of
// automatic installs are refused as unknown or not available in this release.
package uconfig

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/PeterSR/warren-mesh/confload"
	"github.com/PeterSR/warren-mesh/internal/fsutil"
	"github.com/PeterSR/warren-mesh/internal/pinned"
	"github.com/PeterSR/warren-mesh/internal/update/manifest"
)

// Limits. Variables, so tests can shorten them.
var (
	MinSettle, MaxSettle                  = 5 * time.Second, 5 * time.Minute
	MinCanary, MaxCanary                  = 1 * time.Minute, 24 * time.Hour
	MinHealthTimeout, MaxHealthTimeout    = 1 * time.Minute, 1 * time.Hour
	DefaultSettle                         = 30 * time.Second
	DefaultHealthTimeout                  = 5 * time.Minute
	DefaultCanaryStable, DefaultCanaryDev = 15 * time.Minute, 2 * time.Minute
)

// Other limits.
const (
	DefaultMaxSize = 256 << 20
	MaxKeysPerChan = 4
	MaxPath        = 4096
)

// ByteSize is a size such as 256MiB (KiB, MiB, GiB), or a plain number of bytes.
type ByteSize int64

// UnmarshalYAML reads the size.
func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return errors.New("a size must be a number of bytes or a number with KiB, MiB or GiB")
	}
	s := strings.TrimSpace(n.Value)
	mult := int64(1)
	for suffix, m := range map[string]int64{"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30} {
		if strings.HasSuffix(s, suffix) {
			s, mult = strings.TrimSpace(strings.TrimSuffix(s, suffix)), m
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 1 || v > (1<<40)/mult {
		return errors.New("a size must be a positive number of bytes, or a number with KiB, MiB or GiB")
	}
	*b = ByteSize(v * mult)
	return nil
}

// File is the file as written.
type File struct {
	StateDir      string               `yaml:"state_dir"`
	Warren        WarrenSection        `yaml:"warren"`
	Canary        time.Duration        `yaml:"canary"`
	HealthTimeout time.Duration        `yaml:"health_timeout"`
	Components    map[string]Component `yaml:"components"`
}

// WarrenSection says how to reach Warren.
type WarrenSection struct {
	Control string `yaml:"control"`
}

// Component is the update settings of one component.
type Component struct {
	Keys    map[string][]string `yaml:"keys"`
	Key     string              `yaml:"key"`
	Channel string              `yaml:"channel"`
	Auto    bool                `yaml:"auto"`
	Service string              `yaml:"service"`
	Targets map[string]string   `yaml:"targets"`
	Settle  time.Duration       `yaml:"settle"`
	MaxSize ByteSize            `yaml:"max_size"`
	// Pinned says this component's binary is replaced from outside the
	// updater: installs and reverts are refused, with this reason.
	Pinned string `yaml:"pinned"`
}

// Config is the checked configuration.
type Config struct {
	Path          string
	StateDir      string
	Control       string
	Canary        time.Duration // 0: the channel's default
	HealthTimeout time.Duration
	Components    map[string]*Comp
}

// Comp is one checked component.
type Comp struct {
	Name    string
	Keys    manifest.Keys
	Channel string
	Service string
	// Targets map logical targets to absolute paths. Empty means the default,
	// {<component>: <exe of the service>}, which the engine resolves.
	Targets map[string]string
	Settle  time.Duration
	MaxSize int64
	Pinned  string
}

// CanaryFor is the canary of a component: the configured one, else 2 minutes
// on the dev channel and 15 on the stable one.
func (c *Config) CanaryFor(comp *Comp) time.Duration {
	switch {
	case c.Canary > 0:
		return c.Canary
	case comp.Channel == manifest.Dev:
		return DefaultCanaryDev
	}
	return DefaultCanaryStable
}

// Names are the component names in order.
func (c *Config) Names() []string {
	return slices.Sorted(maps.Keys(c.Components))
}

// Load reads and checks the file at path, as the daemon does: target
// directories must belong to the user running the updater and not be writable
// by group or others. Errors from reading the YAML never quote a value (confload's
// rule); the checks name the key path, and may name a path or a label from the file.
func Load(path string) (*Config, error) { return load(path, os.Geteuid()) }

// LoadCLI reads the file for a command that only talks to the running updater
// (install, revert, status, reload), possibly as another user, root included:
// the ownership and permission rules of target directories are the daemon's to
// enforce and are not checked here.
func LoadCLI(path string) (*Config, error) { return load(path, -1) }

func load(path string, uid int) (*Config, error) {
	var f File
	if err := confload.Load(path, &f); err != nil {
		return nil, confload.MarkSafe(err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	c, err := check(&f, uid)
	if err != nil {
		return nil, err
	}
	c.Path = abs
	return c, nil
}

func check(f *File, uid int) (*Config, error) {
	c := &Config{StateDir: f.StateDir, Control: f.Warren.Control, Canary: f.Canary, HealthTimeout: f.HealthTimeout, Components: map[string]*Comp{}}
	if !cleanAbs(c.StateDir) {
		return nil, errors.New("state_dir must be a clean absolute path")
	}
	if !cleanAbs(c.Control) {
		return nil, errors.New("warren.control is required and must be a clean absolute path")
	}
	if c.Canary != 0 && (c.Canary < MinCanary || c.Canary > MaxCanary) {
		return nil, fmt.Errorf("canary must be between %v and %v", MinCanary, MaxCanary)
	}
	if c.HealthTimeout == 0 {
		c.HealthTimeout = DefaultHealthTimeout
	}
	if c.HealthTimeout < MinHealthTimeout || c.HealthTimeout > MaxHealthTimeout {
		return nil, fmt.Errorf("health_timeout must be between %v and %v", MinHealthTimeout, MaxHealthTimeout)
	}
	names := slices.Sorted(maps.Keys(f.Components))
	seen := map[string]string{} // resolved target path -> "component target"
	for _, name := range names {
		comp, err := checkComponent(name, f.Components[name], c.StateDir, uid)
		if err != nil {
			return nil, fmt.Errorf("components.%s: %w", name, err)
		}
		for _, t := range slices.Sorted(maps.Keys(comp.Targets)) {
			key := resolvedKey(comp.Targets[t])
			if other, dup := seen[key]; dup {
				return nil, fmt.Errorf("components.%s.targets.%s: the same file as %s", name, t, other)
			}
			seen[key] = name + " " + t
		}
		for _, other := range c.Names() {
			if c.Components[other].Service == comp.Service {
				return nil, fmt.Errorf("components.%s: service %q is also the service of component %s", name, comp.Service, other)
			}
		}
		c.Components[name] = comp
	}
	return c, nil
}

func checkComponent(name string, in Component, stateDir string, uid int) (*Comp, error) {
	if !manifest.ValidComponent(name) {
		return nil, errors.New("the name must be a label of at most 32 bytes (lower case letters, digits, dash, underscore)")
	}
	if name == "warren" || name == "warren-update" {
		return nil, errors.New("updating " + name + " is not available in this release")
	}
	if in.Auto {
		return nil, errors.New("auto is not available in this release")
	}
	if in.Service == "" {
		return nil, errors.New("service is required: the Warren service it restarts and watches")
	}
	out := &Comp{Name: name, Service: in.Service, Channel: in.Channel, Settle: in.Settle, MaxSize: int64(in.MaxSize), Pinned: in.Pinned}
	if out.Channel == "" {
		out.Channel = manifest.Stable
	}
	if out.Channel != manifest.Stable && out.Channel != manifest.Dev {
		return nil, errors.New("channel must be stable or dev")
	}
	if out.Settle == 0 {
		out.Settle = DefaultSettle
	}
	if out.Settle < MinSettle || out.Settle > MaxSettle {
		return nil, fmt.Errorf("settle must be between %v and %v", MinSettle, MaxSettle)
	}
	if out.MaxSize == 0 {
		out.MaxSize = DefaultMaxSize
	}
	if err := pinned.Check(out.Pinned); err != nil {
		return nil, err
	}
	keys, err := checkKeys(in, out.Channel, out.Pinned != "")
	if err != nil {
		return nil, err
	}
	out.Keys = keys
	out.Targets = map[string]string{}
	if len(in.Targets) > manifest.MaxTargets {
		return nil, fmt.Errorf("at most %d targets", manifest.MaxTargets)
	}
	for t, p := range in.Targets {
		if !manifest.ValidTarget(t) {
			return nil, fmt.Errorf("targets: %q is not a plain file label", t)
		}
		if err := CheckPathSyntax(p); err != nil {
			return nil, fmt.Errorf("targets.%s: %w", t, err)
		}
		if err := CheckTargetPath(p, uid); err != nil {
			return nil, fmt.Errorf("targets.%s: %w", t, err)
		}
		if UnderDir(p, stateDir) {
			return nil, fmt.Errorf("targets.%s: must not be under state_dir", t)
		}
		out.Targets[t] = p
	}
	return out, nil
}

// checkKeys reads and checks the keys. A pinned component installs nothing, so
// it may omit them; any it does list are still checked.
func checkKeys(in Component, channel string, pinned bool) (manifest.Keys, error) {
	raw := map[string][]string{}
	for ch, ks := range in.Keys {
		if ch != manifest.Stable && ch != manifest.Dev {
			return nil, fmt.Errorf("keys: %q is neither stable nor dev", ch)
		}
		raw[ch] = ks
	}
	if in.Key != "" {
		if len(in.Keys) > 0 {
			return nil, errors.New("key and keys are exclusive")
		}
		if channel != manifest.Stable {
			return nil, errors.New("key: is shorthand for a stable key and is valid only on a stable component")
		}
		raw[manifest.Stable] = []string{in.Key}
	}
	keys := manifest.Keys{}
	for ch, ks := range raw {
		if len(ks) > MaxKeysPerChan {
			return nil, fmt.Errorf("keys.%s: at most %d keys", ch, MaxKeysPerChan)
		}
		for i, t := range ks {
			k, err := manifest.ParseKey(t)
			if err != nil {
				return nil, fmt.Errorf("keys.%s[%d]: %w", ch, i, err)
			}
			keys[ch] = append(keys[ch], k)
		}
	}
	if pinned && len(keys) == 0 {
		return keys, nil
	}
	switch channel {
	case manifest.Stable:
		if len(keys[manifest.Stable]) == 0 {
			return nil, errors.New("a stable component needs a stable key")
		}
		if len(keys[manifest.Dev]) > 0 {
			return nil, errors.New("a stable component may not list a dev key")
		}
	case manifest.Dev:
		if len(keys[manifest.Dev]) == 0 {
			return nil, errors.New("a dev component needs a dev key")
		}
	}
	for _, s := range keys[manifest.Stable] {
		for _, d := range keys[manifest.Dev] {
			if s.Same(d) {
				return nil, errors.New("the same public key is listed for stable and dev")
			}
		}
	}
	return keys, nil
}

func cleanAbs(p string) bool {
	return p != "" && len(p) <= MaxPath && filepath.IsAbs(p) && filepath.Clean(p) == p
}

// UnderDir reports whether path is inside dir, once symlinks in both are
// resolved (so a link into the state directory is caught); where a path does
// not exist, the existing part of it is resolved and the rest kept as it is.
func UnderDir(path, dir string) bool {
	rel, err := filepath.Rel(resolvePartial(dir), resolvePartial(path))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// resolvePartial is EvalSymlinks for a path whose tail may not exist.
func resolvePartial(p string) string {
	p = filepath.Clean(p)
	tail := ""
	for {
		if r, err := filepath.EvalSymlinks(p); err == nil {
			return filepath.Join(r, tail)
		}
		parent := filepath.Dir(p)
		if parent == p {
			return filepath.Join(p, tail)
		}
		tail = filepath.Join(filepath.Base(p), tail)
		p = parent
	}
}

// CheckPathSyntax is the part of the target path rules that needs no file
// system: absolute, clean, at most 4096 bytes.
func CheckPathSyntax(p string) error {
	if !cleanAbs(p) {
		return errors.New("must be a clean absolute path of at most 4096 bytes")
	}
	return nil
}

// CheckTargetPath is the file system part of the path rules, checked at load
// and again before a swap: the final element is not a symlink and, if present,
// a regular file; the parent directory exists, belongs to uid and is not
// writable by group or others (a negative uid skips those two checks).
func CheckTargetPath(p string, uid int) error {
	if fi, err := os.Lstat(p); err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			return errors.New("must not be a symlink: the file replaced must be the file that runs")
		}
		if !fi.Mode().IsRegular() {
			return errors.New("must be a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dir := filepath.Dir(p)
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("directory: %w", err)
	}
	if !fi.IsDir() {
		return errors.New("the parent is not a directory")
	}
	if uid < 0 {
		return nil // the daemon's rules, not checked here
	}
	if owner, ok := fsutil.OwnerUID(fi); ok && owner != uid {
		return fmt.Errorf("directory %s is not owned by the user running the updater", dir)
	}
	if fi.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("directory %s is writable by group or others", dir)
	}
	return nil
}

// resolvedKey identifies a target file for uniqueness: the parent directory
// with symlinks resolved, and the name.
func resolvedKey(p string) string {
	dir, name := filepath.Split(p)
	if r, err := filepath.EvalSymlinks(filepath.Clean(dir)); err == nil {
		dir = r
	}
	return filepath.Join(dir, name)
}

// ResolvedKey is exported for the engine's default targets.
func ResolvedKey(p string) string { return resolvedKey(p) }
