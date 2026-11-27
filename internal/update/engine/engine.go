// Package engine is the update machinery of warren-update: the pipeline from
// a signed manifest to a swapped binary (verify, plan, stage, install), the
// health watcher that judges the restarted service from Warren's control API,
// the revert and the recovery at start, and the updater's own socket.
//
// This release covers service components installed from a directory. The engine
// talks to Warren only through the documented control API and imports none of
// the daemon's internals (service, daemon, config); it shares the leaf
// packages internal/platform and internal/fsutil.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PeterSR/warren-mesh/internal/update/state"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
)

// HostPlatform is the goos/goarch of this build; a manifest must list it for
// an install to be accepted. Options.Platform overrides it for an Engine.
const HostPlatform = runtime.GOOS + "/" + runtime.GOARCH

// ErrInjected is what a test hook returns to simulate a crash at that point:
// the operation stops at once and does nothing more, as a killed process would.
var ErrInjected = errors.New("injected crash")

// Refusal is an expected "no": the install or revert is not allowed now. Its
// text is for the operator.
type Refusal struct{ Msg string }

func (r *Refusal) Error() string { return r.Msg }

// freeUnfinishedHint says how an unfinished revert whose service Warren no
// longer reports is freed.
const freeUnfinishedHint = "if its service is gone, add explicit targets to it in the config and reload, so the watcher can finish it; a target must have the name the record uses (normally the component name) and point at the file the service ran"

// refuseUnfinishedRevert is the refusal of an install while the component has
// a revert that is not finished; --force does not pass it.
func refuseUnfinishedRevert(name string) error {
	return refuse("component %s has an unfinished revert: wait for it to finish; --force does not skip it (%s)", name, freeUnfinishedHint)
}

func refuse(format string, args ...any) error { return &Refusal{Msg: fmt.Sprintf(format, args...)} }

// Options make an Engine.
type Options struct {
	Config *uconfig.Config
	// Control reaches Warren; nil for the offline revert, which touches no
	// process.
	Control *Control
	Log     *slog.Logger
	// Version is the updater's own version.
	Version string
	// PollEvery is how often the health watcher looks; default 1 s.
	PollEvery time.Duration
	// Hook, if set, is called at named points of an install, a revert and
	// the recovery; returning ErrInjected stops the operation as a crash would
	// (tests).
	Hook func(point string) error
	// Now is the clock; default time.Now. Tests inject their own.
	Now func() time.Time
	// Wait pauses for a duration, where the engine waits for another process
	// to finish what it is doing; default time.Sleep. Tests inject a hook that
	// needs no real time.
	Wait func(time.Duration)
	// Platform is the goos/goarch a manifest must list; default HostPlatform.
	Platform string
}

// Engine is the updater's logic.
type Engine struct {
	log     *slog.Logger
	store   state.Store
	ctl     *Control
	version string
	poll    time.Duration
	hook    func(string) error
	nowFn   func() time.Time
	wait    func(time.Duration)
	// platform is the goos/goarch a manifest must list.
	platform string

	mu  sync.Mutex // guards cfg, live, holders, locks, clock
	cfg *uconfig.Config
	// live is the component whose Install call is running now; only Install sets
	// and clears it. holders are derived from the records and kept up to date by
	// whoever writes one (syncHolder): what each component's record holds.
	live    string
	holders map[string]string
	locks   map[string]*sync.Mutex
	clock   map[string]*settleClock
	warned  map[string]time.Time

	reloadMu sync.Mutex // one reload at a time: SIGHUP and the socket both call Reload
}

// settleClock is the in-memory settle timer of a watched component: the
// start_seq and lease epoch seen, and since when they have not changed while
// the lease was held.
type settleClock struct {
	since    time.Time
	startSeq int
	epoch    int
}

// New makes an engine.
func New(o Options) *Engine {
	e := &Engine{log: o.Log, ctl: o.Control, version: o.Version, poll: o.PollEvery, hook: o.Hook, cfg: o.Config,
		nowFn: o.Now, wait: o.Wait, platform: o.Platform,
		holders: map[string]string{}, locks: map[string]*sync.Mutex{}, clock: map[string]*settleClock{}, warned: map[string]time.Time{}}
	if e.log == nil {
		e.log = slog.Default()
	}
	if e.nowFn == nil {
		e.nowFn = time.Now
	}
	if e.wait == nil {
		e.wait = time.Sleep
	}
	if e.platform == "" {
		e.platform = HostPlatform
	}
	if e.poll <= 0 {
		e.poll = time.Second
	}
	e.store = state.Store{Dir: o.Config.StateDir}
	return e
}

// Version is the updater's version.
func (e *Engine) Version() string { return e.version }

// now is the engine's clock.
func (e *Engine) now() time.Time { return e.nowFn() }

func (e *Engine) config() *uconfig.Config {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cfg
}

func (e *Engine) component(name string) (*uconfig.Comp, error) {
	c := e.config().Components[name]
	if c == nil {
		return nil, refuse("component %q is not configured", name)
	}
	return c, nil
}

func (e *Engine) at(point string) error {
	if e.hook == nil {
		return nil
	}
	return e.hook(point)
}

// lockComp takes the component's lock (lock order: update.lock, then the
// install mutex, then this).
func (e *Engine) lockComp(name string) func() {
	e.mu.Lock()
	m := e.locks[name]
	if m == nil {
		m = &sync.Mutex{}
		e.locks[name] = m
	}
	e.mu.Unlock()
	m.Lock()
	return m.Unlock
}

// What a record holds. Only a probation that is not yet healthy keeps other
// components from installing: that is the one install whose failure has to be
// attributable. A restored version waiting for its restart, or a revert that is
// not finished, holds only its own component, so a service that cannot be
// reached never blocks the rest of the node.
const (
	holdProbation  = "probation"
	holdRestored   = "restored"
	holdUnfinished = "unfinished"
)

// holdKind is what a record holds: probation before first healthy, a restored
// version still being watched, an unfinished revert, or nothing. It is derived
// from the record, so it survives a restart of the updater.
func holdKind(r *state.Record) string {
	switch {
	case r.Phase == state.Installing, r.Phase == state.Probation && r.HealthySince == nil:
		return holdProbation
	case r.UnfinishedRevert():
		return holdUnfinished
	case r.WatchingRestored():
		return holdRestored
	}
	return ""
}

// acquire takes the install mutex for an Install call: one install at a time
// per node (it never bypasses a live one), and none while another component's
// probation is being judged. The component's own record may hold it too:
// a probation always does, a restored watch or an unfinished revert only until
// force says to go on (they are waiting for a restart, not being judged).
func (e *Engine) acquire(name string, force bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.live != "" {
		return refuse("busy: installing %s", e.live)
	}
	for other, kind := range e.holders {
		if other != name && kind == holdProbation {
			return refuse("busy: %s is on probation", other)
		}
	}
	switch kind := e.holders[name]; {
	case kind == holdProbation:
		return refuse("component %s is on probation: wait for it to settle, or revert it", name)
	case kind == holdUnfinished:
		return refuseUnfinishedRevert(name)
	case kind != "" && !force:
		return refuse("component %s is waiting for a restart (%s); wait for it, or use --force", name, kind)
	}
	e.live = name
	return nil
}

// release ends an Install call's hold; it never touches what the records hold.
func (e *Engine) release(name string) {
	e.mu.Lock()
	if e.live == name {
		e.live = ""
	}
	e.mu.Unlock()
}

// syncHolder makes the derived hold of a component match its record.
func (e *Engine) syncHolder(name string, r *state.Record) {
	if r == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if k := holdKind(r); k != "" {
		e.holders[name] = k
	} else {
		delete(e.holders, name)
	}
}

func (e *Engine) dropHolder(name string) {
	e.mu.Lock()
	delete(e.holders, name)
	e.mu.Unlock()
}

// load reads a record; a component never seen gets the state-lost record
// (not saved).
func (e *Engine) load(name string) (*state.Record, error) {
	r, err := e.store.Load(name)
	if errors.Is(err, state.ErrNone) {
		r = state.New(name)
		r.NeedsManual = state.StateLost
		return r, nil
	}
	return r, err
}

func (e *Engine) save(r *state.Record) error { return e.store.Save(r) }

// targetsFor maps a component's logical targets to paths: its configured
// ones, else the default {component: exe of the service}, which needs the
// service object and meets the same path rules and uniqueness as a configured
// target.
func (e *Engine) targetsFor(c *uconfig.Comp, svc *Service) (map[string]string, error) {
	if len(c.Targets) > 0 {
		return c.Targets, nil
	}
	if svc == nil || svc.Exe == "" {
		return nil, refuse("component %s has no targets in the config and Warren does not report the service's exe (%s)", c.Name, exeProblem(svc))
	}
	if err := uconfig.CheckPathSyntax(svc.Exe); err != nil {
		return nil, refuse("the service's exe %q: %v", svc.Exe, err)
	}
	if err := uconfig.CheckTargetPath(svc.Exe, os.Geteuid()); err != nil {
		return nil, refuse("the service's exe %s: %v", svc.Exe, err)
	}
	cfg := e.config()
	if uconfig.UnderDir(svc.Exe, cfg.StateDir) {
		return nil, refuse("the service's exe %s is under the updater's state_dir", svc.Exe)
	}
	key := uconfig.ResolvedKey(svc.Exe)
	for _, name := range cfg.Names() {
		other := cfg.Components[name]
		if other == c {
			continue
		}
		for _, p := range other.Targets {
			if uconfig.ResolvedKey(p) == key {
				return nil, refuse("the service's exe %s is also a target of component %s", svc.Exe, name)
			}
		}
	}
	return map[string]string{c.Name: svc.Exe}, nil
}

func exeProblem(svc *Service) string {
	if svc == nil {
		return "the service is not managed now"
	}
	if svc.ExeError != "" {
		return "run: must name an absolute path: " + svc.ExeError
	}
	return "run: must name an absolute path"
}

// exeMatches reports whether the service runs one of the targets.
func exeMatches(svc *Service, targets map[string]string) bool {
	if svc == nil || svc.Exe == "" {
		return false
	}
	want := uconfig.ResolvedKey(svc.Exe)
	for _, p := range targets {
		if uconfig.ResolvedKey(p) == want {
			return true
		}
	}
	return false
}

// ---- status ----

// ComponentStatus is what `warren-update status` shows of one component.
type ComponentStatus struct {
	Name           string      `json:"name"`
	Channel        string      `json:"channel"`
	Service        string      `json:"service"`
	Phase          state.Phase `json:"phase"`
	Version        string      `json:"version"`
	Pinned         string      `json:"pinned,omitempty"`
	Updatable      *bool       `json:"updatable,omitempty"`
	NotUpdatable   string      `json:"not_updatable,omitempty"`
	HealthySince   *time.Time  `json:"healthy_since,omitempty"`
	CanaryEnds     *time.Time  `json:"canary_ends,omitempty"`
	Prev           string      `json:"prev,omitempty"`
	Bad            []state.Bad `json:"bad,omitempty"`
	NeedsManual    string      `json:"needs_manual,omitempty"`
	LastError      string      `json:"last_error,omitempty"`
	RestartPending bool        `json:"restart_pending,omitempty"`
	NotManaged     bool        `json:"not_managed,omitempty"`
	Busy           bool        `json:"busy,omitempty"`
}

// NodeStatus is the whole status.
type NodeStatus struct {
	Updater         string            `json:"updater"`
	WarrenVersion   string            `json:"warren_version,omitempty"`
	Pinned          string            `json:"pinned,omitempty"`
	WarrenReachable bool              `json:"warren_reachable"`
	Components      []ComponentStatus `json:"components"`
}

// Status describes every configured component.
func (e *Engine) Status(ctx context.Context) NodeStatus {
	cfg := e.config()
	out := NodeStatus{Updater: e.version, Components: []ComponentStatus{}}
	if e.ctl != nil {
		if ws, err := e.ctl.Status(ctx); err == nil {
			out.WarrenReachable, out.WarrenVersion, out.Pinned = true, ws.Warren.Version, ws.Pinned
		}
	}
	e.mu.Lock()
	live := e.live
	holders := map[string]string{}
	for k, v := range e.holders {
		holders[k] = v
	}
	e.mu.Unlock()
	for _, name := range cfg.Names() {
		c := cfg.Components[name]
		cs := ComponentStatus{Name: name, Channel: c.Channel, Service: c.Service, Pinned: c.Pinned, Busy: live == name || holders[name] == holdProbation}
		if r, err := e.store.Load(name); err == nil {
			cs.Phase, cs.HealthySince, cs.CanaryEnds, cs.Bad, cs.NeedsManual, cs.LastError = r.Phase, r.HealthySince, r.CanaryEnds, r.Bad, r.NeedsManual, r.LastError
			if !r.KnownPhase() {
				cs.Phase = state.Failed
			}
			if r.Installed != nil {
				cs.Version = r.Installed.Version
			}
			if r.Prev != nil {
				cs.Prev = r.Prev.Version
			}
			if r.Request != nil && r.Request.MinStartSeq == nil {
				cs.RestartPending = true
			}
		} else if errors.Is(err, state.ErrNone) {
			cs.Phase, cs.NeedsManual = state.Committed, state.StateLost
		} else {
			cs.Phase, cs.LastError = state.Failed, err.Error()
		}
		if cs.Version == "" && c.Pinned != "" {
			cs.Version = "unknown"
		}
		if e.ctl != nil && out.WarrenReachable {
			if svc, err := e.ctl.Service(ctx, c.Service); err == nil {
				cs.Updatable, cs.NotUpdatable = svc.Updatable, svc.NotUpdatable
			} else if errors.Is(err, ErrNotFound) {
				cs.NotManaged = true
			}
		}
		out.Components = append(out.Components, cs)
	}
	return out
}

// ---- reload ----

// Reload applies a new configuration, all or nothing: everything is checked
// and prepared first, and the configuration is swapped last. A component
// removed from it is refused while its record is an install in flight or an
// unfinished revert (settle first, or revert it, then remove it); a record in
// probation, or a restored version still being watched, is closed as settled,
// unwatched, after the swap. A component whose `service` or `targets` change is
// refused while its record is being installed, watched or reverted. A component
// added gets a state-lost record, written before the swap, so a record that
// cannot be written refuses the reload with the old configuration still in
// force. Reloads run one at a time (SIGHUP and the socket both reach here), and
// a reload waits for an install or a watcher poll that holds a component lock it
// needs.
func (e *Engine) Reload(next *uconfig.Config) error {
	e.reloadMu.Lock()
	defer e.reloadMu.Unlock()
	old := e.config()
	if next.StateDir != old.StateDir || next.Control != old.Control {
		return refuse("state_dir and warren.control cannot change on reload; restart the updater")
	}
	// The components the reload decides something about: those it removes, and
	// those whose service or targets it changes; and those it adds, whose record
	// it creates. All their locks are taken in one pass in name order and held
	// from the checks through the swap, so a record cannot change under the
	// decision and no lock is ever taken out of order.
	var removed []string
	decided := map[string]bool{}
	for _, name := range old.Names() { // sorted
		switch n := next.Components[name]; {
		case n == nil:
			removed = append(removed, name)
			decided[name] = true
		case n.Service != old.Components[name].Service || !maps.Equal(n.Targets, old.Components[name].Targets):
			decided[name] = true
		}
	}
	lockNames := map[string]bool{}
	for n := range decided {
		lockNames[n] = true
	}
	for _, n := range next.Names() {
		lockNames[n] = true
	}
	for _, name := range slices.Sorted(maps.Keys(lockNames)) {
		defer e.lockComp(name)()
	}
	var closing []string
	for _, name := range slices.Sorted(maps.Keys(decided)) {
		r, err := e.store.Load(name)
		if errors.Is(err, state.ErrNone) {
			continue
		}
		if err != nil {
			return fmt.Errorf("component %s: %w", name, err)
		}
		if n := next.Components[name]; n != nil { // its service or targets change
			if holdKind(r) != "" {
				// The one change that may pass: explicit targets for a component that
				// had the default ones, when its revert cannot be finished because
				// Warren no longer reports the service's exe. That is the way out.
				o := old.Components[name]
				if r.UnfinishedRevert() && n.Service == o.Service && len(o.Targets) == 0 && len(n.Targets) > 0 {
					// Only if every target the record needs is among the new ones.
					var missing []string
					for t := range r.Prev.Files {
						if _, ok := n.Targets[t]; !ok {
							missing = append(missing, t)
						}
					}
					if len(missing) == 0 {
						continue
					}
					sort.Strings(missing)
					return refuse("component %s: the record needs the targets %s, which the new targets lack; a target must have the name the record uses (normally the component name) and point at the file the service ran", name, strings.Join(missing, ", "))
				}
				return refuse("component %s is being installed, watched or reverted: its service and targets cannot change until it settles (an unfinished revert of a component with default targets may be freed by adding explicit targets, named as the record names them, normally the component name, and pointing at the file the service ran)", name)
			}
			continue
		}
		switch {
		case r.Phase == state.Installing:
			return refuse("component %s is being installed; reload after it settles", name)
		case r.UnfinishedRevert():
			return refuse("component %s is finishing a revert; reload after it settles (%s)", name, freeUnfinishedHint)
		case r.Phase == state.Probation, r.WatchingRestored():
			closing = append(closing, name)
		}
	}
	for _, name := range next.Names() {
		if err := e.ensureRecordLocked(name); err != nil {
			return err
		}
	}
	e.mu.Lock()
	e.cfg = next
	e.mu.Unlock()
	for _, name := range closing {
		r, err := e.store.Load(name)
		if err != nil {
			continue
		}
		switch {
		case r.Phase == state.Probation:
			r.Phase, r.Deadline = state.Committed, nil
		case r.WatchingRestored():
			r.Request, r.Deadline = nil, nil // settled: nothing is watched any more
		default:
			continue
		}
		e.log.Info("component removed from the config while watched; closed as settled, unwatched", "component", name)
		if err := e.save(r); err != nil {
			e.log.Error("cannot close the record of a removed component", "component", name, "err", err)
		}
	}
	for _, name := range removed {
		e.dropHolder(name)
	}
	return nil
}

// ensureRecordLocked creates the state-lost record of a component that has
// none; the component lock is held.
func (e *Engine) ensureRecordLocked(name string) error {
	_, err := e.store.Load(name)
	if errors.Is(err, state.ErrNone) {
		r := state.New(name)
		r.NeedsManual = state.StateLost
		return e.save(r)
	}
	return err
}

// stagingDir is where a version's files are staged.
func (e *Engine) stagingDir(component, version string) string {
	return filepath.Join(e.config().StateDir, "staging", component, version)
}

func clearDir(path string) { _ = os.RemoveAll(path) }
