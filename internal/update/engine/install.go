package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/PeterSR/warren-mesh/internal/update/manifest"
	"github.com/PeterSR/warren-mesh/internal/update/semver"
	"github.com/PeterSR/warren-mesh/internal/update/state"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
)

// InstallRequest asks for the staged version of a component to be installed.
type InstallRequest struct {
	Component string `json:"component"`
	// Version is the staged version to install.
	Version        string `json:"version"`
	AllowDowngrade bool   `json:"allow_downgrade"`
	// Force skips prudence only: the bad list, "healthy before", a failed
	// record. It never skips the install mutex, nor any signature, channel,
	// key, path or digest check.
	Force  bool `json:"force"`
	DryRun bool `json:"dry_run"`
}

// InstallResult says what happened.
type InstallResult struct {
	Component string            `json:"component"`
	Version   string            `json:"version"`
	Phase     string            `json:"phase"`
	Message   string            `json:"message"`
	Replaces  string            `json:"replaces,omitempty"`
	Targets   map[string]string `json:"targets,omitempty"`
}

// plan is the pure result of planning an install.
type plan struct {
	comp      *uconfig.Comp
	m         *manifest.Manifest
	data, sig []byte
	files     map[string]manifest.File
	targets   map[string]string
	svc       *Service
	dir       string
	manifSHA  string
}

// checkPins refuses a pinned component or a pinned node, and a service whose
// binary is not updatable, quoting the reason. svc may be nil.
func (e *Engine) checkPins(ctx context.Context, c *uconfig.Comp, what string, needSvc bool) (*Service, error) {
	if c.Pinned != "" {
		return nil, refuse("%s refused: component %s is pinned: %s", what, c.Name, c.Pinned)
	}
	if e.ctl == nil {
		return nil, nil
	}
	ws, err := e.ctl.Status(ctx)
	if err != nil {
		return nil, refuse("%s refused: cannot read Warren's status: %v", what, err)
	}
	if ws.Pinned != "" {
		return nil, refuse("%s refused: this node is pinned: %s", what, ws.Pinned)
	}
	svc, err := e.ctl.Service(ctx, c.Service)
	if errors.Is(err, ErrNotFound) {
		if needSvc {
			return nil, refuse("%s refused: Warren has no managed service %q now (not managed)", what, c.Service)
		}
		return nil, nil
	}
	if err != nil {
		return nil, refuse("%s refused: cannot read service %s: %v", what, c.Service, err)
	}
	if svc.Updatable != nil && !*svc.Updatable {
		return nil, refuse("%s refused: the binary of service %s is not updatable: %s", what, c.Service, svc.NotUpdatable)
	}
	return svc, nil
}

// Install installs a staged version. Everything but the last step happens with
// the service still running; the result is `probation` once Warren was asked
// to restart the service, and the health watcher judges it from there.
func (e *Engine) Install(ctx context.Context, req InstallRequest) (*InstallResult, error) {
	// The version names a directory: it is checked before it is used as one.
	if _, err := semver.Parse(req.Version); err != nil {
		return nil, refuse("version: %v", err)
	}
	c, err := e.component(req.Component)
	if err != nil {
		return nil, err
	}
	svc, err := e.checkPins(ctx, c, "install", true)
	if err != nil {
		return nil, err
	}
	if e.ctl == nil {
		return nil, errors.New("install needs a running updater")
	}
	dir := e.stagingDir(c.Name, req.Version)
	m, data, sig, err := e.verifyStaged(c.Name, dir)
	if err != nil {
		return nil, err
	}
	if m.Version != req.Version {
		return nil, refuse("staged version is %s, not %s", m.Version, req.Version)
	}
	if err := e.acquire(c.Name, req.Force); err != nil {
		return nil, err
	}
	unlock := e.lockComp(c.Name)
	defer unlock()
	// The component's settings are read again under its lock: a reload may have
	// changed them while this call waited.
	if c, err = e.component(req.Component); err != nil {
		e.release(req.Component)
		return nil, err
	}
	var rec *state.Record
	// What the record holds is brought up to date before the install mutex is
	// let go, so there is no moment when neither the call nor the record holds it.
	defer func() {
		e.syncHolder(c.Name, rec)
		e.release(c.Name)
	}()
	if req.DryRun {
		defer clearDir(dir) // the last defer runs first: before the lock and the mutex are let go
	}

	rec, err = e.load(c.Name)
	if err != nil {
		return nil, err
	}
	if !rec.KnownPhase() {
		return nil, refuse("component %s: state.json has an unknown phase %q; hands off", c.Name, rec.Phase)
	}
	switch {
	case rec.Phase == state.Installing:
		return nil, refuse("component %s is being installed", c.Name)
	case rec.Phase == state.Probation:
		return nil, refuse("component %s is %s: wait for it to settle, or revert it", c.Name, rec.Phase)
	case rec.UnfinishedRevert():
		// --force does not pass it: the revert is finished by the watcher (or at
		// the next start), never overwritten by an install.
		return nil, refuse("component %s has an unfinished revert: wait for it to finish; --force does not skip it (if its service is gone, add explicit targets to it in the config and reload, so the watcher can finish it; a target must have the name the record uses (normally the component name) and point at the file the service ran)", c.Name)
	case rec.WatchingRestored() && !req.Force:
		return nil, refuse("component %s is waiting for a restart after a revert: wait for it, or use --force", c.Name)
	case rec.Phase == state.Failed && !req.Force:
		return nil, refuse("component %s is failed (%s): fix it by hand, then run `install --force`, or `install --force` a release", c.Name, rec.LastError)
	}
	p, err := e.plan(ctx, c, m, data, sig, dir, svc, rec, req)
	if err != nil {
		return nil, err
	}
	res := &InstallResult{Component: c.Name, Version: m.Version, Targets: p.targets}
	if rec.Installed != nil {
		res.Replaces = rec.Installed.Version
	}
	if req.DryRun {
		res.Phase, res.Message = rec.Phase, "dry run: the install would proceed"
		return res, nil
	}
	if err := e.run(ctx, p, rec); err != nil {
		return nil, err
	}
	res.Phase, res.Message = state.Probation, "restart requested; the new version is on probation"
	clearDir(dir)
	return res, nil
}

// plan decides, from config, state and the manifest, whether the install may
// proceed, and gathers what it needs. It changes nothing but the drift flag.
func (e *Engine) plan(ctx context.Context, c *uconfig.Comp, m *manifest.Manifest, data, sig []byte, dir string, svc *Service, rec *state.Record, req InstallRequest) (*plan, error) {
	files := m.Platforms[Platform].Files
	if len(files) == 0 {
		return nil, refuse("version %s has no build for %s", m.Version, Platform)
	}
	if svc.ExeError != "" || svc.Exe == "" {
		return nil, refuse("%s", exeProblem(svc))
	}
	targets, err := e.targetsFor(c, svc)
	if err != nil {
		return nil, err
	}
	if got, want := fmt.Sprint(m.Targets()), fmt.Sprint(sortedNames(targets)); got != want {
		return nil, refuse("the manifest's targets %s are not the configured targets %s", got, want)
	}
	for _, t := range sortedNames(targets) {
		if err := uconfig.CheckTargetPath(targets[t], os.Geteuid()); err != nil {
			return nil, refuse("target %s: %v", t, err)
		}
		if _, err := os.Lstat(targets[t]); err != nil {
			return nil, refuse("target %s: %v (an install replaces an existing file)", t, err)
		}
	}
	if !exeMatches(svc, targets) {
		return nil, refuse("Warren runs another file (%s) than the one you maintain", svc.Exe)
	}
	if !req.Force && !svc.Lease.Held {
		return nil, refuse("service %s holds no lease now (it was not healthy before); fix it, or use --force", c.Service)
	}
	// Drift: a binary replaced by hand.
	if rec.Installed != nil && rec.Phase != state.Probation {
		if cur, err := hashTargets(targets); err == nil && !sameFiles(cur, rec.Installed.Files) && rec.NeedsManual == "" {
			rec.NeedsManual = state.Drift
			_ = e.save(rec)
		}
	}
	if !req.Force && rec.IsBad(m.Version) {
		return nil, refuse("version %s is marked bad; use --force to install it anyway", m.Version)
	}
	if rec.Installed != nil && !req.AllowDowngrade {
		// The installed version must be exceeded; the floor a revert kept may be
		// met again (it can be a version that never ran, whose install was
		// aborted), but not gone below.
		for _, f := range []struct {
			version string
			equalOK bool
			what    string
		}{{rec.Installed.Version, false, "the installed version"}, {rec.Installed.Floor, true, "the floor a revert kept"}} {
			if f.version == "" {
				continue
			}
			cmp, err := semver.CompareStrings(m.Version, f.version)
			if err != nil {
				return nil, refuse("installed version %q: %v", f.version, err)
			}
			if cmp < 0 || (cmp == 0 && !f.equalOK) {
				return nil, refuse("version %s is not newer than %s, %s (use --allow-downgrade)", m.Version, f.version, f.what)
			}
		}
	}
	for _, t := range sortedKeys(files) {
		if fi, err := os.Lstat(filepath.Join(dir, files[t].SHA256)); err != nil || !fi.Mode().IsRegular() || fi.Size() != files[t].Size {
			return nil, refuse("file for target %s is not staged", t)
		}
	}
	if err := checkSpace(targets, files); err != nil {
		return nil, err
	}
	return &plan{comp: c, m: m, data: data, sig: sig, files: files, targets: targets, svc: svc, dir: dir, manifSHA: sha256hex(data)}, nil
}

// checkSpace is the sum of the new files and copies of the old ones on each
// target's file system, plus 10 percent.
func checkSpace(targets map[string]string, files map[string]manifest.File) error {
	need := map[string]uint64{}
	for t, p := range targets {
		dir := filepath.Dir(p)
		need[dir] += uint64(files[t].Size)
		if fi, err := os.Stat(p); err == nil {
			need[dir] += uint64(fi.Size()) // the .prev copy where links are unavailable
		}
	}
	for dir, n := range need {
		free, err := freeBytes(dir)
		if err != nil {
			continue
		}
		if n+n/10 > free {
			return refuse("not enough space in %s: need %d bytes, %d available", dir, n+n/10, free)
		}
	}
	return nil
}

// run is the install proper after planning. An error after the
// record was written is handled here (the install is aborted and undone) or
// left to the watcher, which recovers an `installing` record that no live
// install owns.
func (e *Engine) run(ctx context.Context, p *plan, rec *state.Record) error {
	c, m := p.comp, p.m
	names := sortedNames(p.targets)
	cfg := e.config()
	prevFiles, err := hashTargets(p.targets)
	if err != nil {
		return err
	}
	// The baseline of the crash counting is taken now, at step 1, from the
	// service as it is, not from what planning saw some time ago.
	base, err := e.ctl.Service(ctx, c.Service)
	if err != nil {
		return refuse("service %s: %v", c.Service, err)
	}
	newFiles := map[string]string{}
	for t, f := range p.files {
		newFiles[t] = f.SHA256
	}
	prevVersion := ""
	if rec.Installed != nil && sameFiles(prevFiles, rec.Installed.Files) {
		prevVersion = rec.Installed.Version
	}
	now := time.Now().UTC()
	deadline := now.Add(cfg.HealthTimeout)
	// Step 1: the record, before any file changes.
	rec.Phase = state.Installing
	rec.Installed = &state.Installed{Version: m.Version, Channel: m.Channel, ManifestSHA256: p.manifSHA, Files: newFiles, InstalledAt: now}
	rec.Prev = &state.Prev{Version: prevVersion, Files: prevFiles}
	rec.Request = nil
	rec.Deadline, rec.HealthySince, rec.CanaryEnds = &deadline, nil, nil
	rec.Failures = []time.Time{}
	rec.Watch = state.Watch{Boot: base.Boot, UnrequestedTotal: base.Unrequested()}
	rec.LastError = ""
	if err := e.save(rec); err != nil {
		return err
	}
	if err := e.at("install-recorded"); err != nil {
		return err
	}
	swapped := false
	fail := func(cause error) error { return e.abort(ctx, c, rec, p.targets, cause, swapped) }
	// Step 2: stage next to each target, with the old file hard-linked as .prev.new.
	for _, t := range names {
		f := p.files[t]
		src, err := os.Open(filepath.Join(p.dir, f.SHA256))
		if err != nil {
			return fail(err)
		}
		err = writeNew(p.targets[t], src, f.Size, f.SHA256)
		src.Close()
		if err != nil {
			return fail(err)
		}
		if err := linkPrev(p.targets[t]); err != nil {
			return fail(err)
		}
		if err := e.at("prepared-" + t); err != nil {
			return err
		}
	}
	for dir := range dirsOf(p.targets) {
		if err := state.SyncDir(dir); err != nil {
			return fail(err)
		}
	}
	// Step 3: look again, with the keys in force now: the signature, the pins,
	// the targets and the exe.
	if cur, err := e.component(c.Name); err != nil {
		return fail(err)
	} else if _, err := manifest.Verify(p.data, p.sig, c.Name, cur.Keys, cur.MaxSize); err != nil {
		return fail(refuse("manifest refused at the swap: %v", err))
	} else if _, err := e.checkPins(ctx, cur, "install", true); err != nil {
		return fail(err)
	}
	svc, err := e.ctl.Service(ctx, c.Service)
	if err != nil {
		return fail(refuse("service %s: %v", c.Service, err))
	}
	if !exeMatches(svc, p.targets) {
		return fail(refuse("Warren now runs another file (%s) than the one you maintain", svc.Exe))
	}
	for _, t := range names {
		if err := uconfig.CheckTargetPath(p.targets[t], os.Geteuid()); err != nil {
			return fail(refuse("target %s: %v", t, err))
		}
		if now, err := hashFile(p.targets[t]); err != nil || now != prevFiles[t] {
			return fail(refuse("target %s changed while the install was prepared", t))
		}
	}
	if err := e.at("verified"); err != nil {
		return err
	}
	// Step 4: swap. Both renames are atomic and the target is never missing.
	for _, t := range names {
		swapped = true // a process may be started from the new file from here on
		if err := os.Rename(p.targets[t]+".prev.new", p.targets[t]+".prev"); err != nil {
			return fail(err)
		}
		if err := os.Rename(p.targets[t]+".new", p.targets[t]); err != nil {
			return fail(err)
		}
		if err := e.at("swapped-" + t); err != nil {
			return err
		}
	}
	for dir := range dirsOf(p.targets) {
		_ = state.SyncDir(dir)
	}
	// Step 5: probation, then the restart request.
	rec.Phase = state.Probation
	rec.Request = &state.Request{At: time.Now().UTC()}
	if err := e.save(rec); err != nil {
		return fail(err) // nothing was restarted yet: undo the swap
	}
	if err := e.at("probation-recorded"); err != nil {
		return err
	}
	if e.requestRestart(ctx, c, rec) {
		if err := e.save(rec); err != nil {
			return err // the record says probation with the request unsent: the watcher sends it again
		}
	} else {
		e.log.Warn("restart not accepted yet; the watcher sends it again", "component", c.Name)
	}
	return e.at("restart-recorded")
}

// requestRestart asks Warren to restart the service and records min_start_seq
// together with the boot it was given in: the boot is read before and after the
// request, and when it changed in between the request is not recorded (it is
// sent again, so the pair is coherent). It returns whether rec.Request was
// updated; the caller saves.
func (e *Engine) requestRestart(ctx context.Context, c *uconfig.Comp, rec *state.Record) bool {
	if rec.Request == nil {
		return false
	}
	s0, err := e.ctl.Service(ctx, c.Service)
	if err != nil {
		return false
	}
	min, err := e.ctl.Restart(ctx, c.Service)
	if err != nil {
		return false
	}
	s1, err := e.ctl.Service(ctx, c.Service)
	if err != nil || s1.Boot != s0.Boot {
		return false
	}
	if rec.Watch.Boot != s0.Boot {
		rec.Watch = state.Watch{Boot: s0.Boot, UnrequestedTotal: s0.Unrequested()}
	}
	rec.Request.MinStartSeq = &min
	return true
}

func dirsOf(targets map[string]string) map[string]bool {
	out := map[string]bool{}
	for _, p := range targets {
		out[filepath.Dir(p)] = true
	}
	return out
}

// abort ends an install that failed before the restart request: the verdict
// is `reverted`, not bad, and what was prepared or swapped is undone by the hash
// rule. The service is restarted only when the swap had begun, since a process
// started from then on (Warren restarting it after a crash) may already run the
// new file; before that nothing ran from it.
func (e *Engine) abort(ctx context.Context, c *uconfig.Comp, rec *state.Record, targets map[string]string, cause error, swapped bool) error {
	if errors.Is(cause, ErrInjected) {
		return cause
	}
	e.log.Warn("install failed; reverting", "component", c.Name, "err", cause)
	defer cleanLeftovers(targets)
	if err := e.revertLocked(ctx, c, rec, revertOpts{reason: cause.Error(), restart: swapped, targets: targets}); err != nil {
		return fmt.Errorf("%w (and the revert failed: %v)", cause, err)
	}
	return cause
}

func sha256hex(b []byte) string {
	return fmt.Sprintf("%x", sum256(b))
}
