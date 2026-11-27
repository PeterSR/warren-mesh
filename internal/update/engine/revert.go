package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/PeterSR/warren-mesh/internal/update/semver"
	"github.com/PeterSR/warren-mesh/internal/update/state"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
)

// revertOpts say why and how a revert runs.
type revertOpts struct {
	reason string
	// bad is the reason to mark the version reverted away bad with, or "".
	bad string
	// restart arms the watch of the restored version and asks Warren to
	// restart the service. It is false for an install that failed before the
	// swap began (nothing ran from the new files) and for the offline revert,
	// which touches no process.
	restart bool
	// targets, if set, are the paths already resolved by the caller.
	targets map[string]string
}

// resolveTargets finds a component's targets, asking Warren for the service's
// exe when the config has none.
func (e *Engine) resolveTargets(ctx context.Context, c *uconfig.Comp) (map[string]string, error) {
	var svc *Service
	if len(c.Targets) == 0 && e.ctl != nil {
		if s, err := e.ctl.Service(ctx, c.Service); err == nil {
			svc = s
		}
	}
	return e.targetsFor(c, svc)
}

// revertLocked is a revert of a service component (docs/updates.md, "Revert"): the targets
// are resolved first (a revert that cannot find its files is refused before
// anything is written), then the verdict is written, then the files are put
// back by the hash rule, then the bookkeeping, then the restart. The caller
// holds the component lock.
func (e *Engine) revertLocked(ctx context.Context, c *uconfig.Comp, rec *state.Record, o revertOpts) error {
	targets := o.targets
	if targets == nil {
		var err error
		if targets, err = e.resolveTargets(ctx, c); err != nil {
			return err
		}
	}
	// 1. The verdict, before any file is renamed. installed and prev stay: the
	// revert is now unfinished (a reverted record with a non-empty prev).
	rec.Phase = state.Reverted
	if o.bad != "" && rec.Installed != nil && !rec.IsBad(rec.Installed.Version) {
		rec.Bad = append(rec.Bad, state.Bad{Version: rec.Installed.Version, Reason: o.bad, At: time.Now().UTC()})
	}
	rec.LastError = o.reason
	rec.Deadline, rec.HealthySince, rec.CanaryEnds = nil, nil, nil
	if err := e.save(rec); err != nil {
		return err
	}
	if err := e.at("revert-verdict"); err != nil {
		return err
	}
	return e.finishRevertLocked(ctx, c, rec, targets, o.restart)
}

// finishRevertLocked completes an unfinished revert and is the one code path
// that undoes an interrupted install, finishes an interrupted revert and does
// a manual one. It decides only by hashing, so it is safe to run any number of
// times. The targets were resolved by the caller.
func (e *Engine) finishRevertLocked(ctx context.Context, c *uconfig.Comp, rec *state.Record, targets map[string]string, restart bool) error {
	if !rec.UnfinishedRevert() {
		return nil
	}
	// A target that equals prev is left alone (this test comes first); one that
	// equals the new version gets .prev renamed back over it; one that equals
	// neither, or a wrong .prev, makes the component failed.
	for _, t := range sortedKeys(rec.Prev.Files) {
		path, ok := targets[t]
		if !ok {
			return e.failRevert(rec, fmt.Errorf("target %s is not configured", t))
		}
		cur, err := hashFile(path)
		switch {
		case err != nil:
			return e.failRevert(rec, fmt.Errorf("target %s: %v", t, err))
		case cur == rec.Prev.Files[t]:
		case rec.Installed != nil && cur == rec.Installed.Files[t]:
			if err := restorePrev(path, rec.Prev.Files[t]); err != nil {
				return e.failRevert(rec, fmt.Errorf("target %s: cannot restore: %v", t, err))
			}
		default:
			return e.failRevert(rec, fmt.Errorf("target %s matches neither the new nor the previous version", t))
		}
		if err := e.at("reverted-" + t); err != nil {
			return err
		}
	}
	cleanLeftovers(targets)
	// Bookkeeping: installed := prev, prev emptied. For a service the watch of
	// the restored version is armed in the same write, before the restart, so a
	// process killed by its own request never leaves the record unfinished. The
	// boot is recorded with the request, so a Warren restart after this point
	// can be told from one before it.
	restored := &state.Installed{Version: rec.Prev.Version, Files: rec.Prev.Files, InstalledAt: time.Now().UTC()}
	if rec.Installed != nil {
		restored.Floor = rec.Installed.Floor
		// The version before is not known: the version reverted away keeps the
		// no-downgrade floor, or a revert would reopen every older version.
		if rec.Prev.Version == "" && rec.Installed.Version != "" {
			restored.Floor = higher(restored.Floor, rec.Installed.Version)
		}
	}
	rec.Installed = restored
	rec.Prev = nil
	rec.HealthySince, rec.CanaryEnds, rec.Deadline = nil, nil, nil
	rec.Watch = state.Watch{}
	if restart {
		rec.Request = &state.Request{At: time.Now().UTC()}
		if e.ctl != nil {
			if s, err := e.ctl.Service(ctx, c.Service); err == nil {
				rec.Watch = state.Watch{Boot: s.Boot, UnrequestedTotal: s.Unrequested()}
			}
		}
	} else {
		rec.Request = nil
	}
	if err := e.save(rec); err != nil {
		return err
	}
	if err := e.at("revert-bookkeeping"); err != nil {
		return err
	}
	if restart && e.ctl != nil {
		// A 404 or an unreachable Warren is not "done": the watcher retries.
		if e.requestRestart(ctx, c, rec) {
			if err := e.save(rec); err != nil {
				return err
			}
		} else {
			e.log.Info("restart after the revert is pending", "component", c.Name)
		}
	}
	return e.at("revert-restarted")
}

func (e *Engine) failRevert(rec *state.Record, cause error) error {
	if errors.Is(cause, ErrInjected) {
		return cause
	}
	rec.Phase = state.Failed
	rec.LastError = "revert failed: " + cause.Error()
	rec.Deadline = nil
	if err := e.save(rec); err != nil {
		return err
	}
	return cause
}

// Revert is the operator's `warren-update revert <component>`: the verdict is
// written first, with the version reverted away marked bad (reason manual) when
// it was committed. A failed or settled component is refused.
func (e *Engine) Revert(ctx context.Context, name string) (*InstallResult, error) {
	c, err := e.component(name)
	if err != nil {
		return nil, err
	}
	if _, err := e.checkPins(ctx, c, "revert", false); err != nil {
		return nil, err
	}
	unlock := e.lockComp(name)
	defer unlock()
	rec, err := e.load(name)
	if err != nil {
		return nil, err
	}
	if err := e.checkRevertable(rec, name); err != nil {
		return nil, err
	}
	from := rec.Installed
	bad := ""
	if rec.Phase == state.Committed {
		bad = state.Manual
	}
	if rec.UnfinishedRevert() {
		var targets map[string]string
		if targets, err = e.resolveTargets(ctx, c); err == nil {
			err = e.finishRevertLocked(ctx, c, rec, targets, true)
		}
	} else {
		err = e.revertLocked(ctx, c, rec, revertOpts{reason: "manual revert", bad: bad, restart: true})
	}
	e.syncHolder(name, rec)
	if err != nil {
		return nil, err
	}
	out := &InstallResult{Component: name, Phase: rec.Phase, Message: "reverted; the previous version is being restarted"}
	if from != nil {
		out.Replaces = from.Version
	}
	if rec.Installed != nil {
		out.Version = rec.Installed.Version
	}
	return out, nil
}

// checkRevertable applies the refusals of revert.
func (e *Engine) checkRevertable(rec *state.Record, name string) error {
	switch {
	case !rec.KnownPhase():
		return refuse("component %s: state.json has an unknown phase %q; hands off", name, rec.Phase)
	case rec.Phase == state.Failed:
		return refuse("component %s is failed: fix it by hand, then run `install --force`, or `install --force` a release", name)
	case rec.Phase == state.Installing:
		return refuse("component %s is being installed; wait for it", name)
	case rec.UnfinishedRevert():
		return nil
	case rec.Prev == nil || len(rec.Prev.Files) == 0:
		return refuse("component %s has nothing to revert to (no previous version, or already reverted)", name)
	}
	return nil
}

// PreflightOffline is what `revert --offline` can refuse before it takes
// update.lock: an unconfigured or pinned component, and one without explicit
// targets (the default target is the service's exe, which only a running Warren
// reports).
func (e *Engine) PreflightOffline(name string) error {
	c, err := e.component(name)
	if err != nil {
		return err
	}
	if c.Pinned != "" {
		return refuse("revert refused: component %s is pinned: %s", name, c.Pinned)
	}
	if len(c.Targets) == 0 {
		return refuse("revert --offline needs explicit targets for component %s in the config: the default target is the service's exe, which only a running Warren reports", name)
	}
	return nil
}

// RevertOffline is `revert --offline`: with the update.lock held (so no daemon
// runs), swap the files back by the hash rule and touch no process. The
// operator restarts the service. With the lock held no install is live, so a
// record in `installing` is an orphan and is reverted as at start (docs/updates.md, "Recovery").
func (e *Engine) RevertOffline(name string) (*InstallResult, error) {
	if err := e.PreflightOffline(name); err != nil {
		return nil, err
	}
	c := e.config().Components[name]
	unlock := e.lockComp(name)
	defer unlock()
	rec, err := e.load(name)
	if err != nil {
		return nil, err
	}
	var from *state.Installed
	if rec.Phase == state.Installing {
		rec.Phase = state.Reverted
		rec.LastError = "interrupted install"
		rec.Deadline = nil
		if err := e.save(rec); err != nil {
			return nil, err
		}
	}
	if err := e.checkRevertable(rec, name); err != nil {
		return nil, err
	}
	from = rec.Installed
	bad := ""
	if rec.Phase == state.Committed {
		bad = state.Manual
	}
	if rec.UnfinishedRevert() {
		err = e.finishRevertLocked(context.Background(), c, rec, c.Targets, false)
	} else {
		err = e.revertLocked(context.Background(), c, rec, revertOpts{reason: "manual revert (offline)", bad: bad})
	}
	if err != nil {
		return nil, err
	}
	out := &InstallResult{Component: name, Phase: rec.Phase, Message: "reverted offline; restart the service to run the previous version"}
	if from != nil {
		out.Replaces = from.Version
	}
	if rec.Installed != nil {
		out.Version = rec.Installed.Version
	}
	return out, nil
}

// higher is the greater of two versions (either may be empty or invalid).
func higher(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	if c, err := semver.CompareStrings(a, b); err == nil && c >= 0 {
		return a
	}
	return b
}
