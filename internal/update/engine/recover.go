package engine

import (
	"context"
	"errors"
	"maps"
	"os"
	"time"

	"github.com/PeterSR/warren-mesh/internal/update/state"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
)

// Recover is the recovery at start (docs/updates.md, "Recovery") for service components. When `warren-update run`
// starts, before it takes the lease, and under update.lock, it:
//
//  1. removes <target>.new and <target>.prev.new of every configured target;
//  2. gives every record in phase `installing` the verdict `reverted` (written
//     first, not marked bad);
//  3. finishes every unfinished revert: a target equal to prev is left alone, a
//     target equal to the new version gets .prev renamed back, anything else
//     makes the phase failed; then the bookkeeping, and the restart is left
//     to the health watcher, which sends it until Warren accepts it;
//  4. flags drift: a target that no longer equals what was installed.
//
// It also creates the state-lost record of a component that has none, and
// derives the install mutex from the records.
func (e *Engine) Recover(ctx context.Context) error {
	cfg := e.config()
	var firstErr error
	for _, name := range cfg.Names() {
		if err := e.recoverOne(ctx, cfg.Components[name]); err != nil {
			e.log.Error("recovery", "component", name, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (e *Engine) recoverOne(ctx context.Context, c *uconfig.Comp) error {
	unlock := e.lockComp(c.Name)
	defer unlock()
	rec, err := e.store.Load(c.Name)
	if errors.Is(err, state.ErrNone) {
		rec = state.New(c.Name)
		rec.NeedsManual = state.StateLost
		if err := e.save(rec); err != nil {
			return err
		}
		e.log.Warn("no state for a configured component; it needs one manual install", "component", c.Name)
	} else if err != nil {
		return err
	}
	if !rec.KnownPhase() {
		e.log.Error("unknown phase in state.json; hands off", "component", c.Name, "phase", rec.Phase)
		return nil
	}
	targets, terr := e.resolveTargets(ctx, c)
	if terr == nil {
		cleanLeftovers(targets)
	}
	if err := e.at("recover-cleaned"); err != nil {
		return err
	}
	if err := e.settleInterrupted(ctx, c, rec); err != nil {
		return err
	}
	// Drift, only for a settled record whose files are meant to be on disk.
	if terr == nil && rec.Installed != nil && len(rec.Installed.Files) > 0 && (rec.Phase == state.Committed || (rec.Phase == state.Reverted && !rec.UnfinishedRevert())) && rec.NeedsManual == "" {
		if cur, err := hashTargets(targets); err == nil && !maps.Equal(cur, rec.Installed.Files) {
			rec.NeedsManual = state.Drift
			e.log.Warn("a binary was replaced by hand; the component needs one manual install", "component", c.Name)
			if err := e.save(rec); err != nil {
				return err
			}
		}
	}
	e.syncHolder(c.Name, rec)
	return nil
}

// settleInterrupted finishes what an updater that died, or an install that
// failed to record its own failure, left behind: a record in `installing` that
// no live install owns (the caller holds the component lock, which a live
// install holds throughout) becomes an unmarked `reverted`, and every
// unfinished revert is finished by the hash rule, with the restart that the
// watch of the restored version needs. If the targets cannot be resolved yet
// (the service's exe is not known), nothing is written and it is tried again.
func (e *Engine) settleInterrupted(ctx context.Context, c *uconfig.Comp, rec *state.Record) error {
	if rec.Phase != state.Installing && !rec.UnfinishedRevert() {
		return nil
	}
	targets, terr := e.resolveTargets(ctx, c)
	if rec.Phase == state.Installing {
		// The verdict is written whether or not the files can be found yet (as at start):
		// the record then holds only its own component, instead of
		// holding the whole node as an install in flight.
		rec.Phase = state.Reverted
		rec.LastError = "interrupted install"
		rec.Deadline = nil
		if err := e.save(rec); err != nil {
			return err
		}
		e.log.Warn("an install was interrupted; reverting it", "component", c.Name)
	}
	if terr != nil {
		e.warnEvery("targets|"+c.Name, "cannot finish a revert yet: the targets are unknown", "component", c.Name, "err", terr)
		return nil
	}
	return e.finishRevertLocked(ctx, c, rec, targets, true)
}

// warnEvery logs a warning at most once a minute for a key, for conditions the
// watcher meets at every poll.
func (e *Engine) warnEvery(key, msg string, args ...any) {
	e.mu.Lock()
	last, seen := e.warned[key]
	if seen && e.now().Sub(last) < time.Minute {
		e.mu.Unlock()
		return
	}
	e.warned[key] = e.now()
	e.mu.Unlock()
	e.log.Warn(msg, args...)
}

// StagingMaxAge is how old a staging directory must be to be removed at start.
var StagingMaxAge = 24 * time.Hour

// CleanOldStaging removes staging left by a crashed upload.
func (e *Engine) CleanOldStaging() {
	e.CleanStaging(func(fi os.FileInfo) bool { return e.now().Sub(fi.ModTime()) > StagingMaxAge })
}
