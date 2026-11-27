package engine

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/PeterSR/warren-mesh/internal/update/state"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
)

// Crash loop: this many unrequested failures within this window revert and
// mark the version bad.
const (
	CrashCount  = 3
	CrashWindow = 5 * time.Minute
)

// Watch runs the health watcher until ctx ends: every PollEvery it looks at
// every component on probation (or watching a restored version) and judges it
// from Warren's per-service status (docs/updates.md, "Healthy, crash loop, timeout" and "Revert").
func (e *Engine) Watch(ctx context.Context) {
	t := time.NewTicker(e.poll)
	defer t.Stop()
	for {
		e.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick is one pass of the watcher over every configured component.
func (e *Engine) Tick(ctx context.Context) {
	for _, name := range e.config().Names() {
		if ctx.Err() != nil {
			return
		}
		if err := e.watchOne(ctx, name); err != nil && !errors.Is(err, ErrInjected) {
			e.log.Warn("watching", "component", name, "err", err)
		}
	}
}

func (e *Engine) watchOne(ctx context.Context, name string) error {
	c := e.config().Components[name]
	if c == nil {
		return nil
	}
	unlock := e.lockComp(name)
	defer unlock()
	rec, err := e.store.Load(name)
	if err != nil {
		return nil
	}
	defer func() { e.syncHolder(name, rec) }()
	switch {
	case !rec.KnownPhase():
		return nil
	case rec.Phase == state.Installing, rec.UnfinishedRevert():
		// The component lock is held by a live install throughout, so here
		// nobody owns this record: it is settled like at start.
		return e.settleInterrupted(ctx, c, rec)
	case rec.Phase == state.Probation:
		return e.watchProbation(ctx, c, rec)
	case rec.WatchingRestored():
		return e.watchRestored(ctx, c, rec)
	}
	return nil
}

// observe reads the service and applies what is common to both watches: the
// absence window, the exe check, a new boot and a pending restart request. It
// returns the service when it can be judged, and nil when this poll counts
// nothing (the service is absent, runs another file, or Warren is unreachable).
func (e *Engine) observe(ctx context.Context, c *uconfig.Comp, rec *state.Record) (*Service, error) {
	svc, err := e.ctl.Service(ctx, c.Service)
	if errors.Is(err, ErrNotFound) {
		return nil, nil // a reload window: retry, count nothing
	}
	if err != nil {
		return nil, nil // Warren unreachable: it is shutting down, or we are
	}
	targets, terr := e.targetsFor(c, svc)
	if terr != nil || !exeMatches(svc, targets) {
		return nil, nil // a reload changed run: like absence
	}
	// A new boot: the counters start over. A request that was accepted in an
	// older boot is satisfied by any start of this one, which began from the
	// files already on disk. The boot is recorded whenever a request is accepted
	// (watch.boot always is the boot of the request), so a request accepted in
	// this very boot is never mistaken for an old one. A request not accepted
	// yet has nothing to be taken over.
	if rec.Watch.Boot != svc.Boot {
		if rec.Watch.Boot != "" {
			if rec.Request != nil && rec.Request.MinStartSeq != nil {
				one := 1
				rec.Request.MinStartSeq = &one
			}
			if rec.Deadline != nil {
				if d, extend := e.deadlineAfterBoot(c, rec, e.now().UTC()); extend {
					rec.Deadline = &d
				}
			}
			e.settleReset(c.Name)
		}
		rec.Watch = state.Watch{Boot: svc.Boot, UnrequestedTotal: svc.Unrequested()}
		if err := e.save(rec); err != nil {
			return nil, err
		}
	}
	// A restart request Warren has not accepted yet is sent again.
	if rec.Request != nil && rec.Request.MinStartSeq == nil {
		if !e.requestRestart(ctx, c, rec) {
			return nil, nil
		}
		if err := e.save(rec); err != nil {
			return nil, err
		}
		if err := e.at("watch-restart-recorded"); err != nil {
			return nil, err
		}
	}
	return svc, nil
}

func ptrTime(t time.Time) *time.Time { return &t }

// deadlineAfterBoot is what a new boot does to an armed deadline. The new boot
// starts the service afresh, so while the anchor (the install time of a
// probation; for a restored watch the proof time, which is stored in
// request.at once the proof has held, the revert request time before that) is
// less than three health timeouts old the deadline moves to one settle window
// plus the health timeout from now: a late boot still gets a whole window. After
// that it is left alone, so a unit that keeps restarting cannot keep a broken
// version on probation for ever: the worst case is the anchor plus four health
// timeouts plus a settle window.
func (e *Engine) deadlineAfterBoot(c *uconfig.Comp, rec *state.Record, now time.Time) (time.Time, bool) {
	ht := e.config().HealthTimeout
	anchor := now
	switch {
	case rec.Phase == state.Reverted && rec.Request != nil:
		anchor = rec.Request.At
	case rec.Installed != nil:
		anchor = rec.Installed.InstalledAt
	}
	if !now.Before(anchor.Add(3 * ht)) {
		return time.Time{}, false
	}
	return now.Add(c.Settle + ht), true
}

func (e *Engine) settleReset(name string) {
	e.mu.Lock()
	delete(e.clock, name)
	e.mu.Unlock()
}

// settled advances the settle clock and says whether the service has held the
// same lease, unchanged, for the settle time since its latest start. It reads
// only start_seq, lease.epoch and lease.held.
func (e *Engine) settled(c *uconfig.Comp, svc *Service, started bool, now time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	sc := e.clock[c.Name]
	if !started || !svc.Lease.Held {
		delete(e.clock, c.Name)
		return false
	}
	if sc == nil || sc.startSeq != svc.StartSeq || sc.epoch != svc.Lease.Epoch {
		sc = &settleClock{since: now, startSeq: svc.StartSeq, epoch: svc.Lease.Epoch}
		e.clock[c.Name] = sc
	}
	return now.Sub(sc.since) >= c.Settle
}

// started is the proof that the new (or restored) file runs: the service is
// running and its start_seq reached request.min_start_seq, which was taken
// under Warren's process lock after the swap.
func started(svc *Service, rec *state.Record) bool {
	return svc.State == "running" && rec.Request != nil && rec.Request.MinStartSeq != nil && svc.StartSeq >= *rec.Request.MinStartSeq
}

func (e *Engine) watchProbation(ctx context.Context, c *uconfig.Comp, rec *state.Record) error {
	now := e.now().UTC()
	svc, err := e.observe(ctx, c, rec)
	if err != nil {
		return err
	}
	if svc != nil {
		// Crash counting: each increase of exits - requested_exits + start_failures
		// is a failure, stamped now.
		if total := svc.Unrequested(); total != rec.Watch.UnrequestedTotal {
			for i := rec.Watch.UnrequestedTotal; i < total; i++ {
				rec.Failures = append(rec.Failures, now)
			}
			rec.Watch.UnrequestedTotal = total
			if err := e.save(rec); err != nil {
				return err
			}
		}
		if crashLoop(rec.Failures) {
			e.log.Warn("crash loop on probation; reverting", "component", c.Name)
			return e.revertLocked(ctx, c, rec, revertOpts{reason: "crash loop: " + strconv.Itoa(CrashCount) + " failures within " + CrashWindow.String(), bad: state.CrashLoop, restart: true})
		}
	}
	// Settled is judged before the deadline in the same poll: a service that
	// has just held its lease for the settle time is healthy, however close the
	// deadline is.
	if svc != nil {
		if ok := e.settled(c, svc, started(svc, rec), now); rec.HealthySince == nil && ok {
			rec.HealthySince = &now
			ends := now.Add(e.config().CanaryFor(c))
			rec.CanaryEnds = &ends
			rec.Deadline = nil
			e.log.Info("healthy", "component", c.Name, "canary_ends", ends)
			return e.save(rec) // the install mutex ends at first healthy: the record no longer holds it
		}
	}
	if rec.HealthySince == nil && rec.Deadline != nil && now.After(*rec.Deadline) {
		e.log.Warn("not healthy in time; reverting", "component", c.Name)
		return e.revertLocked(ctx, c, rec, revertOpts{reason: "not healthy before the deadline", restart: true})
	}
	if svc != nil && rec.HealthySince != nil && rec.CanaryEnds != nil && !now.Before(*rec.CanaryEnds) {
		rec.Phase = state.Committed
		rec.NeedsManual = ""
		rec.CanaryEnds = nil
		e.log.Info("committed", "component", c.Name)
		return e.save(rec)
	}
	return nil
}

// crashLoop is whether CrashCount failures fall within CrashWindow.
func crashLoop(failures []time.Time) bool {
	if len(failures) < CrashCount {
		return false
	}
	f := append([]time.Time(nil), failures...)
	sort.Slice(f, func(i, j int) bool { return f[i].Before(f[j]) })
	for i := 0; i+CrashCount-1 < len(f); i++ {
		if f[i+CrashCount-1].Sub(f[i]) <= CrashWindow {
			return true
		}
	}
	return false
}

// watchRestored watches the restored version after a revert, with no further
// revert (docs/updates.md, "Revert"): its deadline is armed only once the proof holds,
// and not healthy by then makes the component failed.
func (e *Engine) watchRestored(ctx context.Context, c *uconfig.Comp, rec *state.Record) error {
	now := e.now().UTC()
	svc, err := e.observe(ctx, c, rec)
	if err != nil {
		return err
	}
	if svc != nil {
		proved := started(svc, rec)
		if rec.Deadline == nil && proved {
			// From the moment the proof first holds: one settle window to settle,
			// then the health timeout. Never a deadline that has already passed.
			// The proof is also the anchor that a new boot's extension is
			// bounded from, so the request time becomes the proof time in the
			// same write (nothing else reads it).
			rec.Deadline = ptrTime(now.Add(c.Settle + e.config().HealthTimeout))
			rec.Request.At = now
			if err := e.save(rec); err != nil {
				return err
			}
		}
		// Settled before the deadline, in the same poll.
		if e.settled(c, svc, proved, now) {
			rec.HealthySince = &now
			rec.Deadline = nil
			e.log.Info("the restored version is healthy", "component", c.Name)
			return e.save(rec)
		}
	}
	if rec.Deadline != nil && rec.HealthySince == nil && now.After(*rec.Deadline) {
		rec.Phase = state.Failed
		rec.LastError = "the restored version did not become healthy in time"
		rec.Deadline = nil
		e.log.Error("restored version not healthy; failed", "component", c.Name)
		return e.save(rec)
	}
	return nil
}
