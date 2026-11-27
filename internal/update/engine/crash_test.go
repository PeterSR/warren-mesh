package engine

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/PeterSR/warren-mesh/internal/update/state"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
)

// crashAt returns a hook that simulates a kill the first time point is reached.
func crashAt(point string) func(string) error {
	fired := false
	return func(p string) error {
		if p == point && !fired {
			fired = true
			return ErrInjected
		}
		return nil
	}
}

func noLeftovers(t *testing.T, r *rig) {
	t.Helper()
	for _, s := range []string{".new", ".prev.new"} {
		if _, err := os.Stat(r.target + s); err == nil {
			t.Errorf("%s left behind", s)
		}
	}
}

// A crash at any step of an install: the swap is undone when it had not reached
// probation, and the probation carries on when it had.
func TestCrashAtEveryStepOfAnInstall(t *testing.T) {
	undone := []string{"install-recorded", "prepared-myapp", "verified", "swapped-myapp"}
	for _, point := range undone {
		r := newRig(t)
		r.e = r.engine(crashAt(point))
		_, err := r.install("1.0.0-dev.1", "new binary")
		if !errors.Is(err, ErrInjected) {
			t.Fatalf("%s: %v", point, err)
		}
		if point == "swapped-myapp" && r.content() != "new binary" {
			t.Fatalf("%s: the crash was not after the swap", point)
		}
		// The updater starts again: recovery undoes the half install.
		r.e = r.engine(nil)
		if err := r.e.Recover(context.Background()); err != nil {
			t.Fatalf("%s: recover: %v", point, err)
		}
		rec := r.record()
		if r.content() != "old binary" || rec.Phase != state.Reverted || len(rec.Bad) != 0 || rec.UnfinishedRevert() {
			t.Errorf("%s: content %q record %+v", point, r.content(), rec)
		}
		if rec.Installed.Files["myapp"] != sha256hex([]byte("old binary")) || rec.Prev != nil {
			t.Errorf("%s: bookkeeping %+v %+v", point, rec.Installed, rec.Prev)
		}
		noLeftovers(t, r)
		// The restored version is restarted by the watcher and proven healthy.
		r.until(point+": restored healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
	}
	for _, point := range []string{"probation-recorded", "restart-recorded"} {
		r := newRig(t)
		r.e = r.engine(crashAt(point))
		if _, err := r.install("1.0.0-dev.1", "new binary"); !errors.Is(err, ErrInjected) {
			t.Fatalf("%s: %v", point, err)
		}
		r.e = r.engine(nil)
		if err := r.e.Recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		if rec := r.record(); rec.Phase != state.Probation || r.content() != "new binary" {
			t.Fatalf("%s: record %+v content %q", point, rec, r.content())
		}
		if r.e.busyName() != "myapp" {
			t.Errorf("%s: the install mutex was not derived from the record", point)
		}
		r.until(point+": committed", phaseIs(state.Committed))
		if r.content() != "new binary" {
			t.Errorf("%s: %q", point, r.content())
		}
		if r.e.busyName() != "" {
			t.Errorf("%s: mutex held after commit", point)
		}
	}
}

// A crash at any step of a revert, including between the verdict, the rename
// and the restart: the recovery finishes it.
func TestCrashAtEveryStepOfARevert(t *testing.T) {
	for _, point := range []string{"revert-verdict", "reverted-myapp", "revert-bookkeeping", "revert-restarted"} {
		r := newRig(t)
		mustInstall(t, r, "1.0.0-dev.1", "new binary")
		r.until("committed", phaseIs(state.Committed))
		r.e = r.engine(crashAt(point))
		if _, err := r.e.Revert(context.Background(), "myapp"); !errors.Is(err, ErrInjected) {
			t.Fatalf("%s: %v", point, err)
		}
		switch point {
		case "revert-verdict":
			if rec := r.record(); !rec.UnfinishedRevert() || r.content() != "new binary" {
				t.Fatalf("%s: verdict first: %+v %q", point, rec, r.content())
			}
		case "reverted-myapp":
			if rec := r.record(); !rec.UnfinishedRevert() || r.content() != "old binary" {
				t.Fatalf("%s: renamed before the bookkeeping: %+v %q", point, rec, r.content())
			}
		}
		r.e = r.engine(nil)
		if err := r.e.Recover(context.Background()); err != nil {
			t.Fatalf("%s: %v", point, err)
		}
		rec := r.record()
		if r.content() != "old binary" || rec.Phase != state.Reverted || rec.UnfinishedRevert() || rec.Prev != nil {
			t.Errorf("%s: content %q record %+v", point, r.content(), rec)
		}
		if rec.Installed.Files["myapp"] != sha256hex([]byte("old binary")) {
			t.Errorf("%s: installed %+v", point, rec.Installed)
		}
		if len(rec.Bad) != 1 || rec.Bad[0].Reason != state.Manual {
			t.Errorf("%s: bad %+v", point, rec.Bad)
		}
		r.until(point+": restored healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
		// Running recovery again changes nothing and never loops.
		before := r.fw.restartCount()
		if err := r.e.Recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		if r.fw.restartCount() != before {
			t.Errorf("%s: a second recovery restarted the service", point)
		}
	}
}

// The same for a revert the watcher decided on: a crash loop.
func TestCrashDuringAWatcherRevert(t *testing.T) {
	for _, point := range []string{"revert-verdict", "reverted-myapp", "revert-bookkeeping", "revert-restarted"} {
		r := newRig(t)
		mustInstall(t, r, "1.0.0-dev.1", "new binary")
		r.e.Tick(context.Background())
		r.e = r.engine(crashAt(point))
		for i := 0; i < 3; i++ {
			r.fw.with("myapp", crash)
		}
		r.e.Tick(context.Background())
		if rec := r.record(); rec.Phase != state.Reverted {
			t.Fatalf("%s: the watcher did not start the revert: %+v", point, rec)
		}
		r.e = r.engine(nil)
		if err := r.e.Recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		rec := r.record()
		if r.content() != "old binary" || rec.UnfinishedRevert() || !rec.IsBad("1.0.0-dev.1") {
			t.Errorf("%s: content %q record %+v", point, r.content(), rec)
		}
		r.until(point+": restored healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
	}
}

// A target that equals neither version makes the component failed, never a guess.
func TestRecoveryFailsWhenATargetMatchesNeither(t *testing.T) {
	r := newRig(t)
	r.e = r.engine(crashAt("swapped-myapp"))
	r.install("1.0.0-dev.1", "new binary")
	os.WriteFile(r.target, []byte("somebody else"), 0o755)
	r.e = r.engine(nil)
	r.e.Recover(context.Background())
	if rec := r.record(); rec.Phase != state.Failed || r.content() != "somebody else" {
		t.Errorf("record %+v content %q", rec, r.content())
	}
}

func TestRecoveryFailsWhenPrevIsMissingOrWrong(t *testing.T) {
	for name, damage := range map[string]func(p string){
		"missing": func(p string) { os.Remove(p) },
		"wrong":   func(p string) { os.Remove(p); os.WriteFile(p, []byte("not the old one"), 0o755) },
		"symlink": func(p string) { os.Remove(p); os.Symlink("/etc/passwd", p) },
		"dir":     func(p string) { os.Remove(p); os.Mkdir(p, 0o755) },
	} {
		r := newRig(t)
		r.e = r.engine(crashAt("swapped-myapp"))
		r.install("1.0.0-dev.1", "new binary")
		damage(r.target + ".prev")
		r.e = r.engine(nil)
		r.e.Recover(context.Background())
		if rec := r.record(); rec.Phase != state.Failed || r.content() != "new binary" {
			t.Errorf("%s: record %+v content %q", name, rec, r.content())
		}
	}
}

// A failure (not a crash) after the files were prepared aborts the install
// cleanly: reverted, not bad, nothing left behind, no restart.
func TestInstallAbortsWhenTheExeChangesBeforeTheSwap(t *testing.T) {
	r := newRig(t)
	r.e = r.engine(func(p string) error {
		if p == "prepared-myapp" {
			r.fw.with("myapp", func(s *Service) { s.Exe = r.bin + "/elsewhere" })
		}
		return nil
	})
	_, err := r.install("1.0.0-dev.1", "new binary")
	refused(t, err, "another file")
	rec := r.record()
	if rec.Phase != state.Reverted || len(rec.Bad) != 0 || r.content() != "old binary" || r.fw.restartCount() != 0 || rec.Prev != nil || rec.Request != nil {
		t.Errorf("record %+v content %q restarts %d", rec, r.content(), r.fw.restartCount())
	}
	noLeftovers(t, r)
	if r.e.busyName() != "" {
		t.Error("mutex held after an aborted install")
	}
}

func TestInstallAbortsWhenTheTargetChangedWhilePrepared(t *testing.T) {
	r := newRig(t)
	r.e = r.engine(func(p string) error {
		if p == "prepared-myapp" {
			os.WriteFile(r.target, []byte("hand edit"), 0o755)
		}
		return nil
	})
	_, err := r.install("1.0.0-dev.1", "new binary")
	refused(t, err, "changed while")
	if r.content() != "hand edit" || r.record().Phase != state.Failed {
		t.Errorf("content %q record %+v", r.content(), r.record())
	}
	noLeftovers(t, r)
}

func TestRecoveryRemovesLeftoversAndFlagsDrift(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("committed", phaseIs(state.Committed))
	os.WriteFile(r.target+".new", []byte("x"), 0o755)
	os.WriteFile(r.target+".prev.new", []byte("x"), 0o755)
	r.e = r.engine(nil)
	if err := r.e.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	noLeftovers(t, r)
	if r.record().NeedsManual != "" {
		t.Error("drift flagged without a change")
	}
	os.WriteFile(r.target, []byte("replaced by hand"), 0o755)
	if err := r.e.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.record().NeedsManual != state.Drift {
		t.Error("drift not flagged")
	}
	// One successful manual install clears it.
	mustInstall(t, r, "1.0.0-dev.2", "newer")
	r.until("committed", phaseIs(state.Committed))
	if r.record().NeedsManual != "" {
		t.Error("needs_manual not cleared by the install")
	}
}

func TestMissingRecordIsStateLostAndAnInstallAfterwardsClearsIt(t *testing.T) {
	r := newRig(t)
	if err := r.e.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec := r.record(); rec.NeedsManual != state.StateLost || rec.Phase != state.Committed {
		t.Errorf("record %+v", rec)
	}
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("committed", phaseIs(state.Committed))
	if r.record().NeedsManual != "" {
		t.Error("state-lost not cleared")
	}
}

// Recovery needs no Warren to finish a revert's files; the restart waits.
func TestRecoveryWithWarrenAbsentLeavesTheRestartPending(t *testing.T) {
	r := newRig(t)
	r.e = r.engine(crashAt("swapped-myapp"))
	r.install("1.0.0-dev.1", "new binary")
	r.fw.setManaged("myapp", false)
	r.fw.mu.Lock()
	r.fw.refuseRestart = true
	r.fw.mu.Unlock()
	r.e = r.engine(nil)
	if err := r.e.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec := r.record()
	if r.content() != "old binary" || rec.Request == nil || rec.Request.MinStartSeq != nil || rec.Deadline != nil {
		t.Fatalf("content %q record %+v", r.content(), rec)
	}
	r.fw.setManaged("myapp", true)
	r.fw.mu.Lock()
	r.fw.refuseRestart = false
	r.fw.mu.Unlock()
	r.until("restored healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
}

func TestReloadRefusesAnInstallingRemovalAndClosesProbation(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	next := *r.cfg
	next.Components = map[string]*uconfig.Comp{}
	if err := r.e.Reload(&next); err != nil {
		t.Fatal(err)
	}
	if rec := r.record(); rec.Phase != state.Committed {
		t.Errorf("probation of a removed component not closed: %+v", rec)
	}
	if r.e.busyName() != "" {
		t.Error("mutex held for a removed component")
	}
	rec := r.record()
	rec.Phase = state.Installing
	r.store().Save(rec)
	same := *r.cfg
	if err := r.e.Reload(&same); err != nil {
		t.Fatal(err)
	}
	if err := r.e.Reload(&next); err == nil {
		t.Error("a reload that removes an installing component was accepted")
	}
	changed := next
	changed.StateDir = "/elsewhere"
	if err := r.e.Reload(&changed); err == nil {
		t.Error("a changed state_dir was accepted")
	}
}
