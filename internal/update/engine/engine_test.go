package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/update/manifest"
	"github.com/PeterSR/warren-mesh/internal/update/state"
)

func mustInstall(t *testing.T, r *rig, version, content string, mods ...func(*InstallRequest)) *InstallResult {
	t.Helper()
	res, err := r.install(version, content, mods...)
	if err != nil {
		t.Fatalf("install %s: %v", version, err)
	}
	return res
}

func refused(t *testing.T, err error, want string) {
	t.Helper()
	var ref *Refusal
	if !errors.As(err, &ref) || !strings.Contains(err.Error(), want) {
		t.Fatalf("got %v, want a refusal containing %q", err, want)
	}
}

// ---- the happy path ----

func TestInstallHealthyCanaryCommit(t *testing.T) {
	r := newRig(t)
	res := mustInstall(t, r, "1.0.0-dev.1", "new binary")
	if res.Phase != state.Probation {
		t.Fatalf("phase %s", res.Phase)
	}
	rec := r.record()
	if rec.Phase != state.Probation || rec.Request == nil || rec.Request.MinStartSeq == nil || *rec.Request.MinStartSeq != 2 || rec.Deadline == nil {
		t.Fatalf("record after install: %+v", rec)
	}
	if rec.Installed.Version != "1.0.0-dev.1" || rec.Prev.Files["myapp"] != sha256hex([]byte("old binary")) || rec.NeedsManual != state.StateLost {
		t.Fatalf("record: %+v %+v", rec.Installed, rec.Prev)
	}
	if r.content() != "new binary" {
		t.Fatalf("content %q", r.content())
	}
	if b, _ := os.ReadFile(r.target + ".prev"); string(b) != "old binary" {
		t.Errorf(".prev holds %q", b)
	}
	if fi, _ := os.Stat(r.target); fi.Mode().Perm() != 0o755 {
		t.Errorf("mode %v", fi.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(r.state, "staging", "myapp", "1.0.0-dev.1")); err == nil {
		t.Error("staging not cleared")
	}
	// A second install is busy while the first is not yet healthy.
	if _, err := r.install("1.0.0-dev.2", "newer"); err == nil {
		t.Fatal("second install accepted")
	} else {
		refused(t, err, "")
	}
	r.until("healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
	if rec := r.record(); rec.Phase != state.Probation || rec.CanaryEnds == nil || rec.Deadline != nil {
		t.Errorf("healthy record: %+v", rec)
	}
	r.until("committed", phaseIs(state.Committed))
	if rec := r.record(); rec.NeedsManual != "" || len(rec.Bad) != 0 {
		t.Errorf("committed record: %+v", rec)
	}
	// The mutex is free again.
	if _, err := r.install("1.0.0-dev.2", "newer"); err != nil {
		t.Fatalf("install after commit: %v", err)
	}
}

func TestSettleResetsWhenTheLeaseChanges(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	// Keep the lease flapping faster than the settle time: never healthy.
	end := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(end) {
		r.fw.with("myapp", func(s *Service) { s.Lease.Epoch++ })
		r.e.Tick(context.Background())
		time.Sleep(20 * time.Millisecond)
	}
	if rec := r.record(); rec.HealthySince != nil {
		t.Fatal("healthy although the lease epoch kept changing")
	}
	r.until("healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
}

// ---- reverts ----

func TestCrashLoopRevertsAndMarksBad(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.e.Tick(context.Background())
	for i := 0; i < 3; i++ {
		r.fw.with("myapp", crash)
	}
	r.until("reverted", phaseIs(state.Reverted))
	rec := r.record()
	if !rec.IsBad("1.0.0-dev.1") || rec.Bad[0].Reason != state.CrashLoop {
		t.Errorf("bad: %+v", rec.Bad)
	}
	if r.content() != "old binary" {
		t.Errorf("content %q", r.content())
	}
	// Bookkeeping: installed is the old version again, prev is emptied.
	if rec.Prev != nil || rec.Installed.Files["myapp"] != sha256hex([]byte("old binary")) || rec.UnfinishedRevert() {
		t.Errorf("not settled: %+v %+v", rec.Installed, rec.Prev)
	}
	// The restored version is watched, restarted and proven healthy; no further revert.
	r.until("restored version healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
	if r.fw.restartCount() != 2 {
		t.Errorf("restarts %d, want the install's and the revert's", r.fw.restartCount())
	}
	if r.e.busyName() != "" {
		t.Errorf("mutex still held by %q", r.e.busyName())
	}
	// A bad version is refused without --force.
	_, err := r.install("1.0.0-dev.1", "new binary")
	refused(t, err, "bad")
}

// busyName is what keeps another component from installing: a live install, or a
// record on probation.
func (e *Engine) busyName() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.live != "" {
		return e.live
	}
	for n, k := range e.holders {
		if k == holdProbation {
			return n
		}
	}
	return ""
}

func TestStartFailuresCountAsFailures(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.e.Tick(context.Background())
	for i := 0; i < 3; i++ {
		r.fw.with("myapp", func(s *Service) { s.StartFailures++; s.StartSeq++ })
	}
	r.until("reverted", phaseIs(state.Reverted))
	if !r.record().IsBad("1.0.0-dev.1") {
		t.Error("not marked bad")
	}
}

func TestTwoFailuresAreNotACrashLoop(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.e.Tick(context.Background())
	for i := 0; i < 2; i++ {
		r.fw.with("myapp", crash)
	}
	r.until("healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
}

func TestTimeoutRevertsWithoutMarkingBad(t *testing.T) {
	r := newRig(t)
	r.cfg.HealthTimeout = 400 * time.Millisecond
	r.e = r.engine(nil)
	r.fw.onRestart = restartHangs
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("reverted", phaseIs(state.Reverted))
	rec := r.record()
	if len(rec.Bad) != 0 || r.content() != "old binary" || !strings.Contains(rec.LastError, "not healthy") {
		t.Errorf("record %+v content %q", rec, r.content())
	}
	// The restored version never takes a lease either: the watch fails, never reverts again.
	r.until("failed", phaseIs(state.Failed))
	if got := r.fw.restartCount(); got != 2 {
		t.Errorf("restarts %d", got)
	}
}

func TestServiceAbsentForAReloadWindowCountsNothing(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.fw.setManaged("myapp", false)
	for i := 0; i < 5; i++ {
		r.e.Tick(context.Background())
		time.Sleep(10 * time.Millisecond)
	}
	rec := r.record()
	if rec.Phase != state.Probation || len(rec.Failures) != 0 || rec.HealthySince != nil {
		t.Fatalf("an absent service changed the record: %+v", rec)
	}
	r.fw.setManaged("myapp", true)
	r.until("healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
}

func TestRunChangedByAReloadIsLikeAbsence(t *testing.T) {
	r := newRig(t)
	r.cfg.HealthTimeout = 500 * time.Millisecond
	r.e = r.engine(nil)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.fw.with("myapp", func(s *Service) { s.Exe = filepath.Join(r.bin, "other") })
	r.until("reverted at the deadline", phaseIs(state.Reverted))
	if len(r.record().Bad) != 0 {
		t.Error("marked bad")
	}
}

func TestRestoredRestartPendingIsNotFailed(t *testing.T) {
	r := newRig(t)
	r.cfg.HealthTimeout = 300 * time.Millisecond
	r.e = r.engine(nil)
	r.fw.onRestart = restartHangs
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	// The service goes away for good and a restart is not accepted: the revert's restart stays pending.
	r.fw.mu.Lock()
	r.fw.refuseRestart = true
	r.fw.mu.Unlock()
	r.until("reverted", phaseIs(state.Reverted))
	end := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(end) {
		r.e.Tick(context.Background())
		time.Sleep(10 * time.Millisecond)
	}
	rec := r.record()
	if rec.Phase != state.Reverted || rec.Request == nil || rec.Request.MinStartSeq != nil || rec.Deadline != nil {
		t.Fatalf("a restored watch with a pending restart must not run a deadline: %+v", rec)
	}
	if st := r.e.Status(context.Background()); !st.Components[0].RestartPending {
		t.Error("status does not show the restart pending")
	}
	// Warren accepts again: the watch carries on to healthy.
	r.fw.mu.Lock()
	r.fw.refuseRestart = false
	r.fw.onRestart = restartWorks
	r.fw.mu.Unlock()
	r.until("healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
}

func TestNewBootRecomputesTheDeadlineAndBaseline(t *testing.T) {
	r := newRig(t)
	r.cfg.HealthTimeout = 600 * time.Millisecond
	r.e = r.engine(nil)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	first := *r.record().Deadline
	time.Sleep(200 * time.Millisecond)
	// Warren restarted: a new boot, counters start over, the service runs the new file from start 1.
	r.fw.mu.Lock()
	r.fw.boot = "boot-2"
	s := r.fw.svcs["myapp"]
	s.StartSeq, s.Starts, s.Exits, s.RequestedExits = 1, 1, 0, 0
	r.fw.mu.Unlock()
	r.e.Tick(context.Background())
	rec := r.record()
	if rec.Watch.Boot != "boot-2" || *rec.Request.MinStartSeq != 1 || !rec.Deadline.After(first) {
		t.Fatalf("after a new boot: %+v deadline %v (was %v)", rec.Watch, rec.Deadline, first)
	}
	// The install is young, so the deadline is one settle window plus the health
	// timeout from the new boot.
	if want := time.Now().Add(r.comp.Settle + r.cfg.HealthTimeout); rec.Deadline.After(want.Add(time.Second)) || rec.Deadline.Before(want.Add(-time.Second)) {
		t.Errorf("deadline %v, want about %v", rec.Deadline, want)
	}
	r.until("healthy on the new boot", func(rec *state.Record) bool { return rec.HealthySince != nil })
}

func TestManualRevert(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("committed", phaseIs(state.Committed))
	res, err := r.e.Revert(context.Background(), "myapp")
	if err != nil {
		t.Fatal(err)
	}
	if res.Phase != state.Reverted || r.content() != "old binary" {
		t.Fatalf("%+v %q", res, r.content())
	}
	rec := r.record()
	if len(rec.Bad) != 1 || rec.Bad[0].Reason != state.Manual {
		t.Errorf("bad %+v", rec.Bad)
	}
	// A second revert finds an empty prev.
	_, err = r.e.Revert(context.Background(), "myapp")
	refused(t, err, "nothing to revert")
	r.until("restored healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
}

func TestRevertRefusedForAFailedComponentAndInstallNeedsForce(t *testing.T) {
	r := newRig(t)
	r.cfg.HealthTimeout = 200 * time.Millisecond
	r.e = r.engine(nil)
	r.fw.onRestart = restartHangs
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("failed", phaseIs(state.Failed))
	_, err := r.e.Revert(context.Background(), "myapp")
	refused(t, err, "failed")
	_, err = r.install("1.0.0-dev.2", "newer")
	refused(t, err, "install --force")
	r.fw.onRestart = restartWorks
	if _, err := r.install("1.0.0-dev.2", "newer", func(q *InstallRequest) { q.Force = true }); err != nil {
		t.Fatalf("force install: %v", err)
	}
}

func TestUnknownPhaseIsHandsOff(t *testing.T) {
	r := newRig(t)
	rec := state.New("myapp")
	rec.Phase = "frozen"
	r.store().Save(rec)
	_, err := r.install("1.0.0-dev.1", "new binary")
	refused(t, err, "unknown phase")
	_, err = r.e.Revert(context.Background(), "myapp")
	refused(t, err, "unknown phase")
	r.e.Tick(context.Background())
	if r.record().Phase != "frozen" || r.content() != "old binary" {
		t.Error("an unknown phase was touched")
	}
	if st := r.e.Status(context.Background()); st.Components[0].Phase != state.Failed {
		t.Errorf("status shows %q", st.Components[0].Phase)
	}
	if err := r.e.Recover(context.Background()); err != nil || r.record().Phase != "frozen" {
		t.Errorf("recover touched it: %v", err)
	}
}

func TestOfflineRevert(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("committed", phaseIs(state.Committed))
	off := New(Options{Config: r.cfg, Log: quiet(), Version: "test"})
	before := r.fw.restartCount()
	res, err := off.RevertOffline("myapp")
	if err != nil {
		t.Fatal(err)
	}
	if r.content() != "old binary" || r.fw.restartCount() != before || res.Phase != state.Reverted {
		t.Errorf("content %q restarts %d", r.content(), r.fw.restartCount())
	}
	if rec := r.record(); rec.Request != nil || rec.Prev != nil || rec.UnfinishedRevert() {
		t.Errorf("record %+v", rec)
	}
}

// ---- refusals ----

func TestInstallRefusals(t *testing.T) {
	type tc struct {
		name  string
		setup func(r *rig)
		mod   func(*InstallRequest)
		want  string
	}
	for _, c := range []tc{
		{"pinned component", func(r *rig) { r.comp.Pinned = "managed by apt" }, nil, "pinned: managed by apt"},
		{"pinned node", func(r *rig) { r.fw.pinned = "docker image x:y" }, nil, "node is pinned: docker image x:y"},
		{"read-only binary", func(r *rig) {
			r.fw.with("myapp", func(s *Service) { no := false; s.Updatable = &no; s.NotUpdatable = "read-only" })
		}, nil, "not updatable: read-only"},
		{"container binary", func(r *rig) {
			r.fw.with("myapp", func(s *Service) { no := false; s.Updatable = &no; s.NotUpdatable = "container" })
		}, nil, "not updatable: container"},
		{"service not managed", func(r *rig) { r.fw.setManaged("myapp", false) }, nil, "not managed"},
		{"no lease", func(r *rig) { r.fw.with("myapp", func(s *Service) { s.Lease.Held = false }) }, nil, "holds no lease"},
		{"exe is another file", func(r *rig) { r.fw.with("myapp", func(s *Service) { s.Exe = filepath.Join(r.bin, "other") }) }, nil, "another file"},
		{"exe not absolute", func(r *rig) { r.fw.with("myapp", func(s *Service) { s.Exe = ""; s.ExeError = "no such command" }) }, nil, "absolute"},
		{"same version as installed", func(r *rig) {
			rec := state.New("myapp")
			rec.Installed = &state.Installed{Version: "1.0.0-dev.1", Files: map[string]string{"myapp": sha256hex([]byte("old binary"))}}
			r.store().Save(rec)
		}, nil, "not newer"},
		{"downgrade", func(r *rig) {
			rec := state.New("myapp")
			rec.Installed = &state.Installed{Version: "2.0.0-dev.1", Files: map[string]string{"myapp": sha256hex([]byte("old binary"))}}
			r.store().Save(rec)
		}, nil, "not newer"},
		{"bad version", func(r *rig) {
			rec := state.New("myapp")
			rec.Bad = []state.Bad{{Version: "1.0.0-dev.1", Reason: "crash-loop"}}
			r.store().Save(rec)
		}, nil, "marked bad"},
	} {
		r := newRig(t)
		c.setup(r)
		before := r.content()
		_, err := r.install("1.0.0-dev.1", "new binary", c.mod)
		refused(t, err, c.want)
		if r.content() != before {
			t.Errorf("%s: the target changed", c.name)
		}
		if _, err := os.Stat(r.target + ".new"); err == nil {
			t.Errorf("%s: .new left", c.name)
		}
	}
}

func TestForceSkipsPrudenceOnlyAndAllowDowngradeInstallsOlder(t *testing.T) {
	r := newRig(t)
	rec := state.New("myapp")
	rec.Installed = &state.Installed{Version: "2.0.0-dev.1", Files: map[string]string{"myapp": sha256hex([]byte("old binary"))}}
	rec.Bad = []state.Bad{{Version: "1.0.0-dev.1", Reason: "crash-loop"}}
	r.store().Save(rec)
	r.fw.with("myapp", func(s *Service) { s.Lease.Held = false })
	_, err := r.install("1.0.0-dev.1", "new binary", func(q *InstallRequest) { q.Force = true })
	refused(t, err, "not newer") // force never skips the version floor
	if _, err := r.install("1.0.0-dev.1", "new binary", func(q *InstallRequest) { q.Force, q.AllowDowngrade = true, true }); err != nil {
		t.Fatal(err)
	}
	// Force does not skip pins either.
	r2 := newRig(t)
	r2.comp.Pinned = "x"
	_, err = r2.install("1.0.0-dev.1", "new binary", func(q *InstallRequest) { q.Force = true })
	refused(t, err, "pinned")
}

func TestRevertRefusedWhenPinnedOrNotUpdatable(t *testing.T) {
	for name, setup := range map[string]func(r *rig){
		"component": func(r *rig) { r.comp.Pinned = "managed by apt" },
		"node":      func(r *rig) { r.fw.pinned = "docker image x:y" },
		"not updatable": func(r *rig) {
			r.fw.with("myapp", func(s *Service) { no := false; s.Updatable = &no; s.NotUpdatable = "container" })
		},
	} {
		r := newRig(t)
		mustInstall(t, r, "1.0.0-dev.1", "new binary")
		r.until("committed", phaseIs(state.Committed))
		setup(r)
		_, err := r.e.Revert(context.Background(), "myapp")
		refused(t, err, map[string]string{"component": "pinned: managed", "node": "node is pinned", "not updatable": "not updatable: container"}[name])
		if r.content() != "new binary" {
			t.Errorf("%s: reverted anyway", name)
		}
		// Offline revert refuses a pinned component too.
		if name == "component" {
			_, err := New(Options{Config: r.cfg, Log: quiet()}).RevertOffline("myapp")
			refused(t, err, "pinned")
		}
	}
}

func TestStatusShowsPinUpdatableAndRunningVersion(t *testing.T) {
	r := newRig(t)
	r.comp.Pinned = "managed by apt"
	r.fw.pinned = "docker image x:y"
	r.fw.with("myapp", func(s *Service) { no := false; s.Updatable = &no; s.NotUpdatable = "read-only" })
	st := r.e.Status(context.Background())
	c := st.Components[0]
	if st.Pinned != "docker image x:y" || c.Pinned != "managed by apt" || c.Updatable == nil || *c.Updatable || c.NotUpdatable != "read-only" || c.Version != "unknown" || !st.WarrenReachable {
		t.Errorf("status %+v %+v", st, c)
	}
}

func TestDryRunChangesNothing(t *testing.T) {
	r := newRig(t)
	res, err := r.install("1.0.0-dev.1", "new binary", func(q *InstallRequest) { q.DryRun = true })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Message, "dry run") || r.content() != "old binary" || r.fw.restartCount() != 0 {
		t.Errorf("%+v", res)
	}
	if _, err := r.store().Load("myapp"); !errors.Is(err, state.ErrNone) {
		t.Error("a dry run wrote state")
	}
	if r.e.busyName() != "" {
		t.Error("a dry run held the mutex")
	}
}

func TestSymlinkedTargetsAndExeAreRefused(t *testing.T) {
	r := newRig(t)
	link := filepath.Join(r.bin, "link")
	os.Symlink(r.target, link)
	r.comp.Targets = map[string]string{"myapp": link}
	r.fw.with("myapp", func(s *Service) { s.Exe = link })
	_, err := r.install("1.0.0-dev.1", "new binary")
	refused(t, err, "symlink")
}

func TestTargetSetMustMatchTheManifest(t *testing.T) {
	r := newRig(t)
	r.comp.Targets = map[string]string{"myapp": r.target, "extra": filepath.Join(r.bin, "extra")}
	os.WriteFile(filepath.Join(r.bin, "extra"), []byte("x"), 0o755)
	_, err := r.install("1.0.0-dev.1", "new binary")
	refused(t, err, "configured targets")
}

func TestDefaultTargetsComeFromTheServiceExe(t *testing.T) {
	r := newRig(t)
	r.comp.Targets = nil
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	if r.content() != "new binary" {
		t.Errorf("content %q", r.content())
	}
}

func TestMultipleTargetsSwapTogetherAndRevertTogether(t *testing.T) {
	r := newRig(t)
	second := filepath.Join(r.bin, "helper")
	os.WriteFile(second, []byte("old helper"), 0o755)
	r.comp.Targets = map[string]string{"myapp": r.target, "helper": second}
	publish := func(version string) {
		files := map[string]manifest.File{}
		for t, c := range map[string]string{"myapp": "myapp " + version, "helper": "helper " + version} {
			files[t] = manifest.File{Name: t + "_" + version, Size: int64(len(c)), SHA256: sha256hex([]byte(c))}
		}
		data, sig, _ := r.publish(version, "x", func(m *manifest.Manifest) { m.Platforms = map[string]manifest.Platform{Platform: {Files: files}} })
		res, err := r.e.Stage("myapp", data, sig)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range res.Need {
			if err := r.e.ReceiveFile("myapp", version, n.SHA256, strings.NewReader(n.Target+" "+version)); err != nil {
				t.Fatal(err)
			}
		}
	}
	publish("1.0.0-dev.1")
	if _, err := r.e.Install(context.Background(), InstallRequest{Component: "myapp", Version: "1.0.0-dev.1"}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(second)
	if r.content() != "myapp 1.0.0-dev.1" || string(b) != "helper 1.0.0-dev.1" {
		t.Fatalf("%q %q", r.content(), b)
	}
	r.e.Tick(context.Background())
	for i := 0; i < 3; i++ {
		r.fw.with("myapp", crash)
	}
	r.until("reverted", phaseIs(state.Reverted))
	b, _ = os.ReadFile(second)
	if r.content() != "old binary" || string(b) != "old helper" {
		t.Fatalf("after revert %q %q", r.content(), b)
	}
}

// ---- staging ----

func TestStageRefusals(t *testing.T) {
	r := newRig(t)
	data, sig, _ := r.publish("1.0.0-dev.1", "new binary")
	other := newKey(t)
	badSig := manifest.Sign(other.priv, data, &manifest.Manifest{Component: "myapp", Version: "1.0.0-dev.1", Channel: manifest.Dev})
	if _, err := r.e.Stage("myapp", data, badSig); err == nil {
		t.Error("a manifest signed by another key was staged")
	}
	if _, err := r.e.Stage("myapp", append([]byte{' '}, data...), sig); err == nil {
		t.Error("a tampered manifest was staged")
	}
	if _, err := r.e.Stage("other", data, sig); err == nil {
		t.Error("an unconfigured component was staged")
	}
	d2, s2, _ := r.publish("1.0.0-dev.2", "x", func(m *manifest.Manifest) {
		m.Platforms = map[string]manifest.Platform{"plan9/mips": m.Platforms[Platform]}
	})
	_, err := r.e.Stage("myapp", d2, s2)
	refused(t, err, "no build for")

	res, err := r.e.Stage("myapp", data, sig)
	if err != nil || len(res.Need) != 1 {
		t.Fatalf("%v %+v", err, res)
	}
	sha := res.Need[0].SHA256
	for name, body := range map[string]string{"short": "new", "long": "new binary and more", "wrong bytes": "NEW BINARY"} {
		if err := r.e.ReceiveFile("myapp", "1.0.0-dev.1", sha, strings.NewReader(body)); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if err := r.e.ReceiveFile("myapp", "1.0.0-dev.1", strings.Repeat("0", 64), strings.NewReader("x")); err == nil {
		t.Error("a digest not in the manifest accepted")
	}
	if err := r.e.ReceiveFile("myapp", "../1.0.0", sha, strings.NewReader("x")); err == nil {
		t.Error("a bad version accepted")
	}
	entries, _ := os.ReadDir(r.e.stagingDir("myapp", "1.0.0-dev.1"))
	for _, en := range entries {
		if strings.HasSuffix(en.Name(), ".part") || en.Name() == sha {
			t.Errorf("unexpected staged file %s", en.Name())
		}
	}
	if err := r.e.ReceiveFile("myapp", "1.0.0-dev.1", sha, strings.NewReader("new binary")); err != nil {
		t.Fatal(err)
	}
	if res, _ := r.e.Stage("myapp", data, sig); len(res.Need) != 0 {
		t.Error("a staged file is asked for again")
	}
	if fi, _ := os.Stat(filepath.Join(r.e.stagingDir("myapp", "1.0.0-dev.1"), sha)); fi.Mode().Perm() != 0o700 {
		t.Errorf("staged file mode %v", fi.Mode().Perm())
	}
}

func TestInstallRefusesAFileThatChangedInStaging(t *testing.T) {
	r := newRig(t)
	r.stage("1.0.0-dev.1", "new binary")
	dir := r.e.stagingDir("myapp", "1.0.0-dev.1")
	entries, _ := os.ReadDir(dir)
	for _, en := range entries {
		if len(en.Name()) == 64 {
			os.WriteFile(filepath.Join(dir, en.Name()), []byte("EVIL binar"), 0o700)
		}
	}
	_, err := r.e.Install(context.Background(), InstallRequest{Component: "myapp", Version: "1.0.0-dev.1"})
	if err == nil || r.content() != "old binary" {
		t.Fatalf("a changed staged file was installed: %v %q", err, r.content())
	}
	if rec := r.record(); rec.Phase != state.Reverted || len(rec.Bad) != 0 {
		t.Errorf("record %+v", rec)
	}
	if _, err := os.Stat(r.target + ".new"); err == nil {
		t.Error(".new left")
	}
}

func TestKeysInForceAtTheSwapAreUsed(t *testing.T) {
	r := newRig(t)
	r.stage("1.0.0-dev.1", "new binary")
	// The key is removed from the config between staging and the swap.
	other := newKey(t)
	k, _ := manifest.ParseKey(other.text)
	r.comp.Keys = manifest.Keys{manifest.Dev: {k}}
	_, err := r.e.Install(context.Background(), InstallRequest{Component: "myapp", Version: "1.0.0-dev.1"})
	refused(t, err, "manifest refused")
	if r.content() != "old binary" {
		t.Error("installed with a removed key")
	}
}
