package engine

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/update/manifest"
	"github.com/PeterSR/warren-mesh/internal/update/state"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
)

// newBoot plays a Warren restart: a new boot id, the counters start over, and
// the service runs again from start 1 with its lease.
func (f *fakeWarren) newBoot(boot string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.boot = boot
	for _, s := range f.svcs {
		s.StartSeq, s.Starts, s.Exits, s.RequestedExits, s.StartFailures = 1, 1, 0, 0, 0
		s.State = "running"
		s.Lease = Lease{Held: true, Epoch: 1, Since: time.Now()}
	}
}

func noRestart(*Service) {} // a restart that is accepted and does nothing

func (f *fakeWarren) setRestart(fn func(*Service)) {
	f.mu.Lock()
	f.onRestart = fn
	f.mu.Unlock()
}

func (f *fakeWarren) setRefuseFor(name string, v bool) {
	f.mu.Lock()
	if f.refuseFor == nil {
		f.refuseFor = map[string]bool{}
	}
	f.refuseFor[name] = v
	f.mu.Unlock()
}

func (f *fakeWarren) setRefuse(v bool) {
	f.mu.Lock()
	f.refuseRestart = v
	f.mu.Unlock()
}

// ---- a Warren restart (a new boot) and min_start_seq ----

// Warren restarts between step 1 and the restart request of an install. The
// boot in the record is the boot of the request, so min_start_seq is kept, and
// the old process that is still running is not judged as the new version.
func TestNewBootDuringAnInstallKeepsMinStartSeq(t *testing.T) {
	r := newRig(t)
	r.cfg.HealthTimeout = 600 * time.Millisecond
	r.e = r.engine(func(p string) error {
		if p == "prepared-myapp" {
			r.fw.newBoot("boot-2")
			r.fw.setRestart(noRestart) // the process keeps running what it started with
		}
		return nil
	})
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	rec := r.record()
	if rec.Request == nil || rec.Request.MinStartSeq == nil || *rec.Request.MinStartSeq != 2 || rec.Watch.Boot != "boot-2" {
		t.Fatalf("request %+v watch %+v", rec.Request, rec.Watch)
	}
	if r.ticksFor(500*time.Millisecond, func() bool { return r.record().HealthySince != nil }) {
		t.Fatal("the process that never restarted was judged healthy as the new version")
	}
	r.until("reverted at the deadline", phaseIs(state.Reverted))
}

// A manual revert after a Warren restart: the first restart is not accepted,
// and when it is the process that still runs the bad binary is not the
// restored version.
func TestNewBootBeforeARevertRestartKeepsMinStartSeq(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("committed", phaseIs(state.Committed))
	r.fw.newBoot("boot-2")
	r.fw.setRefuse(true)
	r.fw.setRestart(noRestart)
	if _, err := r.e.Revert(context.Background(), "myapp"); err != nil {
		t.Fatal(err)
	}
	if rec := r.record(); rec.Watch.Boot != "boot-2" || rec.Request == nil || rec.Request.MinStartSeq != nil {
		t.Fatalf("after the revert: %+v %+v", rec.Watch, rec.Request)
	}
	r.e.Tick(context.Background())
	r.fw.setRefuse(false)
	r.ticksFor(500*time.Millisecond, nil)
	rec := r.record()
	if rec.HealthySince != nil || rec.Request.MinStartSeq == nil || *rec.Request.MinStartSeq != 2 {
		t.Fatalf("the bad binary's process was judged as the restored version: %+v %+v", rec.HealthySince, rec.Request)
	}
}

// A request accepted in one boot is satisfied by any start of the next boot.
func TestRestoredWatchSurvivesAWarrenRestart(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("committed", phaseIs(state.Committed))
	r.fw.setRestart(restartHangs)
	if _, err := r.e.Revert(context.Background(), "myapp"); err != nil {
		t.Fatal(err)
	}
	if rec := r.record(); rec.Request.MinStartSeq == nil || rec.Watch.Boot != "boot-1" {
		t.Fatalf("%+v %+v", rec.Request, rec.Watch)
	}
	r.e.Tick(context.Background())
	r.fw.newBoot("boot-2") // the restored files start at once, with a lease
	r.until("healthy on the new boot", func(rec *state.Record) bool { return rec.HealthySince != nil })
	if rec := r.record(); *rec.Request.MinStartSeq != 1 || rec.Watch.Boot != "boot-2" {
		t.Errorf("%+v %+v", rec.Request, rec.Watch)
	}
}

// ---- deadlines are never armed in the past ----

func TestDeadlinesAreNeverArmedInThePast(t *testing.T) {
	r := newRig(t)
	r.cfg.HealthTimeout = time.Second
	r.e = r.engine(nil)
	long := r.clk.Now().Add(-time.Hour).UTC()
	old := sha256hex([]byte("old binary"))
	// A restored watch whose request is an hour old: the proof arms a fresh deadline.
	one := 2
	rec := state.New("myapp")
	rec.Phase = state.Reverted
	rec.Installed = &state.Installed{Version: "0.9.0", Files: map[string]string{"myapp": old}, InstalledAt: long}
	rec.Request = &state.Request{At: long, MinStartSeq: &one}
	rec.Watch = state.Watch{Boot: "boot-1"}
	r.store().Save(rec)
	r.fw.with("myapp", func(s *Service) { s.StartSeq = 2 })
	before := r.clk.Now()
	r.e.Tick(context.Background())
	got := r.record()
	if got.Deadline == nil || got.Deadline.Before(before.Add(r.comp.Settle+r.cfg.HealthTimeout-50*time.Millisecond)) {
		t.Fatalf("deadline %v is less than a settle window plus the health timeout from the proof", got.Deadline)
	}
	if got.Phase != state.Reverted {
		t.Errorf("phase %s", got.Phase)
	}
	// After a new boot: an install younger than three timeouts gets a whole
	// window from now; an older one is left alone.
	now := r.clk.Now().UTC()
	young := state.New("myapp")
	young.Phase = state.Probation
	young.Installed = &state.Installed{InstalledAt: now.Add(-r.cfg.HealthTimeout)}
	if d, extend := r.e.deadlineAfterBoot(r.comp, young, now); !extend || d.Before(now.Add(r.comp.Settle+r.cfg.HealthTimeout)) {
		t.Errorf("a young install: %v %v", d, extend)
	}
	aged := state.New("myapp")
	aged.Phase = state.Probation
	aged.Installed = &state.Installed{InstalledAt: now.Add(-3 * r.cfg.HealthTimeout)}
	if _, extend := r.e.deadlineAfterBoot(r.comp, aged, now); extend {
		t.Error("a deadline was extended for an install three timeouts aged")
	}
}

// ---- targets are resolved before a verdict is written ----

func TestRevertResolvesTargetsBeforeTheVerdict(t *testing.T) {
	r := newRig(t)
	r.comp.Targets = nil // the default target is the service's exe
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("committed", phaseIs(state.Committed))
	r.fw.setManaged("myapp", false) // Warren has no such service now
	before := r.record()
	_, err := r.e.Revert(context.Background(), "myapp")
	refused(t, err, "no targets in the config")
	if got := r.record(); got.Phase != state.Committed || got.UnfinishedRevert() || len(got.Bad) != len(before.Bad) {
		t.Errorf("a verdict was written for a revert that could not find its files: %+v", got)
	}
	if r.content() != "new binary" {
		t.Error("files changed")
	}
	// The offline revert needs explicit targets, said up front.
	_, err = New(Options{Config: r.cfg, Log: quiet()}).RevertOffline("myapp")
	refused(t, err, "explicit targets")
	if got := r.record(); got.Phase != state.Committed {
		t.Errorf("offline refusal wrote a verdict: %+v", got)
	}
}

// ---- who holds the install mutex ----

func TestTheWatcherNeverClearsALiveInstallsMutex(t *testing.T) {
	r := newRig(t)
	if err := r.e.acquire("myapp", false); err != nil {
		t.Fatal(err)
	}
	// The watcher syncs the holders from a record with nothing to hold.
	r.e.syncHolder("myapp", state.New("myapp"))
	if err := r.e.acquire("myapp", true); err == nil {
		t.Fatal("a live install was bypassed after the watcher synced the record")
	}
	r.e.release("myapp")
	if err := r.e.acquire("myapp", false); err != nil {
		t.Fatalf("the mutex is not free after release: %v", err)
	}
}

func init() { _ = noRestart }

// A second component for tests of the node-wide mutex.
func (r *rig) addComponent(name string) (target string) {
	r.t.Helper()
	target = filepath.Join(r.bin, name)
	os.WriteFile(target, []byte("old "+name), 0o755)
	r.fw.add(name, target)
	k := r.comp.Keys
	r.cfg.Components[name] = &uconfig.Comp{Name: name, Channel: manifest.Dev, Service: name, Keys: k,
		Targets: map[string]string{name: target}, Settle: r.comp.Settle, MaxSize: r.comp.MaxSize}
	return target
}

func (r *rig) installOther(name, version, content string, mods ...func(*InstallRequest)) (*InstallResult, error) {
	r.t.Helper()
	data, sig, _ := r.publish(version, content, func(m *manifest.Manifest) {
		m.Component = name
		f := m.Platforms[HostPlatform].Files["myapp"]
		f.Name = name + "_" + version
		m.Platforms[HostPlatform] = manifest.Platform{Files: map[string]manifest.File{name: f}}
	})
	res, err := r.e.Stage(name, data, sig)
	if err != nil {
		r.t.Fatal(err)
	}
	for _, n := range res.Need {
		if err := r.e.ReceiveFile(name, version, n.SHA256, strings.NewReader(content)); err != nil {
			r.t.Fatal(err)
		}
	}
	req := InstallRequest{Component: name, Version: version}
	for _, m := range mods {
		m(&req)
	}
	return r.e.Install(context.Background(), req)
}

func TestARestoredWatchHoldsOnlyItsOwnComponent(t *testing.T) {
	r := newRig(t)
	r.addComponent("files")
	r.e = r.engine(nil)
	r.e.Recover(context.Background())
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("committed", phaseIs(state.Committed))
	// A manual revert whose restart cannot be sent: myapp waits for Warren, and
	// goes on waiting while the others are served.
	r.fw.setRefuseFor("myapp", true)
	if _, err := r.e.Revert(context.Background(), "myapp"); err != nil {
		t.Fatal(err)
	}
	if k := r.e.holders["myapp"]; k != holdRestored {
		t.Fatalf("holders %v", r.e.holders)
	}
	// Another component installs, though myapp never settles.
	if _, err := r.installOther("files", "1.0.0-dev.1", "files v1"); err != nil {
		t.Fatalf("another component could not install: %v", err)
	}
	r.until("files healthy", func(*state.Record) bool {
		rec, err := r.store().Load("files")
		return err == nil && rec.HealthySince != nil
	})
	// myapp itself is refused, until force.
	if _, err := r.install("1.0.0-dev.2", "newer"); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("myapp was not refused while waiting for its restart: %v", err)
	}
	r.fw.setRefuseFor("myapp", false)
	if _, err := r.install("1.0.0-dev.2", "newer", func(q *InstallRequest) { q.Force = true }); err != nil {
		t.Fatalf("force did not go past a holder that only waits for a restart: %v", err)
	}
	if r.content() != "newer" {
		t.Errorf("content %q", r.content())
	}
}

func TestAProbationBlocksOtherComponentsAndForceNeverBypassesIt(t *testing.T) {
	r := newRig(t)
	r.addComponent("files")
	r.e = r.engine(nil)
	r.e.Recover(context.Background())
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	_, err := r.installOther("files", "1.0.0-dev.1", "files v1", func(q *InstallRequest) { q.Force = true })
	refused(t, err, "myapp is on probation")
	_, err = r.install("1.0.0-dev.2", "newer", func(q *InstallRequest) { q.Force = true })
	refused(t, err, "probation")
	// A live install is never bypassed either.
	r.until("healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
	if err := r.e.acquire("files", false); err != nil {
		t.Fatal(err)
	}
	if _, err := r.install("1.0.0-dev.2", "newer", func(q *InstallRequest) { q.Force = true }); err == nil {
		t.Fatal("a live install was bypassed")
	}
	r.e.release("files")
}

func TestAnUnfinishedRevertWithUnknownTargetsDoesNotBlockTheNode(t *testing.T) {
	r := newRig(t)
	r.addComponent("files")
	r.comp.Targets = nil // myapp's targets need the service
	r.e = r.engine(nil)
	rec := state.New("myapp")
	rec.Phase = state.Reverted
	rec.Installed = &state.Installed{Version: "1", Files: map[string]string{"myapp": "new"}}
	rec.Prev = &state.Prev{Version: "0", Files: map[string]string{"myapp": sha256hex([]byte("old binary"))}}
	r.store().Save(rec)
	r.fw.setManaged("myapp", false)
	if err := r.e.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !r.record().UnfinishedRevert() || r.e.holders["myapp"] != holdUnfinished {
		t.Fatalf("holders %v", r.e.holders)
	}
	if _, err := r.installOther("files", "1.0.0-dev.1", "files v1"); err != nil {
		t.Fatalf("an unfinished revert blocked another component: %v", err)
	}
}

// ---- a failed save after the install was recorded ----

func TestAFailedSaveAfterStepOneIsRecoveredByTheWatcher(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write anywhere")
	}
	r := newRig(t)
	dir := filepath.Join(r.state, "myapp")
	r.e = r.engine(func(p string) error {
		if p == "swapped-myapp" {
			os.Chmod(dir, 0o500) // the next save fails
		}
		return nil
	})
	_, err := r.install("1.0.0-dev.1", "new binary")
	os.Chmod(dir, 0o700)
	if err == nil {
		t.Fatal("the install did not fail")
	}
	if rec := r.record(); rec.Phase != state.Installing || r.content() != "new binary" {
		t.Fatalf("expected the record stuck in installing over the new file: %+v %q", rec.Phase, r.content())
	}
	if r.e.busyName() != "" {
		t.Errorf("the failed install still holds the mutex: %q", r.e.busyName())
	}
	// The watcher settles a record that no live install owns.
	r.e = r.engine(nil)
	r.until("reverted", phaseIs(state.Reverted))
	if rec := r.record(); r.content() != "old binary" || rec.UnfinishedRevert() || len(rec.Bad) != 0 {
		t.Errorf("content %q record %+v", r.content(), rec)
	}
	r.until("restored healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
}

// ---- reload is all or nothing ----

func TestReloadIsAllOrNothing(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("committed", phaseIs(state.Committed))
	r.fw.setRefuse(true)
	if _, err := r.e.Revert(context.Background(), "myapp"); err != nil {
		t.Fatal(err)
	}
	// An unfinished revert: removing the component is refused.
	removed := *r.cfg
	removed.Components = map[string]*uconfig.Comp{}
	settled := r.record()
	unfinished := *settled
	unfinished.Prev = &state.Prev{Version: "0", Files: map[string]string{"myapp": "x"}}
	r.store().Save(&unfinished)
	if err := r.e.Reload(&removed); err == nil || !strings.Contains(err.Error(), "finishing a revert") {
		t.Fatalf("removal of a component that is finishing a revert: %v", err)
	}
	r.store().Save(settled)
	if r.e.config() != r.cfg {
		t.Error("the configuration changed although the reload was refused")
	}
	// A record that cannot be written for a new component refuses the whole reload.
	next := *r.cfg
	next.Components = map[string]*uconfig.Comp{"myapp": r.comp, "files": {Name: "files", Channel: manifest.Dev, Service: "files", Keys: r.comp.Keys, Settle: r.comp.Settle, MaxSize: 1}}
	os.WriteFile(filepath.Join(r.state, "files"), []byte("in the way"), 0o600)
	if err := r.e.Reload(&next); err == nil {
		t.Fatal("a reload that could not write a record was accepted")
	}
	if r.e.config() != r.cfg {
		t.Error("the configuration changed although a record could not be written")
	}
	// Once it settles, removal is allowed and closes a probation unwatched.
	os.Remove(filepath.Join(r.state, "files"))
	r.fw.setRefuse(false)
	r.until("restored healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
	mustInstall(t, r, "1.0.0-dev.2", "newer")
	if err := r.e.Reload(&removed); err != nil {
		t.Fatal(err)
	}
	if rec := r.record(); rec.Phase != state.Committed || r.e.busyName() != "" {
		t.Errorf("record %+v busy %q", rec, r.e.busyName())
	}
	if r.e.config() != &removed {
		t.Error("the configuration was not swapped")
	}
}

// ---- smaller checks ----

// An abort after the swap began restarts the service: a process started from
// then on may already run the new file.
func TestAbortAfterTheSwapBeganRestartsTheService(t *testing.T) {
	r := newRig(t)
	second := filepath.Join(r.bin, "helper")
	os.WriteFile(second, []byte("old helper"), 0o755)
	r.comp.Targets = map[string]string{"myapp": r.target, "helper": second}
	// The second swap fails: its .prev is a directory with something in it.
	os.MkdirAll(filepath.Join(second+".prev", "x"), 0o755)
	files := map[string]manifest.File{}
	for t, c := range map[string]string{"myapp": "myapp 1", "helper": "helper 1"} {
		files[t] = manifest.File{Name: t + "_1", Size: int64(len(c)), SHA256: sha256hex([]byte(c))}
	}
	data, sig, _ := r.publish("1.0.0-dev.1", "x", func(m *manifest.Manifest) { m.Platforms = map[string]manifest.Platform{HostPlatform: {Files: files}} })
	res, err := r.e.Stage("myapp", data, sig)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range res.Need {
		r.e.ReceiveFile("myapp", "1.0.0-dev.1", n.SHA256, strings.NewReader(n.Target+" 1"))
	}
	before := r.fw.restartCount()
	if _, err := r.e.Install(context.Background(), InstallRequest{Component: "myapp", Version: "1.0.0-dev.1"}); err == nil {
		t.Fatal("the install did not fail")
	}
	if r.content() != "old binary" {
		t.Errorf("myapp was not put back: %q", r.content())
	}
	if r.fw.restartCount() != before+1 {
		t.Errorf("restarts %d, want one after the swap began", r.fw.restartCount()-before)
	}
	if rec := r.record(); rec.Phase != state.Reverted || rec.Request == nil || len(rec.Bad) != 0 {
		t.Errorf("record %+v", rec)
	}
	r.until("restored healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
}

func TestDryRunCleansStagingOnlyWhenItRan(t *testing.T) {
	r := newRig(t)
	r.stage("1.0.0-dev.1", "new binary")
	if err := r.e.acquire("myapp", false); err != nil { // a live install
		t.Fatal(err)
	}
	_, err := r.e.Install(context.Background(), InstallRequest{Component: "myapp", Version: "1.0.0-dev.1", DryRun: true})
	refused(t, err, "busy")
	if _, err := os.Stat(r.e.stagingDir("myapp", "1.0.0-dev.1")); err != nil {
		t.Errorf("a refused dry run cleared the staging of an install: %v", err)
	}
	r.e.release("myapp")
}

func TestStepThreeChecksThePinsAgain(t *testing.T) {
	r := newRig(t)
	r.e = r.engine(func(p string) error {
		if p == "prepared-myapp" {
			r.fw.mu.Lock()
			r.fw.pinned = "docker image x:y"
			r.fw.mu.Unlock()
		}
		return nil
	})
	_, err := r.install("1.0.0-dev.1", "new binary")
	refused(t, err, "pinned")
	if r.content() != "old binary" || r.record().Phase != state.Reverted {
		t.Errorf("content %q record %+v", r.content(), r.record())
	}
}

func TestStagingIsSafeAndChecksDigests(t *testing.T) {
	r := newRig(t)
	data, sig, _ := r.publish("1.0.0-dev.1", "new binary")
	res, err := r.e.Stage("myapp", data, sig)
	if err != nil {
		t.Fatal(err)
	}
	sha := res.Need[0].SHA256
	// Two uploads of the same file at once share nothing.
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = r.e.ReceiveFile("myapp", "1.0.0-dev.1", sha, strings.NewReader("new binary"))
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("writer %d: %v", i, err)
		}
	}
	dir := r.e.stagingDir("myapp", "1.0.0-dev.1")
	if b, _ := os.ReadFile(filepath.Join(dir, sha)); string(b) != "new binary" {
		t.Errorf("staged %q", b)
	}
	entries, _ := os.ReadDir(dir)
	for _, en := range entries {
		if strings.HasSuffix(en.Name(), ".part") {
			t.Errorf("a part file left: %s", en.Name())
		}
	}
	// Right size, wrong bytes: not "already staged".
	os.WriteFile(filepath.Join(dir, sha), []byte("EVIL binar"), 0o700)
	res, _ = r.e.Stage("myapp", data, sig)
	if len(res.Need) != 1 {
		t.Errorf("a staged file with the right size and the wrong digest is not asked for again: %+v", res)
	}
}

func TestPeerAllowedRefusesWhatItCannotRead(t *testing.T) {
	a, b := net.Pipe() // not a unix connection: its credentials cannot be read
	defer a.Close()
	defer b.Close()
	if peerAllowed(a) {
		t.Error("a connection with unreadable credentials was allowed")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("peer credentials are only read on Linux and darwin; every connection is refused here")
	}
	path := filepath.Join(t.TempDir(), "s")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, _ := net.Dial("unix", path)
		if c != nil {
			defer c.Close()
			time.Sleep(200 * time.Millisecond)
		}
	}()
	c, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if !peerAllowed(c) {
		t.Error("a connection from the same user was refused")
	}
}

// A pointer is two files that are replaced one after the other: a reader that
// catches the pair half way looks again, at a point the test controls.
func TestLoadFromDirLooksAgainAtAMismatchedPair(t *testing.T) {
	r := newRig(t)
	dir := t.TempDir()
	_, s1, _ := r.publish("1.0.0-dev.1", "one")
	d2, s2, _ := r.publish("1.0.0-dev.2", "two")
	name := filepath.Join(dir, manifest.FileName("myapp", "dev", ""))
	os.WriteFile(name, d2, 0o644)
	os.WriteFile(name+".minisig", s1, 0o644) // the new manifest with the old signature
	waits := 0
	var wait func(time.Duration)
	e := New(Options{Config: r.cfg, Log: quiet(), Wait: func(d time.Duration) { wait(d) }})
	wait = func(time.Duration) { // the writer finishes its second rename here
		waits++
		os.WriteFile(name+".minisig", s2, 0o644)
	}
	cand, err := e.LoadFromDir(dir, r.comp, "")
	if err != nil || cand.Manifest.Version != "1.0.0-dev.2" || waits != 1 {
		t.Fatalf("%v (waited %d times)", err, waits)
	}
	// A pair that stays wrong is still refused, after one look again.
	os.WriteFile(name+".minisig", s1, 0o644)
	waits = 0
	wait = func(time.Duration) { waits++ }
	if _, err := e.LoadFromDir(dir, r.comp, ""); err == nil || waits != 1 {
		t.Errorf("a mismatched pair: %v (waited %d times)", err, waits)
	}
}

// A torn dev pointer is looked at again before the stable pointer is used
// instead: the stale stable release must not win just because the dev pair was
// caught half way.
func TestATornDevPointerRetriesBeforeFallingBack(t *testing.T) {
	r := newRig(t)
	stableKey := newKey(t)
	sk, _ := manifest.ParseKey(stableKey.text)
	r.comp.Keys[manifest.Stable] = []manifest.Key{sk}
	dir := t.TempDir()
	dev, devSig, _ := r.publish("2.0.0-dev.1", "dev")
	_, oldSig, _ := r.publish("1.9.0-dev.1", "older")
	// The stable pointer: a release signed by the stable key.
	m := &manifest.Manifest{Format: manifest.Format, Component: "myapp", Version: "1.0.0", Channel: manifest.Stable, Created: "2026-11-26T10:00:00Z",
		Platforms: map[string]manifest.Platform{HostPlatform: {Files: map[string]manifest.File{"myapp": {Name: "myapp_1", Size: 1, SHA256: sha256hex([]byte("x"))}}}}}
	sd, _ := m.Marshal()
	os.WriteFile(filepath.Join(dir, manifest.FileName("myapp", "stable", "")), sd, 0o644)
	os.WriteFile(filepath.Join(dir, manifest.FileName("myapp", "stable", ""))+".minisig", manifest.Sign(stableKey.priv, sd, m), 0o644)
	devName := filepath.Join(dir, manifest.FileName("myapp", "dev", ""))
	os.WriteFile(devName, dev, 0o644)
	os.WriteFile(devName+".minisig", oldSig, 0o644) // torn
	e := New(Options{Config: r.cfg, Log: quiet(), Wait: func(time.Duration) { os.WriteFile(devName+".minisig", devSig, 0o644) }})
	cand, err := e.LoadFromDir(dir, r.comp, "")
	if err != nil || cand.Manifest.Version != "2.0.0-dev.1" {
		t.Fatalf("%v %+v", err, cand)
	}
}

// A Warren restart long after the install: the deadline cannot already be past
// or leave no room, so the watch runs through to its result, for a probation
// and for a restored version. (The settle clock starts at the poll that sees
// the new boot, so the deadline needs a settle window plus the health timeout.)
func TestAWatchAfterALateNewBootRunsThroughToItsResult(t *testing.T) {
	one := 2
	for _, phase := range []state.Phase{state.Probation, state.Reverted} {
		r := newRig(t)
		r.cfg.HealthTimeout = time.Second
		r.e = r.engine(nil)
		// Late, but within three health timeouts (1 s each here) of the anchor.
		long := r.clk.Now().Add(-2 * time.Second).UTC()
		rec := state.New("myapp")
		rec.Phase = phase
		rec.Installed = &state.Installed{Version: "1.0.0", Files: map[string]string{"myapp": sha256hex([]byte("old binary"))}, InstalledAt: long}
		rec.Request = &state.Request{At: long, MinStartSeq: &one}
		rec.Watch = state.Watch{Boot: "boot-1"}
		if phase == state.Probation {
			rec.Prev = &state.Prev{Version: "0.9.0", Files: map[string]string{"myapp": sha256hex([]byte("old binary"))}}
			rec.Deadline = ptrTime(long.Add(time.Second)) // already past: the new boot must give a fresh one
		}
		r.store().Save(rec)
		r.fw.newBoot("boot-2")
		r.until(string(phase)+": healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
		if got := r.record(); got.Phase == state.Failed || (phase == state.Probation && got.Phase != state.Probation && got.Phase != state.Committed) {
			t.Errorf("%s: ended as %s (%s)", phase, got.Phase, got.LastError)
		}
		if r.content() != "old binary" {
			t.Errorf("%s: files changed", phase)
		}
	}
}

// ---- reload under the component locks ----

// The records a reload decides on are read under the component locks and the
// decision is made after they are free: a record that changes while the reload
// waits is judged as it is then.
func TestReloadDecidesUnderTheComponentLocks(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary") // probation
	removed := *r.cfg
	removed.Components = map[string]*uconfig.Comp{}
	unlock := r.e.lockComp("myapp") // a watcher or an install is working on myapp
	done := make(chan error, 1)
	go func() { done <- r.e.Reload(&removed) }()
	select {
	case err := <-done:
		t.Fatalf("the reload did not wait for the component lock: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	// While it waits, the record becomes a revert in progress.
	rec := r.record()
	rec.Phase = state.Reverted
	rec.Prev = &state.Prev{Version: "0", Files: map[string]string{"myapp": "x"}} // an unfinished revert
	r.store().Save(rec)
	unlock()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "finishing a revert") {
		t.Fatalf("the reload used a record it read before the lock: %v", err)
	}
	if r.e.config() != r.cfg {
		t.Error("the configuration changed")
	}
}

func TestReloadRefusesToChangeTheServiceOfAComponentMidProbation(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	next := *r.cfg
	changed := *r.comp
	changed.Service = "other"
	next.Components = map[string]*uconfig.Comp{"myapp": &changed}
	if err := r.e.Reload(&next); err == nil || !strings.Contains(err.Error(), "service and targets cannot change") {
		t.Fatalf("got %v", err)
	}
	if r.e.config() != r.cfg {
		t.Error("the configuration changed")
	}
	r.until("committed", phaseIs(state.Committed))
	if err := r.e.Reload(&next); err != nil {
		t.Fatalf("a settled component's service could not change: %v", err)
	}
}

// ---- an orphaned installing record ----

// With the targets unknown, the orphan still gets its verdict, so it holds only
// its own component and the node can install.
func TestAnOrphanedInstallingRecordWithUnknownTargetsGetsItsVerdict(t *testing.T) {
	r := newRig(t)
	r.addComponent("files")
	r.comp.Targets = nil
	r.e = r.engine(nil)
	rec := state.New("myapp")
	rec.Phase = state.Installing
	rec.Installed = &state.Installed{Version: "1", Files: map[string]string{"myapp": "new"}}
	rec.Prev = &state.Prev{Version: "0", Files: map[string]string{"myapp": sha256hex([]byte("old binary"))}}
	r.store().Save(rec)
	r.fw.setManaged("myapp", false)
	r.e.Tick(context.Background())
	if got := r.record(); got.Phase != state.Reverted || !got.UnfinishedRevert() || len(got.Bad) != 0 {
		t.Fatalf("no verdict: %+v", got)
	}
	if k := r.e.holders["myapp"]; k != holdUnfinished {
		t.Fatalf("holders %v", r.e.holders)
	}
	if _, err := r.installOther("files", "1.0.0-dev.1", "files v1"); err != nil {
		t.Fatalf("the orphan blocked the node: %v", err)
	}
	// The warning is rate limited: many polls, one line.
	var lines int
	e := New(Options{Config: r.cfg, Control: NewControl(r.fw.path), Log: slog.New(slog.NewTextHandler(&countWriter{n: &lines}, nil))})
	for i := 0; i < 20; i++ {
		e.Tick(context.Background())
	}
	if lines > 2 {
		t.Errorf("%d log lines for 20 polls", lines)
	}
	// Once Warren reports the service again, the revert finishes.
	r.fw.setManaged("myapp", true)
	r.until("restored", func(rec *state.Record) bool { return !rec.UnfinishedRevert() })
}

type countWriter struct{ n *int }

func (c *countWriter) Write(p []byte) (int, error) {
	*c.n += strings.Count(string(p), "\n")
	return len(p), nil
}

// ---- offline revert ----

func TestOfflineRevertOfAnOrphanedInstallingRecord(t *testing.T) {
	r := newRig(t)
	r.e = r.engine(crashAt("swapped-myapp"))
	r.install("1.0.0-dev.1", "new binary")
	if r.content() != "new binary" || r.record().Phase != state.Installing {
		t.Fatal("setup")
	}
	off := New(Options{Config: r.cfg, Log: quiet()})
	before := r.fw.restartCount()
	res, err := off.RevertOffline("myapp")
	if err != nil {
		t.Fatal(err)
	}
	rec := r.record()
	if r.content() != "old binary" || rec.Phase != state.Reverted || rec.UnfinishedRevert() || len(rec.Bad) != 0 || r.fw.restartCount() != before || res.Phase != state.Reverted {
		t.Errorf("content %q record %+v", r.content(), rec)
	}
}

func TestOfflinePreflightRefusesBeforeAnyLock(t *testing.T) {
	r := newRig(t)
	off := New(Options{Config: r.cfg, Log: quiet()})
	if err := off.PreflightOffline("myapp"); err != nil {
		t.Fatalf("a configured component with targets: %v", err)
	}
	if err := off.PreflightOffline("nobody"); err == nil {
		t.Error("an unconfigured component")
	}
	r.comp.Pinned = "distro"
	refused(t, off.PreflightOffline("myapp"), "pinned: distro")
	r.comp.Pinned, r.comp.Targets = "", nil
	refused(t, off.PreflightOffline("myapp"), "explicit targets")
}

// ---- the floor a revert keeps ----

func TestARevertToAnUnknownVersionKeepsTheDowngradeFloor(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.5", "new binary") // the file before it was never recorded: prev version unknown
	r.until("committed", phaseIs(state.Committed))
	if _, err := r.e.Revert(context.Background(), "myapp"); err != nil {
		t.Fatal(err)
	}
	rec := r.record()
	if rec.Installed.Version != "" || rec.Installed.Floor != "1.0.0-dev.5" {
		t.Fatalf("installed %+v", rec.Installed)
	}
	r.until("restored healthy", func(rec *state.Record) bool { return rec.HealthySince != nil })
	_, err := r.install("1.0.0-dev.3", "older") // below the version that was reverted away
	refused(t, err, "floor")
	if _, err := r.install("1.0.0-dev.3", "older", func(q *InstallRequest) { q.AllowDowngrade = true }); err != nil {
		t.Fatalf("--allow-downgrade: %v", err)
	}
}

// ---- repeated boots cannot keep a version on probation for ever ----

// bootsUntil plays a unit that restarts Warren every 100 ms, the service never
// taking its lease, until the record shows the result or the time is up.
func (r *rig) bootsUntil(t *testing.T, what string, within time.Duration, cond func(*state.Record) bool) time.Duration {
	t.Helper()
	start := r.clk.Now()
	for n := 1; ; n++ {
		r.fw.newBoot("boot-" + strconv.Itoa(n+1))
		r.fw.with("myapp", func(s *Service) { s.Lease.Held = false })
		if r.ticksFor(100*time.Millisecond, func() bool { return cond(r.record()) }) {
			return r.clk.Now().Sub(start)
		}
		if r.clk.Now().Sub(start) > within {
			rec := r.record()
			t.Fatalf("%s: not reached in %v (phase %s, deadline %v)", what, within, rec.Phase, rec.Deadline)
		}
	}
}

func TestRepeatedBootsEndAProbation(t *testing.T) {
	r := newRig(t)
	r.cfg.HealthTimeout = 400 * time.Millisecond
	r.e = r.engine(nil)
	r.fw.setRestart(restartHangs)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	took := r.bootsUntil(t, "a revert", 4*time.Second, phaseIs(state.Reverted))
	// Bounded by the anchor plus four health timeouts plus a settle window.
	if limit := 4*r.cfg.HealthTimeout + r.comp.Settle + time.Second; took > limit {
		t.Errorf("took %v, more than %v", took, limit)
	}
	if len(r.record().Bad) != 0 || r.content() != "old binary" {
		t.Errorf("%+v %q", r.record().Bad, r.content())
	}
}

func TestRepeatedBootsEndARestoredWatch(t *testing.T) {
	r := newRig(t)
	r.cfg.HealthTimeout = 400 * time.Millisecond
	r.e = r.engine(nil)
	two := 2
	now := r.clk.Now().UTC()
	rec := state.New("myapp")
	rec.Phase = state.Reverted
	rec.Installed = &state.Installed{Version: "0.9.0", Files: map[string]string{"myapp": sha256hex([]byte("old binary"))}, InstalledAt: now}
	rec.Request = &state.Request{At: now, MinStartSeq: &two}
	rec.Watch = state.Watch{Boot: "boot-1"}
	rec.Deadline = ptrTime(now.Add(r.comp.Settle + r.cfg.HealthTimeout)) // armed at proof
	r.store().Save(rec)
	took := r.bootsUntil(t, "failed", 4*time.Second, phaseIs(state.Failed))
	if limit := 4*r.cfg.HealthTimeout + r.comp.Settle + time.Second; took > limit {
		t.Errorf("took %v, more than %v", took, limit)
	}
}

// ---- concurrent reloads, removal, targets and versions ----

// SIGHUP and the socket both call Reload. The configuration holds two components
// and two concurrent reloads each remove a different one, ensuring the record of
// the other, while a component is busy. The locks must be taken in one order, or
// each reload would hold what the other needs.
func TestConcurrentReloadsDoNotDeadlock(t *testing.T) {
	r := newRig(t)
	r.addComponent("files")
	r.e = r.engine(nil)
	r.e.Recover(context.Background())
	keepMyapp := *r.cfg
	keepMyapp.Components = map[string]*uconfig.Comp{"myapp": r.cfg.Components["myapp"]} // removes files
	keepFiles := *r.cfg
	keepFiles.Components = map[string]*uconfig.Comp{"files": r.cfg.Components["files"]} // removes myapp
	for i := 0; i < 20; i++ {
		r.e.mu.Lock()
		r.e.cfg = r.cfg
		r.e.mu.Unlock()
		unlock := r.e.lockComp("myapp") // busy
		var wg sync.WaitGroup
		done := make(chan struct{})
		for _, next := range []*uconfig.Config{&keepMyapp, &keepFiles} {
			wg.Add(1)
			go func() { defer wg.Done(); _ = r.e.Reload(next) }()
		}
		go func() { wg.Wait(); close(done) }()
		time.Sleep(10 * time.Millisecond)
		unlock()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent reloads deadlocked")
		}
	}
}

func TestRemovingAComponentWhoseRestoredRestartIsNeverAcceptedClosesItSettled(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	r.until("committed", phaseIs(state.Committed))
	r.fw.setRefuse(true)
	if _, err := r.e.Revert(context.Background(), "myapp"); err != nil {
		t.Fatal(err)
	}
	if !r.record().WatchingRestored() {
		t.Fatal("setup")
	}
	removed := *r.cfg
	removed.Components = map[string]*uconfig.Comp{}
	if err := r.e.Reload(&removed); err != nil {
		t.Fatal(err)
	}
	if rec := r.record(); rec.WatchingRestored() || rec.UnfinishedRevert() || r.e.holders["myapp"] != "" {
		t.Errorf("not settled: %+v holders %v", rec, r.e.holders)
	}
}

func TestForceDoesNotPassAnUnfinishedRevert(t *testing.T) {
	r := newRig(t)
	rec := state.New("myapp")
	rec.Phase = state.Reverted
	rec.Installed = &state.Installed{Version: "1", Files: map[string]string{"myapp": "new"}}
	rec.Prev = &state.Prev{Version: "0", Files: map[string]string{"myapp": sha256hex([]byte("old binary"))}}
	r.store().Save(rec)
	_, err := r.install("1.0.0-dev.2", "newer", func(q *InstallRequest) { q.Force = true })
	refused(t, err, "unfinished revert")
	if r.content() != "old binary" {
		t.Error("files changed")
	}
	// The acquire path says the same, so a holder that is only synced is refused too.
	r.e.syncHolder("myapp", r.record())
	refused(t, r.e.acquire("myapp", true), "unfinished revert")
}

func TestReloadRefusesATargetsChangeMidProbation(t *testing.T) {
	r := newRig(t)
	mustInstall(t, r, "1.0.0-dev.1", "new binary")
	next := *r.cfg
	changed := *r.comp
	changed.Targets = map[string]string{"myapp": filepath.Join(r.bin, "other")}
	next.Components = map[string]*uconfig.Comp{"myapp": &changed}
	if err := r.e.Reload(&next); err == nil || !strings.Contains(err.Error(), "targets cannot change") {
		t.Fatalf("got %v", err)
	}
}

// The component's settings are read again under its lock.
func TestInstallReadsTheComponentAgainAfterTheLock(t *testing.T) {
	r := newRig(t)
	r.stage("1.0.0-dev.1", "new binary")
	unlock := r.e.lockComp("myapp")
	done := make(chan error, 1)
	go func() {
		_, err := r.e.Install(context.Background(), InstallRequest{Component: "myapp", Version: "1.0.0-dev.1"})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	// While the call waits, the configuration is replaced: myapp now maintains another file.
	next := *r.cfg
	changed := *r.comp
	changed.Targets = map[string]string{"myapp": filepath.Join(r.bin, "other")}
	os.WriteFile(filepath.Join(r.bin, "other"), []byte("x"), 0o755)
	next.Components = map[string]*uconfig.Comp{"myapp": &changed}
	r.e.mu.Lock()
	r.e.cfg = &next
	r.e.mu.Unlock()
	unlock()
	refused(t, <-done, "another file")
	if r.content() != "old binary" {
		t.Error("the install used the settings it read before the lock")
	}
}

func TestInstallChecksTheVersionBeforeUsingItAsADirectory(t *testing.T) {
	r := newRig(t)
	for _, v := range []string{"../../x", "1.0.0/../..", "", "v1", "1.0"} {
		_, err := r.e.Install(context.Background(), InstallRequest{Component: "myapp", Version: v})
		// The semver error itself, not the "nothing is staged" that a directory
		// that is not there would also give.
		if err == nil || !strings.HasPrefix(err.Error(), "version: ") || strings.Contains(err.Error(), "nothing is staged") {
			t.Errorf("%q: %v", v, err)
		}
	}
}

// A first install aborted before its restart leaves the floor at a version that
// never ran; trying that version again must not be refused as "not newer".
func TestRetryingAnAbortedFirstInstallIsNotBlockedByItsOwnFloor(t *testing.T) {
	r := newRig(t)
	exe := r.target
	r.e = r.engine(func(p string) error {
		if p == "prepared-myapp" {
			r.fw.with("myapp", func(s *Service) { s.Exe = r.bin + "/elsewhere" })
		}
		return nil
	})
	_, err := r.install("1.0.0-dev.1", "new binary")
	refused(t, err, "another file")
	rec := r.record()
	if rec.Installed == nil || rec.Installed.Floor != "1.0.0-dev.1" {
		t.Fatalf("floor: %+v", rec.Installed)
	}
	// The cause is fixed; the same version is tried again.
	r.fw.with("myapp", func(s *Service) { s.Exe = exe })
	r.e = r.engine(nil)
	if _, err := r.install("1.0.0-dev.1", "new binary"); err != nil {
		t.Fatalf("the retry was refused: %v", err)
	}
	// A lower version is still refused.
	r.until("committed", phaseIs(state.Committed))
	_, err = r.install("1.0.0-dev.0", "older")
	refused(t, err, "not newer")
}

// The CLI checks a file before it streams it, and never through a symlink.
func TestOpenFileChecksTheFileBeforeSending(t *testing.T) {
	dir := t.TempDir()
	content := []byte("the binary")
	os.WriteFile(filepath.Join(dir, "good"), content, 0o644)
	sum := sha256hex(content)
	f, err := OpenFile(dir, "good", int64(len(content)), sum)
	if err != nil {
		t.Fatal(err)
	}
	if b := make([]byte, 3); func() bool { n, _ := f.Read(b); return n != 3 || string(b) != "the" }() {
		t.Error("the reader is not at the start")
	}
	f.Close()
	if _, err := OpenFile(dir, "good", int64(len(content))+1, sum); err == nil {
		t.Error("a wrong size accepted")
	}
	if _, err := OpenFile(dir, "good", int64(len(content)), sha256hex([]byte("other"))); err == nil {
		t.Error("a wrong digest accepted")
	}
	os.Symlink(filepath.Join(dir, "good"), filepath.Join(dir, "link"))
	if _, err := OpenFile(dir, "link", int64(len(content)), sum); err == nil {
		t.Error("a symlink was followed")
	}
	if _, err := OpenFile(dir, "../good", int64(len(content)), sum); err == nil {
		t.Error("a path accepted as a name")
	}
}

// The restored watch is anchored on the proof, not on the request, so a
// Warren restart a little after a late proof still gets a whole window.
func TestARestoredWatchIsBoundedFromTheProofNotTheRequest(t *testing.T) {
	r := newRig(t)
	r.cfg.HealthTimeout = time.Second
	r.e = r.engine(nil)
	two := 2
	rec := state.New("myapp")
	rec.Phase = state.Reverted
	rec.Installed = &state.Installed{Version: "0.9.0", Files: map[string]string{"myapp": sha256hex([]byte("old binary"))}, InstalledAt: r.clk.Now().Add(-time.Hour)}
	rec.Request = &state.Request{At: r.clk.Now().Add(-10 * time.Second).UTC(), MinStartSeq: &two} // accepted long ago
	rec.Watch = state.Watch{Boot: "boot-1"}
	r.store().Save(rec)
	// The service runs the restored file but has no lease yet: the proof holds, it does not settle.
	r.fw.with("myapp", func(s *Service) { s.StartSeq = 2; s.Lease.Held = false })
	r.e.Tick(context.Background())
	proof := r.clk.Now()
	armed := r.record()
	if armed.Deadline == nil || armed.Request.At.Before(proof.Add(-time.Second)) {
		t.Fatalf("the proof did not arm the deadline and the anchor: %+v %+v", armed.Deadline, armed.Request)
	}
	// Warren restarts just before the deadline would pass.
	r.clk.Advance(1050 * time.Millisecond)
	r.fw.newBoot("boot-2")
	r.until("healthy", func(rec *state.Record) bool { return rec.HealthySince != nil || rec.Phase == state.Failed })
	if got := r.record(); got.Phase != state.Reverted || got.HealthySince == nil {
		t.Fatalf("ended as %s (%s)", got.Phase, got.LastError)
	}
}

// The way out of an unfinished revert whose service Warren no longer
// reports: explicit targets, by a reload.
func TestExplicitTargetsFreeAnUnfinishedRevertWithDefaultTargets(t *testing.T) {
	r := newRig(t)
	r.comp.Targets = nil
	rec := state.New("myapp")
	rec.Phase = state.Reverted
	rec.Installed = &state.Installed{Version: "1", Files: map[string]string{"myapp": "new"}}
	rec.Prev = &state.Prev{Version: "0", Files: map[string]string{"myapp": sha256hex([]byte("old binary"))}}
	r.store().Save(rec)
	r.fw.setManaged("myapp", false)
	r.e.Tick(context.Background())
	if !r.record().UnfinishedRevert() {
		t.Fatal("setup")
	}
	// The refusals name the way out.
	r.e.syncHolder("myapp", r.record())
	refused(t, r.e.acquire("myapp", false), "explicit targets")
	removed := *r.cfg
	removed.Components = map[string]*uconfig.Comp{}
	if err := r.e.Reload(&removed); err == nil || !strings.Contains(err.Error(), "explicit targets") {
		t.Errorf("the removal refusal does not name the way out: %v", err)
	}
	// Other changes are still refused; adding explicit targets passes.
	bad := *r.cfg
	badComp := *r.comp
	badComp.Service = "other"
	badComp.Targets = map[string]string{"myapp": r.target}
	bad.Components = map[string]*uconfig.Comp{"myapp": &badComp}
	if err := r.e.Reload(&bad); err == nil {
		t.Error("a service change was accepted during an unfinished revert")
	}
	// A wrongly named target does not free it, and the refusal names what is needed.
	wrong := *r.cfg
	wrongComp := *r.comp
	wrongComp.Targets = map[string]string{"binary": r.target}
	wrong.Components = map[string]*uconfig.Comp{"myapp": &wrongComp}
	if err := r.e.Reload(&wrong); err == nil || !strings.Contains(err.Error(), "needs the targets myapp") {
		t.Errorf("a wrongly named target: %v", err)
	}
	next := *r.cfg
	withTargets := *r.comp
	withTargets.Targets = map[string]string{"myapp": r.target}
	next.Components = map[string]*uconfig.Comp{"myapp": &withTargets}
	if err := r.e.Reload(&next); err != nil {
		t.Fatalf("explicit targets were refused: %v", err)
	}
	r.e.Tick(context.Background())
	if rec := r.record(); rec.UnfinishedRevert() || rec.Installed.Version != "0" {
		t.Errorf("the revert was not finished: %+v", rec)
	}
}
