package supervise

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func fast(t *testing.T) {
	t.Helper()
	bs, bm, br, ka := BackoffStart, BackoffMax, BackoffReset, KillAfter
	BackoffStart, BackoffMax, BackoffReset, KillAfter = 50*time.Millisecond, 400*time.Millisecond, time.Hour, 300*time.Millisecond
	t.Cleanup(func() { BackoffStart, BackoffMax, BackoffReset, KillAfter = bs, bm, br, ka })
}

func sh(script string) []string { return []string{"/bin/sh", "-c", script} }

func env() []string { return MinimalEnv(os.LookupEnv, nil, nil, nil) }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	dl := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(dl) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type times struct {
	mu sync.Mutex
	ts []time.Time
}

func (x *times) add() { x.mu.Lock(); x.ts = append(x.ts, time.Now()); x.mu.Unlock() }
func (x *times) get() []time.Time {
	x.mu.Lock()
	defer x.mu.Unlock()
	return append([]time.Time(nil), x.ts...)
}

func TestRestartWithDoublingBackoff(t *testing.T) {
	fast(t)
	s := New(quiet())
	defer s.StopAll()
	var starts times
	s.Add(Spec{Name: "x", Argv: sh("exit 3"), Env: env(), OnStart: starts.add})
	waitFor(t, "5 starts", func() bool { return len(starts.get()) >= 5 })
	ts := starts.get()
	// Delays: 50, 100, 200, 400 (the cap), measured between starts.
	want := []time.Duration{50, 100, 200, 400}
	for i, w := range want {
		gap := ts[i+1].Sub(ts[i])
		if gap < w*time.Millisecond || gap > w*time.Millisecond+250*time.Millisecond {
			t.Errorf("gap %d = %v, want about %v", i, gap, w*time.Millisecond)
		}
	}
	st := s.Status()[0]
	if st.Restarts < 4 || !strings.Contains(st.LastExit, "exit status 3") {
		t.Errorf("status %+v", st)
	}
}

func TestBackoffResetsAfterLongRun(t *testing.T) {
	fast(t)
	BackoffStart, BackoffMax, BackoffReset = 100*time.Millisecond, 10*time.Second, 300*time.Millisecond
	s := New(quiet())
	defer s.StopAll()
	var starts times
	// Stays up longer than BackoffReset every time, so the delay never grows.
	s.Add(Spec{Name: "x", Argv: sh("sleep 0.4"), Env: env(), OnStart: starts.add})
	waitFor(t, "4 starts", func() bool { return len(starts.get()) >= 4 })
	ts := starts.get()
	for i := 1; i < 4; i++ {
		if gap := ts[i].Sub(ts[i-1]); gap > 400*time.Millisecond+100*time.Millisecond+250*time.Millisecond {
			t.Errorf("gap %d = %v: backoff was not reset", i, gap)
		}
	}
}

func TestOnExitRunsAtOnce(t *testing.T) {
	fast(t)
	BackoffStart = 2 * time.Second
	s := New(quiet())
	defer s.StopAll()
	exited := make(chan time.Time, 4)
	s.Add(Spec{Name: "x", Argv: sh("sleep 0.1"), Env: env(), OnExit: func() { exited <- time.Now() }})
	start := time.Now()
	select {
	case at := <-exited:
		if at.Sub(start) > time.Second {
			t.Errorf("OnExit after %v, the restart delay was waited for", at.Sub(start))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("OnExit never ran")
	}
}

func TestMinimalEnvironment(t *testing.T) {
	t.Setenv("LEAK_ME", "secret")
	t.Setenv("PASS_ME", "passed")
	t.Setenv("HOME", "/home/x")
	lookup := os.LookupEnv
	e := MinimalEnv(lookup, []string{"PASS_ME", "UNSET_ONE"}, map[string]string{"A": "1", "HOME": "/over"}, map[string]string{"WARREN_SERVICE_SOCKET": "/s"})
	got := map[string]string{}
	for _, kv := range e {
		k, v, _ := strings.Cut(kv, "=")
		got[k] = v
	}
	if _, leaked := got["LEAK_ME"]; leaked {
		t.Error("unlisted variable passed")
	}
	if got["PASS_ME"] != "passed" || got["A"] != "1" || got["WARREN_SERVICE_SOCKET"] != "/s" || got["HOME"] != "/over" {
		t.Errorf("env %v", got)
	}
	if _, ok := got["UNSET_ONE"]; ok {
		t.Error("unset passed variable appeared")
	}
	if _, ok := got["PATH"]; !ok {
		t.Error("PATH missing")
	}
	// And the process really sees only that.
	fast(t)
	s := New(quiet())
	defer s.StopAll()
	out := filepath.Join(t.TempDir(), "env")
	s.Add(Spec{Name: "x", Argv: sh("printenv > " + out + "; sleep 30"), Env: e})
	waitFor(t, "env file", func() bool { b, _ := os.ReadFile(out); return len(b) > 0 })
	b, _ := os.ReadFile(out)
	if strings.Contains(string(b), "LEAK_ME") || !strings.Contains(string(b), "PASS_ME=passed") {
		t.Errorf("child env:\n%s", b)
	}
}

func TestStopTermThenKill(t *testing.T) {
	fast(t)
	s := New(quiet())
	var exits times
	s.Add(Spec{Name: "polite", Argv: sh("sleep 30"), Env: env(), OnExit: exits.add})
	s.Add(Spec{Name: "stubborn", Argv: sh(`trap "" TERM; while :; do sleep 0.05; done`), Env: env(), OnExit: exits.add})
	waitFor(t, "both running", func() bool {
		for _, st := range s.Status() {
			if st.State != Running {
				return false
			}
		}
		return true
	})
	time.Sleep(100 * time.Millisecond) // let the trap be set
	start := time.Now()
	s.StopAll()
	took := time.Since(start)
	if took < KillAfter-50*time.Millisecond || took > KillAfter+2*time.Second {
		t.Errorf("stopping took %v, want about %v (the stubborn one needs SIGKILL)", took, KillAfter)
	}
	for _, st := range s.Status() {
		if st.State != Stopped {
			t.Errorf("%s is %s", st.Name, st.State)
		}
	}
	if len(exits.get()) != 2 {
		t.Errorf("exits: %d", len(exits.get()))
	}
	if err := s.Add(Spec{Name: "late", Argv: sh("true")}); err == nil {
		t.Error("Add after stop accepted")
	}
}

func TestShutdownOrderServicesBeforeTransport(t *testing.T) {
	fast(t)
	s := New(quiet())
	var mu sync.Mutex
	var order []string
	rec := func(n string) func() { return func() { mu.Lock(); order = append(order, n); mu.Unlock() } }
	// The service ignores TERM, so it is the slow one; the transport must
	// still wait for it.
	s.Add(Spec{Name: "svc", Group: GroupService, Argv: sh(`trap "" TERM; while :; do sleep 0.05; done`), Env: env(), OnExit: rec("svc")})
	s.Add(Spec{Name: "tr", Group: GroupTransport, Argv: sh("sleep 30"), Env: env(), OnExit: rec("tr")})
	waitFor(t, "running", func() bool {
		for _, st := range s.Status() {
			if st.State != Running {
				return false
			}
		}
		return true
	})
	time.Sleep(100 * time.Millisecond)
	s.StopAll()
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(order, ",") != "svc,tr" {
		t.Errorf("order %v", order)
	}
}

func TestStateRestartingAndRemove(t *testing.T) {
	fast(t)
	BackoffStart = time.Hour
	s := New(quiet())
	s.Add(Spec{Name: "x", Argv: sh("exit 1"), Env: env()})
	waitFor(t, "restarting", func() bool { st := s.Status()[0]; return st.State == Restarting && st.LastExit != "" })
	s.Remove("x") // must not wait out the hour
	if s.Has("x") {
		t.Error("still has x")
	}
	if err := s.Add(Spec{Name: "bad", Argv: []string{"/nonexistent/prog"}, Env: env()}); err != nil {
		t.Fatal(err) // starting fails later, not at Add
	}
	waitFor(t, "start failure shown", func() bool { return strings.Contains(s.Status()[0].LastExit, "cannot start") })
	s.StopAll()
}

func TestOrphansAreReaped(t *testing.T) {
	fast(t)
	BackoffStart = time.Hour
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "pid")
	s := New(quiet())
	defer s.StopAll()
	// The shell starts a grandchild and exits at once, so the grandchild
	// re-parents to Warren, the subreaper, and later exits as our orphan.
	s.Add(Spec{Name: "x", Argv: sh("sleep 0.3 & echo $! > " + pidFile), Env: env()})
	waitFor(t, "pid file", func() bool { b, _ := os.ReadFile(pidFile); return strings.TrimSpace(string(b)) != "" })
	b, _ := os.ReadFile(pidFile)
	pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	// A zombie stays in /proc until it is reaped, so this is the reaping of
	// this very orphan, not of whichever one a counter saw last.
	waitFor(t, "this orphan to be reaped", func() bool {
		_, err := os.Stat("/proc/" + strconv.Itoa(pid))
		return err != nil
	})
}

func TestStateIsStartingBeforeTheFirstStart(t *testing.T) {
	fast(t)
	s := New(quiet())
	defer s.StopAll()
	gate := make(chan struct{})
	s.Add(Spec{Name: "x", Argv: sh("sleep 30"), Env: env(), Prepare: func() ([]*os.File, func(), error) {
		<-gate
		return nil, func() {}, nil
	}})
	if st := s.Status()[0].State; st != Starting {
		t.Errorf("before the first start the state is %q", st)
	}
	close(gate)
	waitFor(t, "running", func() bool { return s.Status()[0].State == Running })
}

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func TestAVeryLongOutputLineDoesNotKillTheProcess(t *testing.T) {
	fast(t)
	BackoffStart = time.Hour
	var buf syncBuf
	s := New(slog.New(slog.NewTextHandler(&buf, nil)))
	defer s.StopAll()
	// A 3 MiB line, then more output, then a clean exit.
	s.Add(Spec{Name: "x", Argv: sh("head -c 3000000 /dev/zero | tr '\\0' a; echo; echo after-the-long-line"), Env: env()})
	waitFor(t, "the clean exit", func() bool { return s.Status()[0].LastExit != "" })
	if got := s.Status()[0].LastExit; got != "exit status 0" {
		t.Errorf("the process ended with %q, not a clean exit", got)
	}
	waitFor(t, "the line after the long one is logged", func() bool { return strings.Contains(buf.String(), "after-the-long-line") })
	if !strings.Contains(buf.String(), "line truncated") {
		t.Error("the long line was not marked truncated")
	}
	if n := len(buf.String()); n > 2<<20 {
		t.Errorf("the log holds %d bytes", n)
	}
}

func TestRestartStopsAndStartsAgain(t *testing.T) {
	fast(t)
	s := New(quiet())
	defer s.StopAll()
	var starts times
	s.Add(Spec{Name: "x", Argv: sh("sleep 30"), Env: env(), OnStart: starts.add})
	waitFor(t, "running", func() bool { return s.Status()[0].State == Running })
	first := s.Status()[0].PID
	s.Restart("x")
	waitFor(t, "a second start", func() bool { return len(starts.get()) == 2 })
	waitFor(t, "running again", func() bool { st := s.Status()[0]; return st.State == Running && st.PID != first })
	if s.Status()[0].Restarts != 1 {
		t.Errorf("restarts %d", s.Status()[0].Restarts)
	}
	s.Restart("nobody") // harmless
}

func TestStopSignalsTheWholeProcessGroup(t *testing.T) {
	fast(t)
	pidFile := filepath.Join(t.TempDir(), "child")
	s := New(quiet())
	defer s.StopAll()
	// The shell's child would outlive a signal sent to the shell alone.
	s.Add(Spec{Name: "x", Argv: sh("sleep 300 & echo $! > " + pidFile + "; wait"), Env: env()})
	waitFor(t, "child pid", func() bool { b, _ := os.ReadFile(pidFile); return strings.HasSuffix(string(b), "\n") })
	b, _ := os.ReadFile(pidFile)
	pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	s.StopAll()
	waitFor(t, "the grandchild to be gone", func() bool {
		_, err := os.Stat("/proc/" + strconv.Itoa(pid))
		return err != nil
	})
}

func TestMembersThatIgnoreSigtermDoNotSurviveTheLeader(t *testing.T) {
	fast(t)
	pidFile := filepath.Join(t.TempDir(), "child")
	s := New(quiet())
	defer s.StopAll()
	// The leader dies of SIGTERM; its background member ignores it.
	s.Add(Spec{Name: "x", Argv: sh(`(trap "" TERM; exec sleep 300) & echo $! > ` + pidFile + `; wait`), Env: env()})
	waitFor(t, "child pid", func() bool { b, _ := os.ReadFile(pidFile); return strings.HasSuffix(string(b), "\n") })
	b, _ := os.ReadFile(pidFile)
	pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	// The member has set its trap and exec'd once its name is `sleep`.
	waitFor(t, "the member to exec sleep", func() bool {
		c, _ := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/comm")
		return strings.TrimSpace(string(c)) == "sleep"
	})
	s.StopAll()
	waitFor(t, "the member to be gone", func() bool {
		_, err := os.Stat("/proc/" + strconv.Itoa(pid))
		return err != nil
	})
}
