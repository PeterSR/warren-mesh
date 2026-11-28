//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

var testTiming = timing{
	restartFloor: 50 * time.Millisecond,
	maxBackoff:   200 * time.Millisecond,
	resetAfter:   time.Hour,
	stopGrace:    200 * time.Millisecond,
	orphanRound:  25 * time.Millisecond,
	killGrace:    3 * time.Second,
	poll:         10 * time.Millisecond,
}

func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "launcher":
			os.Exit(run(os.Args[2:], testTiming))
		case "launcher-with-child":
			os.Exit(runWithExistingChild(os.Args[2:]))
		case "run":
			os.Exit(fakeWarren())
		case "outer", "inner":
			os.Exit(fakeDescendant(os.Args[1], os.Args[2]))
		}
	}
	os.Exit(m.Run())
}

func TestOpenProcRequiresLocalPIDNamespace(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "self"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		status string
		want   string
	}{
		{"local", "Name:\twarren-launch\nNSpid:\t123\n", ""},
		{"wrong PID", "NSpid:\t456\n", "must be mounted"},
		{"ancestor namespace", "NSpid:\t456\t123\n", "must be mounted"},
		{"coincident PIDs", "NSpid:\t123\t123\n", "must be mounted"},
		{"missing field", "Name:\twarren-launch\n", "NSpid missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(filepath.Join(dir, "self", "status"), []byte(tc.status), 0o600); err != nil {
				t.Fatal(err)
			}
			proc, err := openProc(dir, 123)
			if proc != nil {
				defer proc.Close()
			}
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("openProc failed: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("openProc error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRestartDelay(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    int
		uptime  time.Duration
		backoff time.Duration
		delay   time.Duration
		next    time.Duration
	}{
		{"exec error", -1, 0, time.Second, time.Second, 2 * time.Second},
		{"crash", 1, time.Minute, time.Second, time.Second, 2 * time.Second},
		{"cap", 1, time.Minute, 32 * time.Second, 32 * time.Second, time.Minute},
		{"at cap", 1, time.Minute, time.Minute, time.Minute, time.Minute},
		{"just below reset", 1, 5*time.Minute - time.Millisecond, time.Minute, time.Minute, time.Minute},
		{"uptime reset", 1, 5 * time.Minute, time.Minute, time.Second, 2 * time.Second},
		{"requested restart", 75, 0, time.Minute, time.Second, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delay, next := normalTiming.restartDelay(tc.code, tc.uptime, tc.backoff)
			if delay != tc.delay || next != tc.next {
				t.Errorf("restart delay and next backoff = %s, %s, want %s, %s", delay, next, tc.delay, tc.next)
			}
		})
	}
}

func TestLaunchRefusesExistingChildren(t *testing.T) {
	l := startLauncher(t, "existing-child")
	l.wait(1)
	l.waitForLog("already has child processes")
	if err := checkDedicatedProcess(1); err == nil || !strings.Contains(err.Error(), "PID 1") {
		t.Fatalf("PID 1 check returned %v", err)
	}
}

func TestLaunchBackoff(t *testing.T) {
	l := startLauncher(t, "backoff")
	l.wait(0)
	var delays []string
	for line := range strings.SplitSeq(l.log(), "\n") {
		if _, delay, ok := strings.Cut(line, "restart_in="); ok {
			delays = append(delays, delay)
		}
	}
	want := "50ms 100ms 50ms 50ms 100ms 200ms 200ms"
	if got := strings.Join(delays, " "); got != want {
		t.Fatalf("restart delays = %q, want %q\n%s", got, want, l.log())
	}
	starts := strings.Fields(readFile(filepath.Join(l.dir, "starts")))
	if len(starts) != 8 {
		t.Fatalf("got %d starts, want 8", len(starts))
	}
	for i, delay := range strings.Fields(want) {
		before, _ := strconv.ParseInt(starts[i], 10, 64)
		after, _ := strconv.ParseInt(starts[i+1], 10, 64)
		floor, _ := time.ParseDuration(delay)
		if elapsed := time.Duration(after - before); elapsed < floor {
			t.Errorf("restart %d after %s, want at least %s", i+1, elapsed, floor)
		}
	}
}

func TestLaunchSignals(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			l := startLauncher(t, "signals")
			l.waitFor("ready")
			l.signal(syscall.SIGHUP)
			l.waitFor("hangup")
			l.signal(sig)
			l.wait(75)
			l.waitFor(sig.String())
			if starts := strings.Fields(readFile(filepath.Join(l.dir, "starts"))); len(starts) != 1 {
				t.Fatalf("got %d starts after operator stop, want 1", len(starts))
			}
		})
	}
}

func TestLaunchForcesShutdown(t *testing.T) {
	l := startLauncher(t, "hang")
	l.waitFor("ready")
	start := time.Now()
	l.signal(syscall.SIGTERM)
	l.wait(128 + int(syscall.SIGKILL))
	if elapsed := time.Since(start); elapsed < testTiming.stopGrace {
		t.Errorf("forced shutdown after %s, want at least %s", elapsed, testTiming.stopGrace)
	}
	if !strings.Contains(l.log(), "did not stop within the grace period") {
		t.Errorf("missing forced shutdown log\n%s", l.log())
	}
}

func TestPendingStopWithOtherSignalsQueued(t *testing.T) {
	stops := make(chan os.Signal, 1)
	reloads := make(chan os.Signal, 1)
	childExits := make(chan os.Signal, 1)
	l := launcher{stops: stops, reloads: reloads, childExits: childExits}
	reloads <- syscall.SIGHUP
	childExits <- syscall.SIGCHLD
	stops <- syscall.SIGTERM
	if !l.stopPending() {
		t.Fatal("pending stop was missed before launch")
	}
}

func TestLaunchStopsDuringBackoff(t *testing.T) {
	l := startLauncher(t, "missing")
	l.waitForLog("could not start Warren")
	l.signal(syscall.SIGTERM)
	l.wait(0)
}

func TestLaunchReapsNestedSessions(t *testing.T) {
	for _, mode := range []string{"orphan-term", "orphan-kill"} {
		t.Run(mode, func(t *testing.T) {
			l := startLauncher(t, mode)
			l.wait(0)
			l.waitFor("reaped")
			outerPID := strings.TrimSpace(readFile(filepath.Join(l.dir, "outer")))
			innerPID := strings.TrimSpace(readFile(filepath.Join(l.dir, "inner")))
			if outerPID == "" || innerPID == "" || outerPID == innerPID {
				t.Fatalf("unexpected descendant PIDs %q and %q", outerPID, innerPID)
			}
		})
	}
}

type testLauncher struct {
	t          *testing.T
	dir        string
	process    *os.Process
	completion <-chan error
	waited     bool
}

func startLauncher(t *testing.T, mode string) *testLauncher {
	t.Helper()
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	launcher := exe
	args := []string{"launcher", "--config", dir, "--warren", exe}
	switch mode {
	case "backoff":
		// Use the real default executable resolution for this case.
		binary, err := os.ReadFile(exe)
		if err != nil {
			t.Fatal(err)
		}
		launcher = filepath.Join(dir, "warren-launch")
		if err := os.WriteFile(launcher, binary, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(exe, filepath.Join(dir, "warren")); err != nil {
			t.Fatal(err)
		}
		args = args[:3]
	case "missing":
		args[len(args)-1] = filepath.Join(dir, "missing")
	case "existing-child":
		args[0] = "launcher-with-child"
	}

	log, err := os.Create(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(launcher, args...)
	cmd.Env = append(os.Environ(),
		"WARREN_LAUNCH_TEST_MODE="+mode,
		"WARREN_LAUNCHER_MARKER=inherited",
		"GORACE=atexit_sleep_ms=0",
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout, cmd.Stderr = log, log
	err = cmd.Start()
	_ = log.Close()
	if err != nil {
		t.Fatal(err)
	}
	completion := make(chan error, 1)
	go func() { completion <- cmd.Wait() }()
	l := &testLauncher{t: t, dir: dir, process: cmd.Process, completion: completion}
	t.Cleanup(func() {
		if l.waited {
			return
		}
		_ = l.process.Signal(syscall.SIGTERM)
		select {
		case <-l.completion:
		case <-time.After(5 * time.Second):
			_ = l.process.Kill()
			<-l.completion
		}
	})
	return l
}

func (l *testLauncher) signal(sig syscall.Signal) {
	l.t.Helper()
	if err := l.process.Signal(sig); err != nil {
		l.t.Fatalf("signal %s: %v\n%s", sig, err, l.log())
	}
}

func (l *testLauncher) wait(want int) {
	l.t.Helper()
	select {
	case err := <-l.completion:
		l.waited = true
		got := 0
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			got = exit.ExitCode()
		} else if err != nil {
			l.t.Fatal(err)
		}
		if got != want {
			l.t.Fatalf("launcher exit = %d, want %d\n%s", got, want, l.log())
		}
	case <-time.After(15 * time.Second):
		l.t.Fatalf("launcher did not exit\n%s", l.log())
	}
}

func (l *testLauncher) waitFor(name string) {
	l.t.Helper()
	l.waitForCondition(name, func() bool {
		s := readFile(filepath.Join(l.dir, "events"))
		return strings.Contains(s, name+"\n")
	})
}

func (l *testLauncher) waitForLog(message string) {
	l.t.Helper()
	l.waitForCondition(message, func() bool { return strings.Contains(l.log(), message) })
}

func (l *testLauncher) waitForCondition(name string, ready func() bool) {
	l.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	l.t.Fatalf("timed out waiting for %q\n%s", name, l.log())
}

func (l *testLauncher) log() string { return readFile(filepath.Join(l.dir, "log")) }

func readFile(path string) string {
	data, _ := os.ReadFile(path)
	return string(data)
}

func runWithExistingChild(args []string) int {
	child := exec.Command(os.Args[0], "inner", args[1])
	if err := child.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer func() {
		_ = child.Process.Kill()
		_ = child.Wait()
	}()
	return run(args, testTiming)
}

func fakeWarren() int {
	if len(os.Args) != 4 || os.Args[2] != "--config" {
		fmt.Fprintln(os.Stderr, "unexpected Warren arguments", os.Args[1:])
		return 0
	}
	if _, inherited := os.LookupEnv("WARREN_LAUNCHER_MARKER"); inherited {
		fmt.Fprintln(os.Stderr, "launcher marker leaked to Warren")
		return 0
	}
	if session, err := unix.Getsid(0); err != nil || session != os.Getpid() {
		fmt.Fprintln(os.Stderr, "Warren is not its own session leader", session, err)
		return 0
	}
	dir := os.Args[3]
	startsFile := filepath.Join(dir, "starts")
	starts := len(strings.Fields(readFile(startsFile)))
	appendLine(startsFile, strconv.FormatInt(time.Now().UnixNano(), 10))
	events := filepath.Join(dir, "events")
	switch os.Getenv("WARREN_LAUNCH_TEST_MODE") {
	case "backoff":
		codes := []int{1, 1, 75, 1, 1, 1, 1, 0}
		return codes[starts]
	case "signals":
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
		appendLine(events, "ready")
		for sig := range signals {
			appendLine(events, sig.String())
			if sig != syscall.SIGHUP {
				return 75
			}
		}
	case "hang":
		signal.Ignore(syscall.SIGTERM)
		appendLine(events, "ready")
		time.Sleep(10 * time.Second)
	case "orphan-term", "orphan-kill":
		if starts == 0 {
			startDescendant("outer", dir)
			waitForFile(filepath.Join(dir, "inner"))
			return 75
		}
		for _, name := range []string{"outer", "inner"} {
			pid, _ := strconv.Atoi(strings.TrimSpace(readFile(filepath.Join(dir, name))))
			if pid <= 1 || !errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
				fmt.Fprintln(os.Stderr, "descendant still present on restart", name, pid)
				return 0
			}
		}
		appendLine(events, "reaped")
	}
	return 0
}

func startDescendant(role, dir string) {
	cmd := exec.Command(os.Args[0], role, dir)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	_ = cmd.Process.Release()
}

func fakeDescendant(role, dir string) int {
	if os.Getenv("WARREN_LAUNCH_TEST_MODE") == "orphan-kill" || role == "inner" {
		signal.Ignore(syscall.SIGTERM)
	}
	appendLine(filepath.Join(dir, role), strconv.Itoa(os.Getpid()))
	if role == "outer" {
		startDescendant("inner", dir)
	}
	time.Sleep(10 * time.Second)
	return 0
}

func appendLine(path, line string) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		panic(err)
	}
	if _, err := fmt.Fprintln(f, line); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
}

func waitForFile(path string) {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if readFile(path) != "" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic("descendant did not become ready: " + path)
}
