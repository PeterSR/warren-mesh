package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/PeterSR/warren-mesh/internal/daemon"
	"github.com/PeterSR/warren-mesh/internal/platform"
)

type timing struct {
	restartFloor time.Duration
	maxBackoff   time.Duration
	resetAfter   time.Duration
	stopGrace    time.Duration
	orphanRound  time.Duration
	killGrace    time.Duration
	poll         time.Duration
}

var normalTiming = timing{
	restartFloor: time.Second,
	maxBackoff:   time.Minute,
	resetAfter:   5 * time.Minute,
	stopGrace:    30 * time.Second,
	orphanRound:  time.Second,
	killGrace:    time.Second,
	poll:         100 * time.Millisecond,
}

func main() { os.Exit(run(os.Args[1:], normalTiming)) }

func run(args []string, times timing) int {
	fs := flag.NewFlagSet("warren-launch", flag.ContinueOnError)
	config := fs.String("config", "", "Warren configuration file")
	warren := fs.String("warren", "", "path to Warren, by default next to the launcher")
	if err := fs.Parse(args); errors.Is(err, flag.ErrHelp) {
		return 0
	} else if err != nil {
		return 2
	}
	if *config == "" || fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: warren-launch --config path [--warren path]")
		return 2
	}

	exe, err := executable(*warren)
	if err != nil {
		fmt.Fprintln(os.Stderr, "warren-launch:", err)
		return 1
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := checkDedicatedProcess(os.Getpid()); err != nil {
		log.Error("cannot safely supervise Warren", "err", err)
		return 1
	}
	if err := platform.SetSubreaper(); err != nil {
		log.Error("cannot become a child subreaper", "err", err)
		return 1
	}
	var proc *os.Root
	if runtime.GOOS == "linux" {
		proc, err = openProc("/proc", os.Getpid())
		if err != nil {
			log.Error("cannot inspect children", "err", err)
			return 1
		}
		defer proc.Close()
	}

	// A pending stop must survive a burst of reloads or child exits.
	stops := make(chan os.Signal, 1)
	signal.Notify(stops, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stops)
	reloads := make(chan os.Signal, 1)
	signal.Notify(reloads, syscall.SIGHUP)
	defer signal.Stop(reloads)
	childExits := make(chan os.Signal, 1)
	signal.Notify(childExits, syscall.SIGCHLD)
	defer signal.Stop(childExits)
	tick := time.NewTicker(times.poll)
	defer tick.Stop()

	l := launcher{
		log: log, times: times, stops: stops, reloads: reloads, childExits: childExits,
		proc: proc, tick: tick.C, ownGroup: syscall.Getpgrp(),
	}
	env := slices.DeleteFunc(os.Environ(), func(s string) bool {
		return strings.HasPrefix(s, "WARREN_LAUNCHER_MARKER=")
	})
	return l.supervise(exe, *config, env)
}

func executable(path string) (string, error) {
	if path != "" {
		return filepath.Abs(path)
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(exe), "warren"), nil
}

// The launcher loop serializes scanning, signaling and reaping. A child
// that exits stays waitable, so its PID cannot be recycled between a check
// and a signal as long as the loop has not reaped it.
type launcher struct {
	log        *slog.Logger
	times      timing
	stops      <-chan os.Signal
	reloads    <-chan os.Signal
	childExits <-chan os.Signal
	proc       *os.Root
	tick       <-chan time.Time
	ownGroup   int
	stopping   bool
}

func (l *launcher) supervise(exe, config string, env []string) int {
	backoff := l.times.restartFloor
	for {
		if l.stopPending() {
			return 0
		}
		started := time.Now()
		child, err := os.StartProcess(exe, []string{exe, "run", "--config", config}, &os.ProcAttr{
			Env:   env,
			Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
			Sys:   &syscall.SysProcAttr{Setsid: true},
		})
		var delay time.Duration
		if err != nil {
			delay, backoff = l.times.restartDelay(-1, 0, backoff)
			l.log.Warn("could not start Warren", "err", err, "restart_in", delay)
		} else {
			pid := child.Pid
			// Release the os.Process handle. Only this loop calls Wait4.
			_ = child.Release()
			status, err := l.waitForWarren(pid)
			if err != nil {
				l.log.Error("could not wait for Warren", "err", err)
				return 1
			}
			uptime := time.Since(started)
			if err := l.cleanUpChildren(); err != nil {
				l.log.Error("could not reap adopted children", "err", err)
				return 1
			}
			l.stopPending()
			code := exitCode(status)
			if code == 0 || l.stopping {
				return code
			}
			delay, backoff = l.times.restartDelay(code, uptime, backoff)
			if code == daemon.RestartExitCode {
				l.log.Info("Warren requested a restart", "restart_in", delay)
			} else {
				l.log.Warn("Warren exited", "code", code, "restart_in", delay)
			}
		}
		if l.stopDuring(delay) {
			return 0
		}
	}
}

func (t timing) restartDelay(code int, uptime, backoff time.Duration) (delay, next time.Duration) {
	if code == daemon.RestartExitCode {
		return t.restartFloor, t.restartFloor
	}
	if uptime >= t.resetAfter {
		backoff = t.restartFloor
	}
	return backoff, min(2*backoff, t.maxBackoff)
}

func exitCode(status syscall.WaitStatus) int {
	if status.Signaled() {
		return 128 + int(status.Signal())
	}
	return status.ExitStatus()
}

func (l *launcher) waitForWarren(pid int) (syscall.WaitStatus, error) {
	var stopAt time.Time
	for {
		status, exited, err := l.reapUntil(pid)
		if exited || err != nil {
			return status, err
		}
		if !stopAt.IsZero() && !time.Now().Before(stopAt) {
			l.log.Warn("Warren did not stop within the grace period", "pid", pid)
			l.send(pid, true, syscall.SIGKILL)
			stopAt = time.Time{}
		}

		switch sig := l.wake(); sig {
		case syscall.SIGHUP:
			if !l.stopping {
				l.send(pid, false, syscall.SIGHUP)
			}
		case syscall.SIGINT, syscall.SIGTERM:
			if !l.stopping {
				l.stopping = true
				l.send(pid, true, sig)
				stopAt = time.Now().Add(l.times.stopGrace)
			}
		}
	}
}

func (l *launcher) reapUntil(warren int) (syscall.WaitStatus, bool, error) {
	for {
		pid, found, supported := platform.PeekExited()
		if supported && !found {
			return 0, false, nil
		}
		if !supported {
			pid = warren
		} else if pid == warren {
			// Warren still holds its group ID. Give members of that
			// group the same grace as descendants in other groups.
			l.send(pid, true, syscall.SIGTERM)
		}
		var status syscall.WaitStatus
		reaped, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
		switch {
		case errors.Is(err, syscall.EINTR):
			continue
		case err != nil:
			return 0, false, err
		case reaped == 0:
			return 0, false, nil
		case reaped == warren:
			return status, true, nil
		}
	}
}

func (l *launcher) send(pid int, group bool, sig syscall.Signal) {
	target := pid
	if group && pid > 1 && pid != l.ownGroup {
		target = -pid
	}
	if err := syscall.Kill(target, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		l.log.Warn("could not signal child", "pid", pid, "signal", sig, "err", err)
	}
}

func (l *launcher) wake() syscall.Signal {
	select {
	case sig := <-l.stops:
		return sig.(syscall.Signal)
	case <-l.reloads:
		return syscall.SIGHUP
	case <-l.childExits:
		return syscall.SIGCHLD
	case <-l.tick:
		return 0
	}
}

func (l *launcher) stopPending() bool {
	// Discard a reload delivered when no Warren was running.
	select {
	case <-l.reloads:
	default:
	}
	select {
	case <-l.stops:
		l.stopping = true
	default:
	}
	return l.stopping
}

func (l *launcher) stopDuring(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-l.stops:
		l.stopping = true
		return true
	case <-timer.C:
		return l.stopPending()
	}
}

func (l *launcher) recordStop(sig syscall.Signal) {
	if sig == syscall.SIGTERM || sig == syscall.SIGINT {
		l.stopping = true
	}
}
