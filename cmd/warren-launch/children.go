package main

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type childProcess struct {
	pid   int
	group int
	state string
}

// cleanUpChildren finishes before another Warren may start. The launcher
// repeats the scan as intermediate descendants exit and expose their children.
func (l *launcher) cleanUpChildren() error {
	if l.proc == nil {
		return nil
	}
	for round := 0; round < 9; round++ {
		l.signalChildren(syscall.SIGTERM)
		empty, err := l.waitForChildren(l.times.orphanRound)
		if empty || err != nil {
			return err
		}
	}

	// Continue signaling newly adopted children during the final pass. A
	// descendant in its own session may only show up after SIGKILL.
	l.signalChildren(syscall.SIGKILL)
	deadline := time.Now().Add(l.times.killGrace)
	for {
		empty, err := reapChildren()
		if empty || err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			l.log.Warn("cleanup deadline expired with adopted children remaining")
			for _, child := range l.inspectChildren() {
				l.log.Error("child survived cleanup", "pid", child.pid, "state", child.state)
			}
			return nil
		}
		l.recordStop(l.wake())
		l.signalChildren(syscall.SIGKILL)
	}
}

func (l *launcher) inspectChildren() []childProcess {
	children, err := directChildren(l.proc)
	if err != nil {
		l.log.Warn("could not inspect adopted children", "err", err)
	}
	return children
}

func (l *launcher) signalChildren(sig syscall.Signal) {
	for _, child := range l.inspectChildren() {
		// Only signal a group whose leader is still ours to reap. For
		// other children, the PID is the identity we can hold.
		l.send(child.pid, child.pid == child.group, sig)
	}
}

func (l *launcher) waitForChildren(delay time.Duration) (bool, error) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		empty, err := reapChildren()
		if empty || err != nil {
			return empty, err
		}
		select {
		case <-l.stops:
			l.stopping = true
		case <-l.reloads:
		case <-l.childExits:
		case <-l.tick:
		case <-timer.C:
			return false, nil
		}
	}
}

func reapChildren() (bool, error) {
	for {
		pid, err := syscall.Wait4(-1, nil, syscall.WNOHANG, nil)
		switch {
		case errors.Is(err, syscall.ECHILD):
			return true, nil
		case errors.Is(err, syscall.EINTR):
			continue
		case err != nil || pid == 0:
			return false, err
		}
	}
}

// openProc pins the mount and verifies that the process numbers it reports
// are usable with signal and wait. When procfs belongs to an ancestor PID
// namespace, NSpid lists a PID for that namespace as well as our own.
func openProc(path string, pid int) (*os.Root, error) {
	proc, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	status, err := proc.ReadFile("self/status")
	if err != nil {
		_ = proc.Close()
		return nil, err
	}
	diagnostic := "cannot verify procfs PID namespace: NSpid missing from self/status"
	for line := range strings.SplitSeq(string(status), "\n") {
		if value, ok := strings.CutPrefix(line, "NSpid:"); ok {
			fields := strings.Fields(value)
			if len(fields) == 1 && fields[0] == strconv.Itoa(pid) {
				return proc, nil
			}
			diagnostic = "procfs must be mounted in the launcher's PID namespace"
			break
		}
	}
	_ = proc.Close()
	return nil, errors.New(diagnostic)
}

func directChildren(proc *os.Root) ([]childProcess, error) {
	entries, err := fs.ReadDir(proc.FS(), ".")
	if err != nil {
		return nil, err
	}
	parent := strconv.Itoa(os.Getpid())
	var children []childProcess
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 || !entry.IsDir() {
			continue
		}
		stat, err := proc.ReadFile(filepath.Join(entry.Name(), "stat"))
		if err != nil {
			continue
		}
		// The executable name is parenthesized and may contain spaces,
		// parentheses and newlines. State, PPID and PGID follow it.
		i := strings.LastIndexByte(string(stat), ')')
		if i < 0 {
			continue
		}
		fields := strings.Fields(string(stat[i+1:]))
		if len(fields) < 3 || fields[1] != parent {
			continue
		}
		group, err := strconv.Atoi(fields[2])
		if err != nil {
			continue
		}
		children = append(children, childProcess{pid: pid, group: group, state: fields[0]})
	}
	return children, nil
}
