package main

import (
	"errors"

	"golang.org/x/sys/unix"
)

func checkDedicatedProcess(pid int) error {
	if pid == 1 {
		return errors.New("warren-launch cannot run as PID 1")
	}
	// Waitid with WNOWAIT observes existing children without reaping them.
	// ECHILD is the only result that establishes we own no earlier children.
	for {
		var info unix.Siginfo
		err := unix.Waitid(unix.P_ALL, 0, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT|unix.WALL, nil)
		switch {
		case errors.Is(err, unix.ECHILD):
			return nil
		case errors.Is(err, unix.EINTR):
			continue
		case err == nil:
			return errors.New("warren-launch already has child processes")
		default:
			return err
		}
	}
}
