package platform

import (
	"net"

	"golang.org/x/sys/unix"
)

// MaxSunPath is the longest unix socket path: sun_path is 104 bytes on
// darwin, with its terminating NUL.
const MaxSunPath = 103

// ConnUID is the uid of the process at the other end of a unix socket
// connection, from LOCAL_PEERCRED.
func ConnUID(c net.Conn) (int, bool) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return 0, false
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, false
	}
	var x *unix.Xucred
	_ = raw.Control(func(fd uintptr) {
		x, err = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	})
	if err != nil || x == nil {
		return 0, false
	}
	return int(x.Uid), true
}

// SetNonDumpable does nothing on darwin: there is no per-process dumpable
// flag, so other processes of the same user are not stopped from inspecting
// Warren by this call.
func SetNonDumpable() error { return nil }

// Dumpable is always true on darwin.
func Dumpable() (bool, error) { return true, nil }

// SetDumpable does nothing on darwin.
func SetDumpable() error { return nil }

// SetSubreaper does nothing on darwin: orphaned grandchildren are not
// re-parented to Warren, so they are not reaped by it (launchd collects
// them).
func SetSubreaper() error { return nil }

// PeekExited is not available here: supported is false, and the caller
// reaps first and acts on the group after (a pid could in theory have been
// reused in between).
func PeekExited() (pid int, found, supported bool) { return 0, false, false }
