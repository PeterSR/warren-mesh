package platform

import (
	"net"
	"unsafe"

	"golang.org/x/sys/unix"
)

// MaxSunPath is the longest unix socket path: sun_path is 108 bytes with
// its terminating NUL.
const MaxSunPath = 107

// ConnUID is the uid of the process at the other end of a unix socket
// connection (SO_PEERCRED), as seen from this process's user namespace.
func ConnUID(c net.Conn) (int, bool) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return 0, false
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, false
	}
	var cred *unix.Ucred
	_ = raw.Control(func(fd uintptr) {
		cred, err = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})
	if err != nil || cred == nil {
		return 0, false
	}
	return int(cred.Uid), true
}

// SetNonDumpable makes the process non-dumpable (PR_SET_DUMPABLE 0), so
// other processes of the same user cannot attach with ptrace or copy its
// descriptors with pidfd_getfd. It does not survive exec.
func SetNonDumpable() error { return unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0) }

// Dumpable reports the current setting, for tests.
func Dumpable() (bool, error) {
	v, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
	return v == 1, err
}

// SetDumpable sets the flag back, for tests.
func SetDumpable() error { return unix.Prctl(unix.PR_SET_DUMPABLE, 1, 0, 0, 0) }

// SetSubreaper makes orphaned descendants re-parent to this process
// (PR_SET_CHILD_SUBREAPER), so they can be reaped, as an init process does.
func SetSubreaper() error { return unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) }

// PeekExited reports the pid of a child that has exited and not been
// reaped yet, without reaping it (waitid with WNOWAIT), so the caller can
// act on the process group while its leader is still a zombie and its pid
// cannot have been reused. found is false when there is none. supported is
// always true on Linux.
func PeekExited() (pid int, found, supported bool) {
	var info unix.Siginfo
	err := unix.Waitid(unix.P_ALL, 0, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil)
	if err != nil || info.Signo == 0 {
		return 0, false, true
	}
	// si_pid follows signo, errno, code and (on 64-bit systems) padding.
	off := 12
	if unsafe.Sizeof(uintptr(0)) == 8 {
		off = 16
	}
	return int(*(*int32)(unsafe.Add(unsafe.Pointer(&info), off))), true, true
}
