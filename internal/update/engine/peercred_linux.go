package engine

import (
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// peerAllowed reports whether the process at the other end of a unix
// connection runs as the updater's user or root. A connection whose peer
// credentials cannot be read is refused.
func peerAllowed(c net.Conn) bool {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return false
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return false
	}
	allowed := false
	_ = raw.Control(func(fd uintptr) {
		if cred, err := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED); err == nil {
			allowed = int(cred.Uid) == os.Geteuid() || cred.Uid == 0
		}
	})
	return allowed
}
