package transport

import "golang.org/x/sys/unix"

// copiesDescriptor reports whether this process can copy descriptor fd out of
// process pid with pidfd_getfd, the attack that PR_SET_DUMPABLE 0 stops.
func copiesDescriptor(pid, fd int) bool {
	pfd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return false
	}
	defer unix.Close(pfd)
	got, err := unix.PidfdGetfd(pfd, fd, 0)
	if err != nil {
		return false
	}
	unix.Close(got)
	return true
}
