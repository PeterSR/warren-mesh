//go:build !linux

package transport

// copiesDescriptor is false where there is no pidfd_getfd.
func copiesDescriptor(pid, fd int) bool { return false }
