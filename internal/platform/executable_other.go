//go:build !linux

package platform

// ExecutableIdentity cannot identify the running image on this system.
func ExecutableIdentity() (dev, ino uint64, ok bool) { return 0, 0, false }
