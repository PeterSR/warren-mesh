package platform

import "golang.org/x/sys/unix"

// ExecutableIdentity identifies the image this process is running, even
// after the path used to start it has been replaced.
func ExecutableIdentity() (dev, ino uint64, ok bool) {
	var st unix.Stat_t
	if err := unix.Stat("/proc/self/exe", &st); err != nil {
		return 0, 0, false
	}
	return uint64(st.Dev), st.Ino, true //nolint:unconvert // Dev differs across Linux architectures
}
