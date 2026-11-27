package platform

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// DefaultUpdatableEnv is the real system.
func DefaultUpdatableEnv() UpdatableEnv {
	return UpdatableEnv{
		ContainerFiles: []string{"/.dockerenv", "/run/.containerenv"},
		Root:           "/",
		Access: func(path string, mode uint32) error {
			return unix.Faccessat(unix.AT_FDCWD, path, mode, unix.AT_EACCESS)
		},
		Dev: func(path string) (uint64, error) {
			var st syscall.Stat_t
			if err := syscall.Stat(path, &st); err != nil {
				return 0, err
			}
			return uint64(st.Dev), nil //nolint:unconvert // the field type differs between systems
		},
	}
}
