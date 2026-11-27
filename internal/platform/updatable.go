package platform

import (
	"errors"
	"syscall"
)

// Why a binary cannot be replaced by an update.
const (
	// ReasonReadOnly: Warren's user cannot write the file, or cannot write
	// and search its directory.
	ReasonReadOnly = "read-only"
	// ReasonMountPoint: the binary is itself a mount point (a single-file bind
	// mount), so renaming a new file over it fails.
	ReasonMountPoint = "mount-point"
	// ReasonContainer: Warren runs in a container and the binary is in the
	// container's own layer (the file system of "/"), not on a volume, so a
	// replacement would be lost with the container.
	ReasonContainer = "container"
)

// UpdatableEnv is what the updatable check looks at. The defaults are the real
// system; tests replace them.
type UpdatableEnv struct {
	// ContainerFiles are files whose presence means Warren runs in a
	// container.
	ContainerFiles []string
	// Root is the path whose file system is the container's own layer.
	Root string
	// Access checks the effective user's access to a path (faccessat with
	// AT_EACCESS); nil means the system cannot say.
	Access func(path string, mode uint32) error
	// Dev is the st_dev of a path.
	Dev func(path string) (uint64, error)
}

// Access modes for UpdatableEnv.Access.
const (
	AccessW = 2
	AccessX = 1
)

// readOnly maps an access error: true for the errors that mean "not
// writable by this user here", and known=false for any other (a missing file,
// an I/O error), which says nothing.
func readOnly(err error) (ro, known bool) {
	switch {
	case err == nil:
		return false, true
	case errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EROFS), errors.Is(err, syscall.EPERM):
		return true, true
	}
	return false, false
}

// Check says whether the binary at exe could be replaced by renaming a new file
// over it. known is false where the system cannot say (not Linux), and then
// the other results mean nothing; an access error other than EACCES, EROFS or
// EPERM (a missing file, an I/O error) is also unknown. The reasons are checked
// in order: read-only (W_OK on the file, W_OK and X_OK on its directory), then
// container (a container file exists and the directory's device is that of
// Root), then mount-point (the file's device is not its directory's). A
// symlinked exe is judged by what it points to, for the file checks.
func (e UpdatableEnv) Check(exe string) (updatable bool, reason string, known bool) {
	if e.Access == nil || e.Dev == nil {
		return false, "", false
	}
	dir := dirOf(exe)
	for _, c := range []struct {
		path string
		mode uint32
	}{{exe, AccessW}, {dir, AccessW | AccessX}} {
		ro, ok := readOnly(e.Access(c.path, c.mode))
		if !ok {
			return false, "", false
		}
		if ro {
			return false, ReasonReadOnly, true
		}
	}
	dirDev, err := e.Dev(dir)
	if err != nil {
		return false, "", false
	}
	// The directory is what is compared with the root: a single-file bind mount
	// and overlayfs without xino would otherwise give the file another device
	// than its layer. This comes before the mount-point check, which overlayfs
	// would also trip.
	for _, f := range e.ContainerFiles {
		if _, err := e.Dev(f); err != nil {
			continue
		}
		rootDev, err := e.Dev(e.Root)
		if err != nil {
			return false, "", false // in a container, but cannot tell which layer
		}
		if dirDev == rootDev {
			return false, ReasonContainer, true
		}
		break
	}
	// The binary's device differs from its directory's when the file is a mount
	// point of its own: rename over it gives EBUSY.
	exeDev, err := e.Dev(exe)
	if err != nil {
		return false, "", false
	}
	if exeDev != dirDev {
		return false, ReasonMountPoint, true
	}
	return true, "", true
}

func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			if i == 0 {
				return "/"
			}
			return p[:i]
		}
	}
	return "."
}
