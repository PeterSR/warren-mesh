//go:build !linux

package socketdir

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// AdjustDir looks at a directory and, if decide says so, changes its mode.
// The directory is opened without following a link and both looked at and
// changed by descriptor. A link, or anything that is not a directory, is
// ErrNotPlainDir. It returns the state it found and whether the mode was
// changed.
func AdjustDir(dir string, decide func(DirState) (os.FileMode, bool)) (DirState, bool, error) {
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
			return DirState{}, false, ErrNotPlainDir
		}
		return DirState{}, false, err
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return DirState{}, false, err
	}
	ds := DirState{Perm: os.FileMode(st.Mode & 0o777), Uid: int(st.Uid)}
	to, change := decide(ds)
	if !change {
		return ds, false, nil
	}
	if err := unix.Fchmod(fd, uint32(to)); err != nil {
		return ds, false, err
	}
	return ds, true, nil
}
