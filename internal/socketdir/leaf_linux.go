package socketdir

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// stagePrefix names the staging directories.
const stagePrefix = ".stage-"

// stageMu serialises the use of staging directories (see listenLeaf).
var stageMu sync.Mutex

// cleanStages removes what a Warren that died while it bound a socket left in
// dir: a ".stage-*" directory owned by Warren's user, with the sockets it
// owns inside it. Anything else (another owner, another kind of file) is left
// alone. Everything is done through descriptors, without following links.
// The socket directories must not be shared between Warrens: another Warren
// of the same user binding in the same directory at this moment would have
// its staging directory removed (this process only protects its own).
func cleanStages(dirFD int) {
	ents, err := os.ReadDir("/proc/self/fd/" + strconv.Itoa(dirFD))
	if err != nil {
		return
	}
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, stagePrefix) {
			continue
		}
		var st unix.Stat_t
		if unix.Fstatat(dirFD, name, &st, unix.AT_SYMLINK_NOFOLLOW) != nil ||
			st.Mode&unix.S_IFMT != unix.S_IFDIR || int(st.Uid) != Euid() {
			continue
		}
		fd, err := unix.Openat(dirFD, name, unix.O_PATH|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			continue
		}
		// What was opened is looked at again: the entry could have been swapped
		// between the check and the open.
		var fst unix.Stat_t
		if unix.Fstat(fd, &fst) != nil || fst.Mode&unix.S_IFMT != unix.S_IFDIR || int(fst.Uid) != Euid() {
			unix.Close(fd)
			continue
		}
		if inner, err := os.ReadDir("/proc/self/fd/" + strconv.Itoa(fd)); err == nil {
			for _, ie := range inner {
				var ist unix.Stat_t
				if unix.Fstatat(fd, ie.Name(), &ist, unix.AT_SYMLINK_NOFOLLOW) == nil &&
					ist.Mode&unix.S_IFMT == unix.S_IFSOCK && int(ist.Uid) == Euid() {
					_ = unix.Unlinkat(fd, ie.Name(), 0)
				}
			}
		}
		unix.Close(fd)
		_ = unix.Unlinkat(dirFD, name, unix.AT_REMOVEDIR) // fails, harmlessly, if something else is inside
	}
}

// listenLeaf makes the socket at path without following any link in its
// last two components, and returns it with the identity of the file made.
// The directory is opened with O_NOFOLLOW and held by descriptor, and the
// socket is bound through /proc/self/fd, so a directory swapped for a link
// meanwhile is never followed.
func listenLeaf(path string, mode os.FileMode) (net.Listener, *sock, error) {
	// Staging directories are made and removed under this lock, so a clean-up
	// of leftovers never meets one that is in use.
	stageMu.Lock()
	defer stageMu.Unlock()
	dir, base := filepath.Split(path)
	dir = filepath.Clean(dir)
	if base == "" || strings.ContainsRune(base, '/') {
		return nil, nil, fmt.Errorf("%q is not a socket path", path)
	}
	dirFD, err := unix.Open(dir, unix.O_PATH|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot open directory %s (a link is refused): %w", dir, err)
	}
	fail := func(err error) (net.Listener, *sock, error) {
		unix.Close(dirFD)
		return nil, nil, err
	}

	var st unix.Stat_t
	switch err := unix.Fstatat(dirFD, base, &st, unix.AT_SYMLINK_NOFOLLOW); {
	case err == nil:
		if st.Mode&unix.S_IFMT != unix.S_IFSOCK {
			return fail(fmt.Errorf("%s exists and is not a socket; left alone", path))
		}
		if int(st.Uid) != Euid() {
			return fail(fmt.Errorf("%s is a socket owned by uid %d; left alone", path, st.Uid))
		}
		// Ours from a Warren that was killed. Unlinkat removes the name and
		// never follows it.
		if err := unix.Unlinkat(dirFD, base, 0); err != nil {
			return fail(fmt.Errorf("cannot replace the old socket at %s: %w", path, err))
		}
	case errors.Is(err, unix.ENOENT):
	default:
		return fail(err)
	}

	// bind creates a socket with the process umask, which may leave group or
	// others write access for a moment before the chmod below. A socket whose
	// final mode closes either of them is therefore bound in a private (0700)
	// staging directory made beside it and renamed into place once its mode is
	// right, so it never exists under its name with looser permissions than
	// its final ones. A 0666 socket has nothing to tighten and is bound in
	// place; that is also the case of every socket in a directory that others
	// share (a child's), where no staging directory may be made.
	stageFD, stage := -1, ""
	bindFD := dirFD
	if mode&0o066 != 0o066 {
		var rnd [6]byte
		if _, err := rand.Read(rnd[:]); err != nil {
			return fail(err)
		}
		cleanStages(dirFD)
		stage = stagePrefix + hex.EncodeToString(rnd[:])
		if err := unix.Mkdirat(dirFD, stage, 0o700); err != nil {
			return fail(fmt.Errorf("cannot make a staging directory beside %s (making a socket needs permission to create a directory in %s): %w", path, dir, err))
		}
		fd, err := unix.Openat(dirFD, stage, unix.O_PATH|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			_ = unix.Unlinkat(dirFD, stage, unix.AT_REMOVEDIR)
			return fail(err)
		}
		stageFD, bindFD = fd, fd
	}
	dropStage := func() {
		if stageFD >= 0 {
			_ = unix.Unlinkat(dirFD, stage, unix.AT_REMOVEDIR)
			unix.Close(stageFD)
			stageFD = -1
		}
	}
	// The proc path is short, which also spares sun_path.
	bindPath := "/proc/self/fd/" + strconv.Itoa(bindFD) + "/" + base
	ln, err := net.Listen("unix", bindPath)
	if err != nil {
		dropStage()
		return fail(err)
	}
	// Closing must not unlink a path that may no longer be ours.
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	closeAll := func(err error) (net.Listener, *sock, error) {
		_ = ln.Close()
		_ = unix.Unlinkat(bindFD, base, 0)
		dropStage()
		return fail(err)
	}

	fileFD, err := unix.Openat(bindFD, base, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return closeAll(err)
	}
	if err := unix.Fstat(fileFD, &st); err != nil {
		unix.Close(fileFD)
		return closeAll(err)
	}
	switch {
	case st.Mode&unix.S_IFMT != unix.S_IFSOCK:
		unix.Close(fileFD)
		return closeAll(fmt.Errorf("%s is no longer a socket", path))
	case int(st.Uid) != Euid():
		// Someone renamed their own socket over ours meanwhile. It is not
		// ours to remove.
		unix.Close(fileFD)
		_ = ln.Close()
		dropStage() // what is in it is not ours to remove, and the directory then stays
		unix.Close(dirFD)
		return nil, nil, fmt.Errorf("%s belongs to uid %d, not to Warren", path, st.Uid)
	}
	// An O_PATH descriptor cannot be fchmod'ed, but its /proc link can be
	// chmod'ed, and that names this very file.
	if err := os.Chmod("/proc/self/fd/"+strconv.Itoa(fileFD), mode); err != nil {
		unix.Close(fileFD)
		return closeAll(err)
	}
	if stage != "" {
		// Into place under its final mode, atomically, and the staging
		// directory goes.
		// NOREPLACE: whatever appeared at the name since it was cleared is not
		// ours to replace (the same rule as for an entry found there at start).
		if err := unix.Renameat2(stageFD, base, dirFD, base, unix.RENAME_NOREPLACE); err != nil {
			unix.Close(fileFD)
			if errors.Is(err, unix.EEXIST) {
				return closeAll(fmt.Errorf("%s appeared while the socket was being made; left alone", path))
			}
			return closeAll(fmt.Errorf("cannot move the socket into place at %s: %w", path, err))
		}
		dropStage()
	}
	l := leaf{
		unlink:  func() { _ = unix.Unlinkat(dirFD, base, 0) },
		release: func() { unix.Close(fileFD); unix.Close(dirFD) },
		chmod:   func(m os.FileMode) error { return os.Chmod("/proc/self/fd/"+strconv.Itoa(fileFD), m) },
	}
	return ln, &sock{path: path, dev: uint64(st.Dev), ino: st.Ino, leaf: l}, nil
}
