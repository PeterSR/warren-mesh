//go:build !linux

package socketdir

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

// listenLeaf makes the socket at path. Outside Linux there is no way to bind
// or chmod through a directory descriptor, so the checks are made by path:
// the directory and any existing entry are examined without following links
// first, and the new socket is examined again afterwards. What is lost is
// the guarantee against a directory swapped for a link in the short window
// between the checks and the bind; a child that can race Warren there can
// make it create a socket elsewhere that Warren owns.
func listenLeaf(path string, mode os.FileMode) (net.Listener, *sock, error) {
	dir := filepath.Dir(path)
	if fi, err := os.Lstat(dir); err != nil || !fi.IsDir() {
		return nil, nil, fmt.Errorf("%s is not a directory (a link is refused)", dir)
	}
	if fi, err := os.Lstat(path); err == nil {
		st, _ := fi.Sys().(*syscall.Stat_t)
		switch {
		case fi.Mode()&os.ModeSocket == 0:
			return nil, nil, fmt.Errorf("%s exists and is not a socket; left alone", path)
		case st == nil || int(st.Uid) != Euid():
			return nil, nil, fmt.Errorf("%s is a socket owned by another user; left alone", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, nil, fmt.Errorf("cannot replace the old socket at %s: %w", path, err)
		}
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, nil, err
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	fi, err := os.Lstat(path)
	if err != nil || fi.Mode()&os.ModeSocket == 0 {
		_ = ln.Close()
		return nil, nil, fmt.Errorf("%s is not the socket just made", path)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || int(st.Uid) != Euid() {
		_ = ln.Close()
		return nil, nil, fmt.Errorf("%s belongs to another user, not to Warren", path)
	}
	if err := os.Chmod(path, mode); err != nil {
		_ = ln.Close()
		_ = os.Remove(path)
		return nil, nil, err
	}
	// No staging directory here: a socket made with a loose umask is
	// reachable until the chmod above. Not supported on this platform.
	l := leaf{unlink: func() { _ = os.Remove(path) }, release: func() {}, chmod: func(m os.FileMode) error { return os.Chmod(path, m) }}
	return ln, &sock{path: path, dev: uint64(st.Dev), ino: uint64(st.Ino), leaf: l}, nil
}
