// Package fsutil holds the small file system helpers that Warren's daemon and
// updater share: durable writes, a hash of a file, directory sync, the owner of
// a file, and a non-blocking lock. It is a leaf package: it imports nothing of
// Warren's.
//
// The helpers are mechanisms only. What a directory's owner and mode must be
// is a policy, and each caller states its own (the daemon and the updater
// accept different modes on purpose).
package fsutil

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// SyncDir fsyncs a directory, so a rename or a link made in it survives a
// crash.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// SHA256Reader is the hex SHA-256 of what r yields until EOF.
func SHA256Reader(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// SHA256File is the hex SHA-256 of the file at path.
func SHA256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return SHA256Reader(f)
}

// WriteAtomic replaces the file at path with b, so that after a crash it holds
// the old content or the new, never less: b goes to a temporary file in the
// same directory (a hidden file named .<name>.tmp-<random>, made exclusively, so a link or
// file put there beforehand is never followed or written), which is synced
// before it is renamed over path, and the directory is synced after. A failed
// write leaves nothing behind. The file gets mode mode.
func WriteAtomic(path string, b []byte, mode os.FileMode) error {
	return WriteAtomicFunc(path, mode, func(w io.Writer) error {
		_, err := w.Write(b)
		return err
	})
}

// WriteAtomicFunc is WriteAtomic for content that fill writes: if fill fails,
// nothing is put at path.
func WriteAtomicFunc(path string, mode os.FileMode, fill func(w io.Writer) error) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // a no-op once renamed
	if err := fill(tmp); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return SyncDir(dir)
}

// WriteNoClobber writes b to path through a temporary file in the same
// directory and links it into place, so a failed write leaves nothing behind
// and an existing file is never replaced; the directory is synced. If the
// directory cannot be synced the file is removed again.
func WriteNoClobber(path string, b []byte, mode os.FileMode) error {
	return WriteNoClobberSync(path, b, mode, SyncDir)
}

// WriteNoClobberSync is WriteNoClobber with the directory sync given, for tests
// that need it to fail.
func WriteNoClobberSync(path string, b []byte, mode os.FileMode, syncDir func(string) error) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".new-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Link(tmp.Name(), path); err != nil { // fails if the file exists
		return err
	}
	if err := syncDir(dir); err != nil {
		os.Remove(path) // not durable: leave nothing behind
		return err
	}
	return nil
}

// WriteExclusive creates the file dst, which must not exist, writes what fill
// yields, gives it mode mode, and syncs it. The name is fixed, not random, and
// is created with O_EXCL, so an existing file or symlink at dst makes it fail
// rather than be followed. The file is created 0700 and only gets mode once it
// is written. If anything fails, dst is removed. The directory is not synced.
func WriteExclusive(dst string, mode os.FileMode, fill func(w io.Writer) error) error {
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			f.Close()
			os.Remove(dst)
		}
	}()
	if err := fill(f); err != nil {
		return err
	}
	if err := f.Chmod(mode); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}

// OwnerUID is the uid that owns the file fi describes, where the system says.
func OwnerUID(fi os.FileInfo) (uid int, ok bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int(st.Uid), true
}

// ErrLocked says another open file description holds the lock.
var ErrLocked = errors.New("locked by another holder")

// TryLock takes an exclusive flock on f without waiting. It returns ErrLocked
// when another holder has it. The lock is held until f is closed; take it once
// per file, never through a second descriptor of the same process.
func TryLock(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return ErrLocked
	}
	return err
}
