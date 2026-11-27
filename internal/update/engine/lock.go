package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/PeterSR/warren-mesh/internal/fsutil"
)

// LockName is the file in the state directory whose exclusive flock marks the
// one process that may write the updater's state: the daemon holds it for its
// whole life, the offline revert takes it for a moment.
const LockName = "update.lock"

// SocketName is the updater's own control socket in the state directory.
const SocketName = "updater.sock"

// PrepareStateDir makes the state directory (0700) and refuses one that
// others can use. The updater's policy is strict: any access for group or
// others (0o077) refuses, where the daemon only refuses write (0o022).
func PrepareStateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("state_dir %s must not be accessible by group or others (mode %v); make it 0700", dir, fi.Mode().Perm())
	}
	if uid, ok := fsutil.OwnerUID(fi); ok && uid != os.Geteuid() {
		return fmt.Errorf("state_dir %s is not owned by the user running the updater", dir)
	}
	return nil
}

// CheckStateDir refuses a state directory that does not exist, or that is not
// the updater's own (owned by the user running this command, mode 0700). It
// never creates it: the offline revert must not invent a state directory, nor
// write one for another user.
func CheckStateDir(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("state_dir: %w (an offline revert does not create it)", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("state_dir %s is not a directory", dir)
	}
	if uid, ok := fsutil.OwnerUID(fi); ok && uid != os.Geteuid() {
		return fmt.Errorf("state_dir %s belongs to another user: run this command as the user that runs warren-update", dir)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("state_dir %s must not be accessible by group or others (mode %v); make it 0700", dir, fi.Mode().Perm())
	}
	return nil
}

// AcquireLock takes update.lock, waiting up to wait for it (0: do not wait).
// The returned file holds the lock until it is closed; the caller keeps it
// open and never takes the lock again through another descriptor.
func AcquireLock(stateDir string, wait time.Duration) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(stateDir, LockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(wait)
	for {
		err := fsutil.TryLock(f)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, fsutil.ErrLocked) {
			f.Close()
			return nil, err
		}
		if !time.Now().Before(deadline) {
			f.Close()
			return nil, fmt.Errorf("another updater holds %s: a running `warren-update run` or an offline revert", filepath.Join(stateDir, LockName))
		}
		time.Sleep(100 * time.Millisecond)
	}
}
