package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/PeterSR/warren-mesh/internal/update/state"
)

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// hashTargets hashes every target that exists; a missing one is left out.
func hashTargets(targets map[string]string) (map[string]string, error) {
	out := map[string]string{}
	for t, p := range targets {
		h, err := hashFile(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[t] = h
	}
	return out, nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sameFiles(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// mode is the mode of a replacement: the existing file's mode masked to 0755,
// so setuid and setgid bits are never set.
func modeFor(path string) os.FileMode {
	if fi, err := os.Lstat(path); err == nil {
		return fi.Mode().Perm() & 0o755
	}
	return 0o755
}

// writeNew writes <target>.new exclusively from src while hashing, and
// fails if the bytes are not the signed ones.
func writeNew(target string, src io.Reader, size int64, wantSHA string) error {
	dst := target + ".new"
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
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(src, size+1))
	if err != nil {
		return err
	}
	if n != size {
		return fmt.Errorf("%s: %d bytes, the manifest signs %d", filepath.Base(target), n, size)
	}
	if hex.EncodeToString(h.Sum(nil)) != wantSHA {
		return fmt.Errorf("%s: the bytes do not match the signed digest", filepath.Base(target))
	}
	if err := f.Chmod(modeFor(target)); err != nil {
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

// linkPrev makes <target>.prev.new a hard link to the current target, or a
// copy where links are unavailable.
func linkPrev(target string) error {
	dst := target + ".prev.new"
	os.Remove(dst)
	if err := os.Link(target, dst); err == nil {
		return nil
	}
	src, err := os.Open(target)
	if err != nil {
		return err
	}
	defer src.Close()
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, src); err != nil {
		f.Close()
		os.Remove(dst)
		return err
	}
	if err := f.Chmod(modeFor(target)); err != nil {
		f.Close()
		os.Remove(dst)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(dst)
		return err
	}
	return f.Close()
}

// restorePrev renames <target>.prev over the target after checking that it is
// a regular file (opened without following a symlink) with the expected hash.
func restorePrev(target, wantSHA string) error {
	prev := target + ".prev"
	f, err := os.OpenFile(prev, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("%s: %w", filepath.Base(prev), err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", filepath.Base(prev))
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != wantSHA {
		return fmt.Errorf("%s does not hold the previous version", filepath.Base(prev))
	}
	if err := os.Rename(prev, target); err != nil {
		return err
	}
	return state.SyncDir(filepath.Dir(target))
}

// cleanLeftovers removes <target>.new and <target>.prev.new.
func cleanLeftovers(targets map[string]string) {
	for _, p := range targets {
		os.Remove(p + ".new")
		os.Remove(p + ".prev.new")
	}
}

// freeBytes is the space available to an unprivileged user on the file system
// of dir.
func freeBytes(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil //nolint:unconvert // the field types differ between systems
}

func sum256(b []byte) [32]byte { return sha256.Sum256(b) }

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }
