package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/PeterSR/warren-mesh/internal/update/manifest"
	"github.com/PeterSR/warren-mesh/internal/update/semver"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
)

// Candidate is a verified manifest read from a source directory.
type Candidate struct {
	Manifest *manifest.Manifest
	Data     []byte
	Sig      []byte
	Dir      string
}

// ManifestName is the file name of a manifest in a source: the exact release
// <component>-<version>.manifest.json when version is not empty, else the
// pointer of a channel, <component>.<channel>.manifest.json.
func ManifestName(component, channel, version string) string {
	if version != "" {
		return component + "-" + version + ".manifest.json"
	}
	return component + "." + channel + ".manifest.json"
}

// LoadFromDir reads and verifies the manifest of a component from a source
// directory (the layout in docs/updates.md, "Making a release"). With a version it reads that
// exact release (the version is validated as semver before it is put in a file
// name). Without one it reads the pointer of the component's channel, and for a
// dev component that holds a stable key the stable pointer too; the higher
// verified version wins. The directory is untrusted: only a signature made by a
// key of the component counts.
func LoadFromDir(dir string, c *uconfig.Comp, version string) (*Candidate, error) {
	var names []string
	if version != "" {
		if _, err := semver.Parse(version); err != nil {
			return nil, fmt.Errorf("--version: %w", err)
		}
		names = []string{ManifestName(c.Name, "", version)}
	} else {
		names = []string{ManifestName(c.Name, c.Channel, "")}
		if c.Channel == manifest.Dev && len(c.Keys[manifest.Stable]) > 0 {
			names = append(names, ManifestName(c.Name, manifest.Stable, ""))
		}
	}
	var best *Candidate
	var firstErr error
	for _, n := range names {
		cand, err := readPair(dir, n, c, version)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			// A pointer is a manifest and a signature in two files, which cannot be
			// replaced together: a reader that catches the pair between the two
			// renames looks again once, before it falls back to another pointer.
			Wait(RetryAfter)
			cand, err = readPair(dir, n, c, version)
		}
		if err != nil {
			if firstErr == nil || !errors.Is(err, os.ErrNotExist) {
				firstErr = err
			}
			continue
		}
		if best == nil {
			best = cand
			continue
		}
		if cmp, _ := semver.CompareStrings(cand.Manifest.Version, best.Manifest.Version); cmp > 0 {
			best = cand
		}
	}
	if best == nil {
		if firstErr == nil {
			firstErr = fmt.Errorf("no manifest for %s in %s", c.Name, dir)
		}
		return nil, firstErr
	}
	return best, nil
}

// RetryAfter is how long LoadFromDir waits before it reads a pointer again
// after a manifest and signature that do not belong together; Wait is what
// waits (tests replace it, so they need no real time).
var (
	RetryAfter = 200 * time.Millisecond
	Wait       = time.Sleep
)

// readPair reads and verifies one manifest and its signature.
func readPair(dir, n string, c *uconfig.Comp, version string) (*Candidate, error) {
	data, err := readLimited(filepath.Join(dir, n), manifest.MaxBytes)
	if err != nil {
		return nil, err
	}
	sig, err := readLimited(filepath.Join(dir, n+".minisig"), 4096)
	if err != nil {
		return nil, err
	}
	m, err := manifest.Verify(data, sig, c.Name, c.Keys, c.MaxSize)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", n, err)
	}
	if version != "" && m.Version != version {
		return nil, fmt.Errorf("%s holds version %s, not %s", n, m.Version, version)
	}
	return &Candidate{Manifest: m, Data: data, Sig: sig, Dir: dir}, nil
}

// OpenFile opens a file of a source by its manifest name hint, and checks it
// before anything is sent: the hint is a plain name (checked by the manifest),
// resolved only inside dir and never through a symlink; the file must be a
// regular file with the signed size and digest. The reader is left at the
// start. (The daemon checks again: this one only spares the upload.)
func OpenFile(dir, name string, size int64, sha string) (*os.File, error) {
	if !manifest.ValidName(name) {
		return nil, fmt.Errorf("%q is not a plain file name", name)
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() || fi.Size() != size {
		f.Close()
		return nil, fmt.Errorf("%s is not a regular file of the signed size (%d bytes)", name, size)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		f.Close()
		return nil, err
	}
	if hex.EncodeToString(h.Sum(nil)) != sha {
		f.Close()
		return nil, fmt.Errorf("%s does not match the signed digest", name)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
