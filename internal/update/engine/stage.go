package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/PeterSR/warren-mesh/internal/update/manifest"
	"github.com/PeterSR/warren-mesh/internal/update/semver"
)

// StageNeed is one file still to be uploaded.
type StageNeed struct {
	Target string `json:"target"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// StageResult answers a Stage.
type StageResult struct {
	Component string      `json:"component"`
	Version   string      `json:"version"`
	Channel   string      `json:"channel"`
	Manual    bool        `json:"manual"`
	Need      []StageNeed `json:"need"`
}

const (
	stagedManifest = "manifest.json"
	stagedSig      = "manifest.json.minisig"
)

// verifyStaged reads and verifies the manifest staged for a version, with the
// keys in force now.
func (e *Engine) verifyStaged(comp string, dir string) (*manifest.Manifest, []byte, []byte, error) {
	c, err := e.component(comp)
	if err != nil {
		return nil, nil, nil, err
	}
	data, err := readLimited(filepath.Join(dir, stagedManifest), manifest.MaxBytes)
	if err != nil {
		return nil, nil, nil, refuse("nothing is staged for this version: %v", err)
	}
	sig, err := readLimited(filepath.Join(dir, stagedSig), 4096)
	if err != nil {
		return nil, nil, nil, refuse("nothing is staged for this version: %v", err)
	}
	m, err := manifest.Verify(data, sig, comp, c.Keys, c.MaxSize)
	if err != nil {
		return nil, nil, nil, refuse("manifest refused: %v", err)
	}
	return m, data, sig, nil
}

func readLimited(path string, max int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("%s is larger than %d bytes", filepath.Base(path), max)
	}
	return b, nil
}

// Stage verifies a manifest for a component, makes its staging directory
// (0700) and says which files of this platform are still to be uploaded. The
// files are only ever accepted by digest, as named in the verified manifest.
func (e *Engine) Stage(comp string, manifestBytes, sig []byte) (*StageResult, error) {
	c, err := e.component(comp)
	if err != nil {
		return nil, err
	}
	m, err := manifest.Verify(manifestBytes, sig, comp, c.Keys, c.MaxSize)
	if err != nil {
		return nil, refuse("manifest refused: %v", err)
	}
	pl, ok := m.Platforms[Platform]
	if !ok {
		return nil, refuse("version %s has no build for %s", m.Version, Platform)
	}
	dir := e.stagingDir(comp, m.Version)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, stagedManifest), manifestBytes, 0o600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, stagedSig), sig, 0o600); err != nil {
		return nil, err
	}
	res := &StageResult{Component: comp, Version: m.Version, Channel: m.Channel, Manual: m.Manual, Need: []StageNeed{}}
	for _, t := range sortedKeys(pl.Files) {
		f := pl.Files[t]
		// Already staged only if the bytes on disk have the signed digest, not
		// just the signed size.
		if stagedOK(filepath.Join(dir, f.SHA256), f) {
			continue
		}
		res.Need = append(res.Need, StageNeed{Target: t, SHA256: f.SHA256, Size: f.Size})
	}
	return res, nil
}

// stagedOK reports whether the file at path is a regular file with the signed
// size and digest.
func stagedOK(path string, f manifest.File) bool {
	fi, err := os.Lstat(path)
	if err != nil || !fi.Mode().IsRegular() || fi.Size() != f.Size {
		return false
	}
	sum, err := hashFile(path)
	return err == nil && sum == f.SHA256
}

// ReceiveFile takes the bytes of one staged file: only a digest named in the
// staged manifest for this platform, stopped at the signed size, hashed as it
// arrives, and kept only when size and digest are the signed ones.
func (e *Engine) ReceiveFile(comp, version, sha string, r io.Reader) error {
	if _, err := semver.Parse(version); err != nil {
		return refuse("bad version: %v", err)
	}
	dir := e.stagingDir(comp, version)
	m, _, _, err := e.verifyStaged(comp, dir)
	if err != nil {
		return err
	}
	var want *manifest.File
	for _, f := range m.Platforms[Platform].Files {
		if f.SHA256 == sha {
			f := f
			want = &f
		}
	}
	if want == nil {
		return refuse("digest %s is not named in the manifest for %s", sha, Platform)
	}
	// A part file of its own for each writer, so two uploads of the same file
	// never share one.
	f, err := os.CreateTemp(dir, sha+".*.part")
	if err != nil {
		return err
	}
	part := f.Name()
	if err := f.Chmod(0o700); err != nil {
		f.Close()
		os.Remove(part)
		return err
	}
	ok := false
	defer func() {
		if !ok {
			f.Close()
			os.Remove(part)
		}
	}()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(r, want.Size+1))
	if err != nil {
		return err
	}
	if n != want.Size {
		return refuse("the file has %d bytes (or more), the manifest signs %d", n, want.Size)
	}
	if hex.EncodeToString(h.Sum(nil)) != sha {
		return refuse("the bytes do not match digest %s", sha)
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	ok = true
	final := filepath.Join(dir, sha)
	if err := os.Rename(part, final); err != nil {
		os.Remove(part)
		return err
	}
	// Checked again at the name it will be used by.
	if !stagedOK(final, *want) {
		os.Remove(final)
		return refuse("the staged file does not match the signed size and digest")
	}
	return nil
}

// CleanStaging removes staging directories older than a day (a crashed
// upload) at start.
func (e *Engine) CleanStaging(olderThan func(os.FileInfo) bool) {
	root := filepath.Join(e.config().StateDir, "staging")
	comps, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, c := range comps {
		vers, _ := os.ReadDir(filepath.Join(root, c.Name()))
		for _, v := range vers {
			if fi, err := v.Info(); err == nil && olderThan(fi) {
				clearDir(filepath.Join(root, c.Name(), v.Name()))
			}
		}
	}
}
