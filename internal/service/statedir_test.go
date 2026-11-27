package service

import (
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A state_dir that group or others may write to is refused at start.
func TestStateDirOthersMayWriteIsRefused(t *testing.T) {
	quick(t)
	dir := tmpDir(t)
	cfg := parseYAML(t, dir, baseYAML)
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, m := range []os.FileMode{0o770, 0o707, 0o777} {
		if err := os.Chmod(cfg.StateDir, m); err != nil {
			t.Fatal(err)
		}
		n, err := New(Options{Config: cfg, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if err == nil {
			err = n.Start()
			if err == nil {
				n.Close()
			}
		}
		if err == nil {
			t.Errorf("state_dir %o was accepted", m)
		} else if !strings.Contains(err.Error(), "state_dir") {
			t.Errorf("state_dir %o: %v", m, err)
		}
	}
}

// What a Warren that died while writing names.json left is removed at start.
func TestLeftoverNamesTempFilesAreRemovedAtStart(t *testing.T) {
	quick(t)
	dir := tmpDir(t)
	cfg := parseYAML(t, dir, baseYAML)
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"names.json.tmp", "names.json.tmp-123", ".names.json.tmp-456"} {
		if err := os.WriteFile(filepath.Join(cfg.StateDir, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	n, err := New(Options{Config: cfg, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	// Check the planted names only: a running node writes names.json through
	// temporary files of its own.
	for _, f := range []string{"names.json.tmp", "names.json.tmp-123", ".names.json.tmp-456"} {
		if _, err := os.Lstat(filepath.Join(cfg.StateDir, f)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s left (%v)", f, err)
		}
	}
}
