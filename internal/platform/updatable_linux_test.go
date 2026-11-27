package platform

import (
	"os"
	"path/filepath"
	"testing"
)

// The real access check, on a read-only file and on a read-only directory.
func TestDefaultEnvSeesReadOnlyFilesAndDirectories(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write anything")
	}
	dir := filepath.Join(t.TempDir(), "bin")
	os.Mkdir(dir, 0o755)
	exe := filepath.Join(dir, "svc")
	os.WriteFile(exe, []byte("x"), 0o755)
	e := DefaultUpdatableEnv()
	e.ContainerFiles = nil
	if ok, reason, known := e.Check(exe); !ok || reason != "" || !known {
		t.Fatalf("writable: %v %q %v", ok, reason, known)
	}
	os.Chmod(exe, 0o555)
	if ok, reason, _ := e.Check(exe); ok || reason != ReasonReadOnly {
		t.Errorf("read-only file: %v %q", ok, reason)
	}
	os.Chmod(exe, 0o755)
	os.Chmod(dir, 0o555)
	defer os.Chmod(dir, 0o755)
	if ok, reason, _ := e.Check(exe); ok || reason != ReasonReadOnly {
		t.Errorf("read-only directory: %v %q", ok, reason)
	}
	// The real container branch: a container file exists and the binary's
	// directory is on the device of the root path.
	os.Chmod(dir, 0o755)
	marker := filepath.Join(dir, "marker")
	os.WriteFile(marker, nil, 0o600)
	e.ContainerFiles = []string{marker}
	e.Root = dir
	if ok, reason, _ := e.Check(exe); ok || reason != ReasonContainer {
		t.Errorf("container: %v %q", ok, reason)
	}
}
