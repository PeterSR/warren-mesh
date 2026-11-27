package platform

import (
	"syscall"
	"testing"
)

// env is a fake system: access to the paths in denied fails with EACCES (or
// the error in failWith), every path has device 1 unless devs says otherwise,
// and a container file exists only when devs names it.
func env(denied map[string]bool, devs map[string]uint64) UpdatableEnv {
	return UpdatableEnv{
		ContainerFiles: []string{"/.dockerenv", "/run/.containerenv"},
		Root:           "/",
		Access: func(p string, mode uint32) error {
			if denied[p] {
				return syscall.EACCES
			}
			return nil
		},
		Dev: func(p string) (uint64, error) {
			if d, ok := devs[p]; ok {
				return d, nil
			}
			if p == "/.dockerenv" || p == "/run/.containerenv" {
				return 0, syscall.ENOENT
			}
			return 1, nil
		},
	}
}

func TestUpdatableBranches(t *testing.T) {
	const exe = "/opt/x/bin/svc"
	const dir = "/opt/x/bin"
	cases := []struct {
		name   string
		e      UpdatableEnv
		ok     bool
		reason string
		known  bool
	}{
		{"plain", env(nil, nil), true, "", true},
		{"read-only file", env(map[string]bool{exe: true}, nil), false, ReasonReadOnly, true},
		{"read-only directory", env(map[string]bool{dir: true}, nil), false, ReasonReadOnly, true},
		{"container, own layer (docker)", env(nil, map[string]uint64{"/.dockerenv": 1, dir: 5, exe: 5, "/": 5}), false, ReasonContainer, true},
		{"container, own layer (podman)", env(nil, map[string]uint64{"/run/.containerenv": 1, dir: 5, exe: 5, "/": 5}), false, ReasonContainer, true},
		{"container, on a volume", env(nil, map[string]uint64{"/.dockerenv": 1, dir: 6, exe: 6, "/": 5}), true, "", true},
		{"same device but no container", env(nil, map[string]uint64{dir: 5, exe: 5, "/": 5}), true, "", true},
		// A file that overlayfs without xino gives another device than its layer
		// is still in the container's layer: the directory is compared.
		{"container, file device differs from the layer (overlayfs without xino)", env(nil, map[string]uint64{"/.dockerenv": 1, dir: 5, exe: 8, "/": 5}), false, ReasonContainer, true},
		{"single-file bind mount", env(nil, map[string]uint64{dir: 5, exe: 9}), false, ReasonMountPoint, true},
		{"read-only before mount-point", env(map[string]bool{exe: true}, map[string]uint64{dir: 5, exe: 9}), false, ReasonReadOnly, true},
		{"a bind-mounted file in a container is a mount-point on a volume", env(nil, map[string]uint64{"/.dockerenv": 1, dir: 6, exe: 9, "/": 5}), false, ReasonMountPoint, true},
		{"read-only before container", env(map[string]bool{exe: true}, map[string]uint64{"/.dockerenv": 1, dir: 5, exe: 5, "/": 5}), false, ReasonReadOnly, true},
		{"unknown", UpdatableEnv{}, false, "", false},
	}
	for _, c := range cases {
		ok, reason, known := c.e.Check(exe)
		if ok != c.ok || reason != c.reason || known != c.known {
			t.Errorf("%s: got %v %q %v", c.name, ok, reason, known)
		}
	}
}

// Only EACCES, EROFS and EPERM mean read-only; any other error says nothing.
func TestAccessErrorsOtherThanDenialAreUnknown(t *testing.T) {
	const exe = "/opt/x/bin/svc"
	for errno, want := range map[syscall.Errno]struct {
		reason string
		known  bool
	}{
		syscall.EACCES: {ReasonReadOnly, true},
		syscall.EROFS:  {ReasonReadOnly, true},
		syscall.EPERM:  {ReasonReadOnly, true},
		syscall.ENOENT: {"", false},
		syscall.EIO:    {"", false},
		syscall.ELOOP:  {"", false},
	} {
		for _, onDir := range []bool{false, true} {
			e := env(nil, nil)
			e.Access = func(p string, mode uint32) error {
				if (p == exe) != onDir {
					return errno
				}
				return nil
			}
			ok, reason, known := e.Check(exe)
			if ok || reason != want.reason || known != want.known {
				t.Errorf("%v (dir %v): got %v %q %v", errno, onDir, ok, reason, known)
			}
		}
	}
	// In a container whose root cannot be looked at: unknown, not updatable.
	e := env(nil, map[string]uint64{"/.dockerenv": 1})
	e.Dev = func(p string) (uint64, error) {
		if p == "/" || p == "/opt/x/bin/svc" {
			return 0, syscall.EIO
		}
		return 1, nil
	}
	if _, _, known := e.Check("/opt/x/bin/svc"); known {
		t.Error("unknown root device was known")
	}
	// A missing device is unknown too.
	e = env(nil, nil)
	e.Dev = func(string) (uint64, error) { return 0, syscall.ENOENT }
	if _, _, known := e.Check(exe); known {
		t.Error("an unreadable device is known")
	}
}

func TestDirOf(t *testing.T) {
	for in, want := range map[string]string{"/a/b": "/a", "/a": "/", "a": ".", "/a/b/c": "/a/b"} {
		if got := dirOf(in); got != want {
			t.Errorf("dirOf(%q) = %q, want %q", in, got, want)
		}
	}
}
