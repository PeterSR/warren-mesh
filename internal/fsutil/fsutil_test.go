package fsutil

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteAtomicReplacesTheFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f.json")
	for _, want := range []string{"one", "two"} {
		if err := WriteAtomic(p, []byte(want), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, _ := os.ReadFile(p); string(got) != want {
			t.Errorf("got %q want %q", got, want)
		}
	}
	if fi, _ := os.Stat(p); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v", fi.Mode().Perm())
	}
	if ents, _ := os.ReadDir(filepath.Dir(p)); len(ents) != 1 {
		t.Errorf("a temporary file was left behind: %v", ents)
	}
}

// The temporary file is made exclusively: a link at a predictable name is not
// followed.
func TestWriteAtomicIgnoresALinkAtTheOldTemporaryName(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim")
	p := filepath.Join(dir, "names.json")
	if err := os.WriteFile(victim, []byte("precious"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{p + ".tmp", p + ".tmp-1"} {
		if err := os.Symlink(victim, name); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteAtomic(p, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(victim); string(b) != "precious" {
		t.Errorf("the file behind the link was written: %q", b)
	}
	if b, _ := os.ReadFile(p); string(b) != "replacement" {
		t.Errorf("names.json is %q", b)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 4 {
		t.Errorf("a temporary file was left behind: %v", ents)
	}
}

func TestWriteAtomicFuncLeavesNothingWhenFillFails(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	if err := os.WriteFile(p, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	err := WriteAtomicFunc(p, 0o644, func(w io.Writer) error {
		w.Write([]byte("half"))
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err %v", err)
	}
	if b, _ := os.ReadFile(p); string(b) != "old" {
		t.Errorf("the old file was changed: %q", b)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 1 {
		t.Errorf("leftovers: %v", ents)
	}
}

func TestWriteNoClobber(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "k")
	if err := WriteNoClobber(p, []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteNoClobber(p, []byte("two"), 0o600); !errors.Is(err, os.ErrExist) {
		t.Fatalf("err %v, want it to say the file exists", err)
	}
	if b, _ := os.ReadFile(p); string(b) != "one" {
		t.Errorf("content %q", b)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 1 {
		t.Errorf("leftovers: %v", ents)
	}
}

func TestWriteNoClobberRemovesTheFileWhenTheDirectorySyncFails(t *testing.T) {
	dir := t.TempDir()
	err := WriteNoClobberSync(filepath.Join(dir, "k"), []byte("one"), 0o600, func(string) error { return os.ErrInvalid })
	if err == nil {
		t.Fatal("no error")
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Errorf("left behind: %v", ents)
	}
}

func TestWriteExclusiveRefusesAnExistingNameAndASymlink(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim")
	os.WriteFile(victim, []byte("precious"), 0o600)
	link := filepath.Join(dir, "l")
	os.Symlink(victim, link)
	fill := func(w io.Writer) error { _, err := w.Write([]byte("x")); return err }
	if err := WriteExclusive(link, 0o755, fill); !errors.Is(err, os.ErrExist) {
		t.Errorf("a symlink: err %v", err)
	}
	if b, _ := os.ReadFile(victim); string(b) != "precious" {
		t.Errorf("the file behind the link was written: %q", b)
	}
	if err := WriteExclusive(victim, 0o755, fill); !errors.Is(err, os.ErrExist) {
		t.Errorf("an existing file: err %v", err)
	}
	if b, _ := os.ReadFile(victim); string(b) != "precious" {
		t.Errorf("an existing file was removed or changed: %q", b)
	}
}

func TestWriteExclusiveModeAndFailure(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.new")
	if err := WriteExclusive(p, 0o755, func(w io.Writer) error { _, err := w.Write([]byte("x")); return err }); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(p); fi.Mode().Perm() != 0o755 {
		t.Errorf("mode %v", fi.Mode().Perm())
	}
	q := filepath.Join(dir, "b.new")
	if err := WriteExclusive(q, 0o755, func(io.Writer) error { return os.ErrInvalid }); err == nil {
		t.Fatal("no error")
	}
	if _, err := os.Lstat(q); err == nil {
		t.Error("a failed write left its file")
	}
}

func TestSHA256(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f")
	os.WriteFile(p, []byte("abc"), 0o600)
	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got, err := SHA256File(p); err != nil || got != want {
		t.Errorf("file: %q %v", got, err)
	}
	if got, err := SHA256Reader(strings.NewReader("abc")); err != nil || got != want {
		t.Errorf("reader: %q %v", got, err)
	}
	if _, err := SHA256File(p + "x"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("missing: %v", err)
	}
}

func TestOwnerUIDAndTryLock(t *testing.T) {
	dir := t.TempDir()
	fi, _ := os.Stat(dir)
	if uid, ok := OwnerUID(fi); !ok || uid != os.Geteuid() {
		t.Errorf("owner %d %v", uid, ok)
	}
	p := filepath.Join(dir, "l")
	a, _ := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o600)
	b, _ := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o600)
	defer a.Close()
	defer b.Close()
	if err := TryLock(a); err != nil {
		t.Fatal(err)
	}
	if err := TryLock(b); !errors.Is(err, ErrLocked) {
		t.Errorf("a second holder: %v", err)
	}
	a.Close()
	if err := TryLock(b); err != nil {
		t.Errorf("after release: %v", err)
	}
}
