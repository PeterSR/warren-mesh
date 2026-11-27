// Command warren-release makes and checks signed releases for warren-update. It
// holds all the signing code: the updater only verifies.
//
//	warren-release keygen --component myapp --channel dev [--dir D]
//	warren-release publish --component myapp --channel dev --version V
//	                       --file myapp=linux/amd64=PATH ... --key K --out dist
//	warren-release verify --key K.pub [--component myapp] [--max-size BYTES] DIR
//
// Keys are minisign keys (encrypted, mode 0600) and the signatures verify with
// the stock `minisign -V`. See docs/updates.md.
package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"aead.dev/minisign"
	"golang.org/x/term"

	"github.com/PeterSR/warren-mesh/internal/update/manifest"
	"github.com/PeterSR/warren-mesh/internal/update/semver"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
	"github.com/PeterSR/warren-mesh/internal/version"
)

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func run(args []string, in io.Reader, out, errw io.Writer) int {
	if len(args) == 0 {
		usage(errw)
		return 2
	}
	var err error
	switch args[0] {
	case "keygen":
		err = keygen(args[1:], in, out, errw)
	case "publish":
		err = publish(args[1:], in, out, errw)
	case "verify":
		err = verify(args[1:], out, errw)
	case "version":
		fmt.Fprintln(out, version.Version)
		return 0
	default:
		usage(errw)
		return 2
	}
	var u *usageError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &u):
		fmt.Fprintln(errw, "warren-release:", err)
		return 2
	default:
		fmt.Fprintln(errw, "warren-release:", err)
		return 1
	}
}

type usageError struct{ msg string }

func (e *usageError) Error() string { return e.msg }

func usage(w io.Writer) {
	fmt.Fprint(w, `usage: warren-release <command> [flags]

  keygen --component C --channel dev|stable [--dir D] [--password-file F | --no-password]
      make an encrypted minisign key pair C-<channel>.key and .pub, and print the
      config line. The password comes from --password-file, else the environment
      variable WARREN_RELEASE_PASSWORD, else a prompt.
  publish --component C --channel dev|stable --version V
          --file TARGET=GOOS/GOARCH=PATH ... --key KEYFILE --out DIR [--manual]
          [--max-size BYTES] [--password-file F] [--allow-downgrade]
      write the manifest, its signature and the files into DIR (a valid source)
  verify --key PUBFILE [--component C] [--max-size BYTES] DIR
      check what an updater would: signatures, binding, and every file's size and digest
  version
      print the version
`)
}

func flagSet(name string, errw io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("warren-release "+name, flag.ContinueOnError)
	fs.SetOutput(errw)
	return fs
}

func defaultKeyDir() string {
	if h, err := os.UserHomeDir(); err == nil {
		return filepath.Join(h, ".config", "warren", "keys")
	}
	return "."
}

// password finds the key password: a file, the environment, or a prompt.
func password(file string, in io.Reader, errw io.Writer, confirm bool) (string, error) {
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}
		return strings.TrimRight(string(b), "\r\n"), nil
	}
	if v, ok := os.LookupEnv("WARREN_RELEASE_PASSWORD"); ok {
		return v, nil
	}
	f, isFile := in.(*os.File)
	if isFile && term.IsTerminal(int(f.Fd())) {
		fmt.Fprint(errw, "key password: ")
		b, err := term.ReadPassword(int(f.Fd()))
		fmt.Fprintln(errw)
		if err != nil {
			return "", err
		}
		if confirm {
			fmt.Fprint(errw, "again: ")
			b2, err := term.ReadPassword(int(f.Fd()))
			fmt.Fprintln(errw)
			if err != nil {
				return "", err
			}
			if !bytes.Equal(b, b2) {
				return "", errors.New("the passwords differ")
			}
		}
		return string(b), nil
	}
	b, err := io.ReadAll(io.LimitReader(in, 4096))
	if err != nil {
		return "", err
	}
	return strings.TrimRight(strings.SplitN(string(b), "\n", 2)[0], "\r"), nil
}

func keygen(args []string, in io.Reader, out, errw io.Writer) error {
	fs := flagSet("keygen", errw)
	comp := fs.String("component", "", "component the key signs for")
	channel := fs.String("channel", "", "stable or dev")
	dir := fs.String("dir", defaultKeyDir(), "where to write the key files")
	pwFile := fs.String("password-file", "", "read the password from this file")
	noPw := fs.Bool("no-password", false, "write an unencrypted key (for tests and unattended CI that guards the file otherwise)")
	if err := fs.Parse(args); err != nil {
		return &usageError{err.Error()}
	}
	if !manifest.ValidComponent(*comp) {
		return &usageError{"--component must be a label (lower case letters, digits, dash, underscore; at most 32 bytes)"}
	}
	if *channel != manifest.Stable && *channel != manifest.Dev {
		return &usageError{"--channel must be stable or dev"}
	}
	if err := os.MkdirAll(*dir, 0o700); err != nil {
		return err
	}
	base := filepath.Join(*dir, *comp+"-"+*channel)
	for _, p := range []string{base + ".key", base + ".pub"} {
		if _, err := os.Lstat(p); err == nil {
			return fmt.Errorf("%s exists; not overwriting a key", p)
		}
	}
	var pw string
	if !*noPw {
		var err error
		if pw, err = password(*pwFile, in, errw, true); err != nil {
			return err
		}
		if pw == "" {
			return &usageError{"an empty password is refused; use --no-password to write an unencrypted key on purpose"}
		}
	}
	pub, priv, err := minisign.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	var keyBytes []byte
	if *noPw {
		keyBytes, err = priv.MarshalText()
	} else {
		keyBytes, err = minisign.EncryptKey(pw, priv)
	}
	if err != nil {
		return err
	}
	pubBytes, _ := pub.MarshalText()
	if afterKey != nil {
		afterKey(base)
	}
	if err := writeNoClobber(base+".key", append(keyBytes, '\n'), 0o600); err != nil {
		return err
	}
	if err := writeNoClobber(base+".pub", append(pubBytes, '\n'), 0o644); err != nil {
		os.Remove(base + ".pub") // a half written file, if any
		os.Remove(base + ".key") // never leave a key without its public half
		return err
	}
	fmt.Fprintf(out, "wrote %s.key (private, keep it safe) and %s.pub\n", base, base)
	fmt.Fprintf(out, "updater config, under components.%s:\n  keys: {%s: [%s]}\n", *comp, *channel, pub.String())
	return nil
}

// writeNoClobber writes a file through a temporary file in the same directory
// and links it into place, so a failed write leaves nothing behind and an
// existing file is never replaced; the directory is synced.
func writeNoClobber(path string, b []byte, mode os.FileMode) error {
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
	if err := syncDirFn(dir); err != nil {
		os.Remove(path) // not durable: leave nothing behind
		return err
	}
	return nil
}

// syncDirFn is syncDir; tests replace it.
var syncDirFn = syncDir

func loadPrivateKey(path, pwFile string, in io.Reader, errw io.Writer) (minisign.PrivateKey, error) {
	var zero minisign.PrivateKey
	b, err := os.ReadFile(path)
	if err != nil {
		return zero, err
	}
	if fi, err := os.Stat(path); err == nil && fi.Mode().Perm()&0o077 != 0 {
		return zero, fmt.Errorf("%s is accessible by group or others (mode %v); chmod 600 it", path, fi.Mode().Perm())
	}
	if !minisign.IsEncrypted(b) {
		var k minisign.PrivateKey
		if err := k.UnmarshalText(b); err != nil {
			return zero, fmt.Errorf("%s: %w", path, err)
		}
		return k, nil
	}
	pw, err := password(pwFile, in, errw, false)
	if err != nil {
		return zero, err
	}
	k, err := minisign.DecryptKey(pw, b)
	if err != nil {
		return zero, fmt.Errorf("%s: cannot decrypt (wrong password?)", path)
	}
	return k, nil
}

// fileSpec is one --file TARGET=GOOS/GOARCH=PATH.
type fileSpec struct{ target, platform, path string }

func parseFileSpec(s string) (fileSpec, error) {
	parts := strings.SplitN(s, "=", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return fileSpec{}, fmt.Errorf("--file %q: want TARGET=GOOS/GOARCH=PATH", s)
	}
	return fileSpec{parts[0], parts[1], parts[2]}, nil
}

func publish(args []string, in io.Reader, out, errw io.Writer) error {
	fs := flagSet("publish", errw)
	comp := fs.String("component", "", "component")
	channel := fs.String("channel", "", "stable or dev")
	version := fs.String("version", "", "version (semver; a dev release must be a pre-release)")
	var files multiFlag
	fs.Var(&files, "file", "TARGET=GOOS/GOARCH=PATH (repeatable)")
	keyPath := fs.String("key", "", "the private key file")
	outDir := fs.String("out", "dist", "the directory to write the source layout into")
	manual := fs.Bool("manual", false, "sign the release `manual`: never installed by auto")
	maxSize := fs.Int64("max-size", uconfig.DefaultMaxSize, "the most a file may be")
	pwFile := fs.String("password-file", "", "read the key password from this file")
	allowDown := fs.Bool("allow-downgrade", false, "move the channel pointer to a lower version than it holds now")
	if err := fs.Parse(args); err != nil {
		return &usageError{err.Error()}
	}
	if *keyPath == "" || len(files) == 0 || *version == "" {
		return &usageError{"publish needs --version, --key and at least one --file"}
	}
	m := &manifest.Manifest{Format: manifest.Format, Component: *comp, Version: *version, Channel: *channel,
		Created: time.Now().UTC().Format(time.RFC3339), Manual: *manual, Platforms: map[string]manifest.Platform{}}
	blobs := map[string]string{} // published file name -> source path
	var specs []fileSpec
	for _, f := range files {
		sp, err := parseFileSpec(f)
		if err != nil {
			return &usageError{err.Error()}
		}
		specs = append(specs, sp)
		fi, err := os.Stat(sp.path)
		if err != nil {
			return err
		}
		if !fi.Mode().IsRegular() {
			return fmt.Errorf("%s is not a regular file", sp.path)
		}
		sum, err := sha256File(sp.path)
		if err != nil {
			return err
		}
		pl := m.Platforms[sp.platform]
		if pl.Files == nil {
			pl.Files = map[string]manifest.File{}
		}
		if _, dup := pl.Files[sp.target]; dup {
			return fmt.Errorf("target %s is given twice for %s", sp.target, sp.platform)
		}
		name := fileName(sp.target, *version, sp.platform)
		pl.Files[sp.target] = manifest.File{Name: name, Size: fi.Size(), SHA256: sum}
		m.Platforms[sp.platform] = pl
		blobs[name] = sp.path
	}
	// Checks: the manifest's own (dev is a pre-release, same targets on every
	// platform, sizes), then the version guard.
	if err := m.Check(*maxSize); err != nil {
		return err
	}
	if err := versionGuard(m, specs); err != nil {
		return err
	}
	if !*allowDown {
		if err := checkPointer(*outDir, m); err != nil {
			return err
		}
	}
	data, err := m.Marshal()
	if err != nil {
		return err
	}
	key, err := loadPrivateKey(*keyPath, *pwFile, in, errw)
	if err != nil {
		return err
	}
	sig := manifest.Sign(key, data, m)
	// What an updater would check, before anything is written.
	pub, _ := key.Public().(minisign.PublicKey)
	k, err := manifest.ParseKey(pub.String())
	if err != nil {
		return err
	}
	if _, err := manifest.Verify(data, sig, m.Component, manifest.Keys{m.Channel: {k}}, *maxSize); err != nil {
		return fmt.Errorf("the signed manifest does not verify: %w", err)
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return err
	}
	if beforeCopy != nil {
		beforeCopy()
	}
	// The files are hashed again while they are copied: the password prompt may
	// have taken a while, and what is published must be what was signed.
	for name, src := range blobs {
		var want manifest.File
		for _, p := range m.Platforms {
			for _, f := range p.Files {
				if f.Name == name {
					want = f
				}
			}
		}
		if err := copyVerified(src, filepath.Join(*outDir, name), want); err != nil {
			return err
		}
	}
	// The exact release first, the mutable pointer last: the pointer never names
	// files that are not there yet. Each pair is written signature first and
	// manifest last (both by rename). A reader that catches a pointer between
	// the two renames sees a manifest and signature that do not belong together
	// and fails to verify; the updater looks again once.
	for _, n := range []string{manifestFile(m.Component, "", m.Version), manifestFile(m.Component, m.Channel, "")} {
		if err := writeAtomic(filepath.Join(*outDir, n+".minisig"), sig); err != nil {
			return err
		}
		if err := writeAtomic(filepath.Join(*outDir, n), data); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "published %s %s (%s) to %s: %d platform(s), signed by key %X\n", m.Component, m.Version, m.Channel, *outDir, len(m.Platforms), key.ID())
	return nil
}

// checkPointer refuses to move the channel pointer in outDir to a lower
// version than the one it names now: a reader would be moved back by a
// publish, which only --allow-downgrade may do. The old pointer is read, not
// trusted; one that cannot be read does not stop a publish.
func checkPointer(outDir string, m *manifest.Manifest) error {
	b, err := os.ReadFile(filepath.Join(outDir, manifestFile(m.Component, m.Channel, "")))
	if err != nil {
		return nil
	}
	old, err := manifest.Parse(b)
	if err != nil {
		return nil
	}
	if c, err := semver.CompareStrings(m.Version, old.Version); err == nil && c < 0 {
		return fmt.Errorf("the %s pointer in %s names %s, which is newer than %s; pass --allow-downgrade to move it back", m.Channel, outDir, old.Version, m.Version)
	}
	return nil
}

func manifestFile(component, channel, version string) string {
	if version != "" {
		return component + "-" + version + ".manifest.json"
	}
	return component + "." + channel + ".manifest.json"
}

// fileName is the name hint of a published file: <target>_<version>_<os>_<arch>.
func fileName(target, version, platform string) string {
	return target + "_" + version + "_" + strings.ReplaceAll(platform, "/", "_")
}

// versionGuard, for warren and warren-update, runs the host-platform binary
// with `version` and refuses when its output differs from --version.
func versionGuard(m *manifest.Manifest, specs []fileSpec) error {
	if m.Component != "warren" && m.Component != "warren-update" {
		return nil
	}
	host := runtime.GOOS + "/" + runtime.GOARCH
	for _, sp := range specs {
		if sp.platform != host || sp.target != m.Component {
			continue
		}
		abs, err := filepath.Abs(sp.path)
		if err != nil {
			return err
		}
		outb, err := exec.Command(abs, "version").Output()
		if err != nil {
			return fmt.Errorf("version guard: running %s version: %w", sp.path, err)
		}
		if got := strings.TrimSpace(string(outb)); got != m.Version {
			return fmt.Errorf("version guard: %s reports version %q, not %q", sp.path, got, m.Version)
		}
		return nil
	}
	return fmt.Errorf("version guard: no %s binary for the host platform %s among the --file arguments", m.Component, host)
}

func sha256File(path string) (string, error) {
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

// afterKey is called after the private key is written and before the public one (tests).
var afterKey func(base string)

// beforeCopy is called after signing and before the files are copied (tests).
var beforeCopy func()

// copyVerified copies src to dst while hashing it, and refuses (leaving
// nothing at dst) when the bytes are not the size and digest that were signed.
func copyVerified(src, dst string, want manifest.File) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".part-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), in)
	if err != nil {
		tmp.Close()
		return err
	}
	if n != want.Size || hex.EncodeToString(h.Sum(nil)) != want.SHA256 {
		tmp.Close()
		return fmt.Errorf("%s changed after it was signed (%d bytes, digest %s); nothing was published", src, n, hex.EncodeToString(h.Sum(nil))[:12])
	}
	if err := tmp.Chmod(0o644); err != nil {
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
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dst))
}

func writeAtomic(path string, b []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".part-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o644); err != nil {
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
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func verify(args []string, out, errw io.Writer) error {
	fs := flagSet("verify", errw)
	keyPath := fs.String("key", "", "the public key file (or the bare RW... key)")
	comp := fs.String("component", "", "check only this component's manifests")
	maxSize := fs.Int64("max-size", uconfig.DefaultMaxSize, "the most a file may be")
	if err := fs.Parse(args); err != nil {
		return &usageError{err.Error()}
	}
	if fs.NArg() != 1 || *keyPath == "" {
		return &usageError{"verify needs --key and one directory"}
	}
	dir := fs.Arg(0)
	k, err := readPublicKey(*keyPath)
	if err != nil {
		return err
	}
	names, err := filepath.Glob(filepath.Join(dir, "*.manifest.json"))
	if err != nil {
		return err
	}
	checked, failed := 0, 0
	for _, path := range names {
		base := filepath.Base(path)
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		m, err := manifest.Parse(data)
		if err != nil {
			fmt.Fprintf(out, "FAIL %s: %v\n", base, err)
			failed++
			continue
		}
		if *comp != "" && m.Component != *comp {
			continue
		}
		checked++
		if err := verifyOne(dir, path, data, k, m, *maxSize); err != nil {
			fmt.Fprintf(out, "FAIL %s: %v\n", base, err)
			failed++
			continue
		}
		fmt.Fprintf(out, "ok   %s: %s %s (%s), %d platform(s)\n", base, m.Component, m.Version, m.Channel, len(m.Platforms))
	}
	if checked == 0 {
		return fmt.Errorf("no manifests found in %s", dir)
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d manifests failed", failed, checked)
	}
	return nil
}

func verifyOne(dir, path string, data []byte, k manifest.Key, m *manifest.Manifest, maxSize int64) error {
	sig, err := os.ReadFile(path + ".minisig")
	if err != nil {
		return err
	}
	if _, err := manifest.Verify(data, sig, m.Component, manifest.Keys{m.Channel: {k}}, maxSize); err != nil {
		return err
	}
	// The file name must say what the manifest says.
	base := filepath.Base(path)
	if base != manifestFile(m.Component, "", m.Version) && base != manifestFile(m.Component, m.Channel, "") {
		return fmt.Errorf("the file name does not match the component %s, channel %s and version %s inside", m.Component, m.Channel, m.Version)
	}
	for _, p := range m.SortedPlatforms() {
		for _, t := range sortedFileKeys(m.Platforms[p].Files) {
			f := m.Platforms[p].Files[t]
			fi, err := os.Stat(filepath.Join(dir, f.Name))
			if err != nil {
				return fmt.Errorf("%s %s: %w", p, t, err)
			}
			if fi.Size() != f.Size {
				return fmt.Errorf("%s %s: %d bytes, the manifest signs %d", p, t, fi.Size(), f.Size)
			}
			sum, err := sha256File(filepath.Join(dir, f.Name))
			if err != nil {
				return err
			}
			if sum != f.SHA256 {
				return fmt.Errorf("%s %s: digest does not match", p, t)
			}
		}
	}
	return nil
}

func sortedFileKeys(m map[string]manifest.File) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func readPublicKey(s string) (manifest.Key, error) {
	if strings.HasPrefix(s, "RW") {
		if k, err := manifest.ParseKey(s); err == nil {
			return k, nil
		}
	}
	pk, err := minisign.PublicKeyFromFile(s)
	if err != nil {
		return manifest.Key{}, err
	}
	return manifest.ParseKey(pk.String())
}
