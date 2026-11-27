// Package manifest is the release manifest of the update system
// ("warren-manifest/1"): its strict parsing and checks, and the verification
// of its minisign signature against the keys of one component. Both
// warren-update (which only verifies) and warren-release (which also signs)
// use it. Nothing here knows about paths: a manifest names logical targets.
package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/PeterSR/warren-mesh/internal/update/semver"
)

// Format is the manifest format, and the domain separator of the signed
// trusted comment. A new field means a new format.
const Format = "warren-manifest/1"

// Limits.
const (
	MaxBytes     = 16 << 10
	MaxPlatforms = 8
	MaxTargets   = 8
	MaxComponent = 32
)

// Channels.
const (
	Stable = "stable"
	Dev    = "dev"
)

// File is one file of a platform: the bytes are checked against Size and
// SHA256, and Name is only a hint for directory and HTTP sources.
type File struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Platform is what one goos/goarch installs: files by target.
type Platform struct {
	Files map[string]File `json:"files"`
}

// Manifest is the signed description of one release of one component.
type Manifest struct {
	Format    string              `json:"format"`
	Component string              `json:"component"`
	Version   string              `json:"version"`
	Channel   string              `json:"channel"`
	Created   string              `json:"created"`
	Manual    bool                `json:"manual"`
	Platforms map[string]Platform `json:"platforms"`
}

var (
	componentRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
	targetRe    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	nameRe      = regexp.MustCompile(`^[A-Za-z0-9_+-][A-Za-z0-9._+-]{0,127}$`)
	platformRe  = regexp.MustCompile(`^[a-z0-9]+/[a-z0-9]+$`)
	sha256Re    = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// ValidComponent reports whether s is a component label.
func ValidComponent(s string) bool { return len(s) <= MaxComponent && componentRe.MatchString(s) }

// ValidTarget reports whether s is a logical target name.
func ValidTarget(s string) bool { return targetRe.MatchString(s) }

// ValidName reports whether s is a file name hint.
func ValidName(s string) bool { return nameRe.MatchString(s) }

// FileName is the file name of a manifest in a release directory: the exact
// release <component>-<version>.manifest.json when version is not empty, else
// the pointer of a channel, <component>.<channel>.manifest.json. The signature
// sits next to it with ".minisig" added.
func FileName(component, channel, version string) string {
	if version != "" {
		return component + "-" + version + ".manifest.json"
	}
	return component + "." + channel + ".manifest.json"
}

// Parse reads a manifest strictly: at most 16 KiB of UTF-8 JSON, one object,
// no unknown fields, no duplicate-looking tricks (a second value is refused).
// It does not check the values; see Check.
func Parse(data []byte) (*Manifest, error) {
	if len(data) > MaxBytes {
		return nil, fmt.Errorf("manifest is larger than %d bytes", MaxBytes)
	}
	if !utf8.Valid(data) {
		return nil, errors.New("manifest is not valid UTF-8")
	}
	// encoding/json takes the last of two equal keys and matches keys without
	// regard to case; neither is accepted in a signed manifest.
	if err := checkKeys(data, reflect.TypeOf(Manifest{})); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("manifest: data after the object")
	}
	return &m, nil
}

// checkKeys walks the JSON text and refuses a repeated key in any object, and
// any key of a struct that is not spelled exactly as the field's tag.
func checkKeys(data []byte, t reflect.Type) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	return walk(dec, t)
}

func walk(dec *json.Decoder, t reflect.Type) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return nil // a scalar
	}
	if d == '[' {
		for dec.More() {
			if err := walk(dec, nil); err != nil {
				return err
			}
		}
		_, err := dec.Token()
		return err
	}
	fields := map[string]reflect.Type{}
	var elem reflect.Type
	if t != nil && t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
			if name != "" {
				fields[name] = t.Field(i).Type
			}
		}
	} else if t != nil && t.Kind() == reflect.Map {
		elem = t.Elem()
	}
	seen := map[string]bool{}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return err
		}
		key, _ := kt.(string)
		if seen[key] {
			return fmt.Errorf("duplicate key %q", key)
		}
		seen[key] = true
		ft := elem
		if t != nil && t.Kind() == reflect.Struct {
			var known bool
			if ft, known = fields[key]; !known {
				for name := range fields {
					if strings.EqualFold(name, key) {
						return fmt.Errorf("key %q must be spelled %q", key, name)
					}
				}
			}
		}
		if err := walk(dec, ft); err != nil {
			return err
		}
	}
	_, err = dec.Token()
	return err
}

// Check verifies the values of a parsed manifest. maxSize is the most a file
// may be (the component's max_size).
func (m *Manifest) Check(maxSize int64) error {
	if m.Format != Format {
		return fmt.Errorf("format is %q, want %q", m.Format, Format)
	}
	if !ValidComponent(m.Component) {
		return fmt.Errorf("component %q is not a label of at most %d bytes", m.Component, MaxComponent)
	}
	v, err := semver.Parse(m.Version)
	if err != nil {
		return fmt.Errorf("version: %w", err)
	}
	switch m.Channel {
	case Dev:
		if !v.IsPre() {
			return errors.New("a dev manifest must carry a pre-release version")
		}
	case Stable:
		if v.IsPre() {
			return errors.New("a stable manifest must not carry a pre-release version")
		}
	default:
		return fmt.Errorf("channel %q is neither stable nor dev", m.Channel)
	}
	if _, err := time.Parse(time.RFC3339, m.Created); err != nil {
		return fmt.Errorf("created: %w", err)
	}
	if n := len(m.Platforms); n == 0 || n > MaxPlatforms {
		return fmt.Errorf("a manifest lists 1 to %d platforms, not %d", MaxPlatforms, n)
	}
	var first []string
	for _, p := range m.SortedPlatforms() {
		if !platformRe.MatchString(p) {
			return fmt.Errorf("platform %q is not goos/goarch", p)
		}
		files := m.Platforms[p].Files
		if n := len(files); n == 0 || n > MaxTargets {
			return fmt.Errorf("platform %s lists 1 to %d targets, not %d", p, MaxTargets, n)
		}
		targets := make([]string, 0, len(files))
		for t, f := range files {
			if !ValidTarget(t) {
				return fmt.Errorf("target %q is not a plain file label", t)
			}
			if !ValidName(f.Name) {
				return fmt.Errorf("platform %s target %s: name %q is not a plain file name", p, t, f.Name)
			}
			if f.Size < 1 || f.Size > maxSize {
				return fmt.Errorf("platform %s target %s: size %d is outside 1 to %d", p, t, f.Size, maxSize)
			}
			if !sha256Re.MatchString(f.SHA256) {
				return fmt.Errorf("platform %s target %s: sha256 is not 64 lower case hex digits", p, t)
			}
			targets = append(targets, t)
		}
		sort.Strings(targets)
		if first == nil {
			first = targets
		} else if strings.Join(first, "\x00") != strings.Join(targets, "\x00") {
			return fmt.Errorf("platform %s lists other targets than the first platform", p)
		}
	}
	return nil
}

// SortedPlatforms are the platform names in order.
func (m *Manifest) SortedPlatforms() []string {
	out := make([]string, 0, len(m.Platforms))
	for p := range m.Platforms {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// Targets are the target names every platform lists, in order.
func (m *Manifest) Targets() []string {
	ps := m.SortedPlatforms()
	if len(ps) == 0 {
		return nil
	}
	out := make([]string, 0, len(m.Platforms[ps[0]].Files))
	for t := range m.Platforms[ps[0]].Files {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// TrustedComment is the comment the signature must carry: it repeats the
// identity of the manifest so `minisign -V` shows what was signed, and it
// must equal the body's.
func (m *Manifest) TrustedComment() string {
	return Format + " " + m.Component + " " + m.Version + " " + m.Channel
}

// Marshal is the canonical encoding warren-release writes: indented JSON with
// a final newline. Verification is over the bytes as they are.
func (m *Manifest) Marshal() ([]byte, error) {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	b = append(b, '\n')
	if len(b) > MaxBytes {
		return nil, fmt.Errorf("manifest is larger than %d bytes", MaxBytes)
	}
	return b, nil
}
