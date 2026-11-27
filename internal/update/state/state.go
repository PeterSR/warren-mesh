// Package state is the per-component state.json of the updater. Its format is
// fixed: new fields are additive, readers ignore fields they do not know and
// writers keep them. The file is written by temp file, fsync, rename and a
// directory fsync, only ever by the updater under its update.lock.
package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/PeterSR/warren-mesh/internal/fsutil"
)

// Schema is the state.json schema this code reads and writes.
const Schema = 1

// Phase is where a component is in its life; its values are the constants
// below, a closed set. Anything else means hands off.
type Phase string

const (
	Installing Phase = "installing"
	Probation  Phase = "probation"
	Committed  Phase = "committed"
	Reverted   Phase = "reverted"
	Failed     Phase = "failed"
)

// Reasons for needs_manual.
const (
	StateLost = "state-lost"
	Drift     = "drift"
)

// Reasons for a bad version.
const (
	CrashLoop = "crash-loop"
	Manual    = "manual"
)

// Installed is the version whose files are in the targets on disk.
type Installed struct {
	Version        string            `json:"version"`
	Channel        string            `json:"channel"`
	ManifestSHA256 string            `json:"manifest_sha256"`
	Files          map[string]string `json:"files"`
	InstalledAt    time.Time         `json:"installed_at"`
	// Floor, when set, is a version that no install may go below without
	// --allow-downgrade; installing the floor itself again is allowed (it may be
	// a version that never ran, whose install was aborted). After a revert to a
	// version that is not known (the files were there before the updater kept
	// state) it is the version that was reverted away, so the no-downgrade rule
	// keeps its meaning. Additive: older readers ignore it.
	Floor string `json:"floor,omitempty"`

	unknown map[string]json.RawMessage
}

// Prev is what a revert restores. An empty Files means settled: nothing to
// revert to.
type Prev struct {
	Version string            `json:"version"`
	Files   map[string]string `json:"files"`

	unknown map[string]json.RawMessage
}

// Request is the restart request of an install or a revert. MinStartSeq is
// null until Warren has accepted the request (and so tells the updater to send
// it again). For a restored watch, At is overwritten with the proof time when
// the deadline is armed, so it is the anchor a new boot's extension is bounded from.
type Request struct {
	At          time.Time `json:"at"`
	MinStartSeq *int      `json:"min_start_seq"`

	unknown map[string]json.RawMessage
}

// Watch is the baseline of the crash counting.
type Watch struct {
	Boot             string `json:"boot"`
	UnrequestedTotal int    `json:"unrequested_total"`

	unknown map[string]json.RawMessage
}

// Bad is a version that must not be installed again without force.
type Bad struct {
	Version string    `json:"version"`
	Reason  string    `json:"reason"`
	At      time.Time `json:"at"`

	unknown map[string]json.RawMessage
}

// Record is the state of one component.
type Record struct {
	Schema       int         `json:"schema"`
	Component    string      `json:"component"`
	Phase        Phase       `json:"phase"`
	Installed    *Installed  `json:"installed"`
	Prev         *Prev       `json:"prev"`
	Request      *Request    `json:"request"`
	Deadline     *time.Time  `json:"deadline"`
	HealthySince *time.Time  `json:"healthy_since"`
	CanaryEnds   *time.Time  `json:"canary_ends"`
	Failures     []time.Time `json:"failures"`
	Watch        Watch       `json:"watch"`
	Bad          []Bad       `json:"bad"`
	NeedsManual  string      `json:"needs_manual"`
	LastError    string      `json:"last_error"`

	// unknown keeps the fields this code does not know, so a newer writer's
	// additions survive a rewrite.
	unknown map[string]json.RawMessage
}

// KnownPhase reports whether the phase is one this code understands.
func (r *Record) KnownPhase() bool {
	switch r.Phase {
	case Installing, Probation, Committed, Reverted, Failed:
		return true
	}
	return false
}

// UnfinishedRevert is a verdict written whose files and bookkeeping are not
// done: reverted with a non-empty prev.
func (r *Record) UnfinishedRevert() bool {
	return r.Phase == Reverted && r.Prev != nil && len(r.Prev.Files) > 0
}

// WatchingRestored is a settled revert whose restored version is still being
// watched: nothing proved healthy yet.
func (r *Record) WatchingRestored() bool {
	return r.Phase == Reverted && !r.UnfinishedRevert() && r.Request != nil && r.HealthySince == nil
}

// IsBad reports whether a version is on the bad list.
func (r *Record) IsBad(version string) bool {
	for _, b := range r.Bad {
		if b.Version == version {
			return true
		}
	}
	return false
}

// New is the record of a component never seen.
func New(component string) *Record {
	return &Record{Schema: Schema, Component: component, Phase: Committed, Failures: []time.Time{}, Bad: []Bad{}}
}

// jsonKeys are the exact JSON key names of a struct type, from its tags.
func jsonKeys(t reflect.Type) map[string]bool {
	keys := map[string]bool{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if f.IsExported() && name != "" && name != "-" {
			keys[name] = true
		}
	}
	return keys
}

// decodeExact decodes data into v (a pointer to a struct) using only the keys
// whose case matches the struct's tags exactly, and returns every other key,
// raw, so a rewrite keeps it. (encoding/json alone matches keys without regard
// to case, which would take a key such as "Phase" for "phase" and also keep it.)
func decodeExact(data []byte, v any) (map[string]json.RawMessage, error) {
	var all map[string]json.RawMessage
	if err := json.Unmarshal(data, &all); err != nil {
		return nil, err
	}
	known := jsonKeys(reflect.TypeOf(v).Elem())
	exact := map[string]json.RawMessage{}
	var unknown map[string]json.RawMessage
	for k, raw := range all {
		if known[k] {
			exact[k] = raw
			continue
		}
		if unknown == nil {
			unknown = map[string]json.RawMessage{}
		}
		unknown[k] = raw
	}
	b, err := json.Marshal(exact)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return nil, err
	}
	return unknown, nil
}

// encodeWith encodes v (a struct value) with the unknown keys added back.
func encodeWith(v any, unknown map[string]json.RawMessage) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil || len(unknown) == 0 {
		return b, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	for k, raw := range unknown {
		if _, ok := m[k]; !ok {
			m[k] = raw
		}
	}
	return json.Marshal(m)
}

// The nested objects keep their unknown fields too.

type installedPlain Installed

func (x *Installed) UnmarshalJSON(b []byte) error {
	u, err := decodeExact(b, (*installedPlain)(x))
	x.unknown = u
	return err
}
func (x Installed) MarshalJSON() ([]byte, error) { return encodeWith(installedPlain(x), x.unknown) }

type prevPlain Prev

func (x *Prev) UnmarshalJSON(b []byte) error {
	u, err := decodeExact(b, (*prevPlain)(x))
	x.unknown = u
	return err
}
func (x Prev) MarshalJSON() ([]byte, error) { return encodeWith(prevPlain(x), x.unknown) }

type requestPlain Request

func (x *Request) UnmarshalJSON(b []byte) error {
	u, err := decodeExact(b, (*requestPlain)(x))
	x.unknown = u
	return err
}
func (x Request) MarshalJSON() ([]byte, error) { return encodeWith(requestPlain(x), x.unknown) }

type watchPlain Watch

func (x *Watch) UnmarshalJSON(b []byte) error {
	u, err := decodeExact(b, (*watchPlain)(x))
	x.unknown = u
	return err
}
func (x Watch) MarshalJSON() ([]byte, error) { return encodeWith(watchPlain(x), x.unknown) }

type badPlain Bad

func (x *Bad) UnmarshalJSON(b []byte) error {
	u, err := decodeExact(b, (*badPlain)(x))
	x.unknown = u
	return err
}
func (x Bad) MarshalJSON() ([]byte, error) { return encodeWith(badPlain(x), x.unknown) }

type recordPlain Record

// Marshal encodes the record, keeping unknown fields.
func (r *Record) Marshal() ([]byte, error) {
	if r.Failures == nil {
		r.Failures = []time.Time{}
	}
	if r.Bad == nil {
		r.Bad = []Bad{}
	}
	b, err := encodeWith(recordPlain(*r), r.unknown)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, b, "", "  "); err != nil {
		return nil, err
	}
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

// Parse decodes a record. A schema this code does not know is an error: the
// file is left alone.
func Parse(data []byte) (*Record, error) {
	var r Record
	u, err := decodeExact(data, (*recordPlain)(&r))
	if err != nil {
		return nil, fmt.Errorf("state.json: %w", err)
	}
	if r.Schema != Schema {
		return nil, fmt.Errorf("state.json: schema %d is not understood (this updater reads %d)", r.Schema, Schema)
	}
	r.unknown = u
	return &r, nil
}

// Store keeps the records of a state directory: <dir>/<component>/state.json.
type Store struct{ Dir string }

// Path is the file of a component.
func (s Store) Path(component string) string { return filepath.Join(s.Dir, component, "state.json") }

// ErrNone says there is no record.
var ErrNone = errors.New("no state record")

// Load reads one record; ErrNone when there is none.
func (s Store) Load(component string) (*Record, error) {
	b, err := os.ReadFile(s.Path(component))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNone
	}
	if err != nil {
		return nil, err
	}
	r, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", component, err)
	}
	if r.Component != component {
		return nil, fmt.Errorf("%s: state.json is for component %q", component, r.Component)
	}
	return r, nil
}

// Save writes the record atomically (see fsutil.WriteAtomic).
func (s Store) Save(r *Record) error {
	b, err := r.Marshal()
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.Path(r.Component))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return fsutil.WriteAtomic(s.Path(r.Component), b, 0o600)
}
