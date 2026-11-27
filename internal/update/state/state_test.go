package state

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRoundTripKeepsUnknownFields(t *testing.T) {
	s := Store{Dir: t.TempDir()}
	dir := filepath.Join(s.Dir, "myapp")
	os.MkdirAll(dir, 0o700)
	// A newer writer added a field and a nested one.
	os.WriteFile(s.Path("myapp"), []byte(`{"schema":1,"component":"myapp","phase":"committed","future":{"a":[1,2]},"request":null}`), 0o600)
	r, err := s.Load("myapp")
	if err != nil {
		t.Fatal(err)
	}
	r.NeedsManual = StateLost
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(s.Path("myapp"))
	if !strings.Contains(string(b), `"future"`) || !strings.Contains(string(b), `"needs_manual": "state-lost"`) {
		t.Errorf("rewritten file lost a field:\n%s", b)
	}
	again, err := s.Load("myapp")
	if err != nil || again.NeedsManual != StateLost {
		t.Fatalf("%v %+v", err, again)
	}
}

func TestSaveWritesEveryFrozenKey(t *testing.T) {
	s := Store{Dir: t.TempDir()}
	if err := s.Save(New("myapp")); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(s.Path("myapp"))
	for _, k := range []string{"schema", "component", "phase", "installed", "prev", "request", "deadline", "healthy_since", "canary_ends", "failures", "watch", "bad", "needs_manual", "last_error"} {
		if !strings.Contains(string(b), `"`+k+`"`) {
			t.Errorf("key %s missing:\n%s", k, b)
		}
	}
	entries, _ := os.ReadDir(filepath.Dir(s.Path("myapp")))
	if len(entries) != 1 {
		t.Errorf("temp files left: %v", entries)
	}
}

func TestFullRecordRoundTrips(t *testing.T) {
	s := Store{Dir: t.TempDir()}
	seq := 8
	now := time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)
	r := New("myapp")
	r.Phase = Probation
	r.Installed = &Installed{Version: "1.4.0", Channel: "stable", ManifestSHA256: "m", Files: map[string]string{"myapp": "h1"}, InstalledAt: now}
	r.Prev = &Prev{Version: "1.3.2", Files: map[string]string{"myapp": "h0"}}
	r.Request = &Request{At: now, MinStartSeq: &seq}
	r.Deadline, r.HealthySince, r.CanaryEnds = &now, &now, &now
	r.Failures = []time.Time{now}
	r.Watch = Watch{Boot: "b", UnrequestedTotal: 4}
	r.Bad = []Bad{{Version: "1.5.0", Reason: CrashLoop, At: now}}
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load("myapp")
	if err != nil {
		t.Fatal(err)
	}
	if got.Installed.Files["myapp"] != "h1" || *got.Request.MinStartSeq != 8 || got.Watch.UnrequestedTotal != 4 || !got.IsBad("1.5.0") || got.IsBad("1.4.0") || !got.Deadline.Equal(now) {
		t.Errorf("round trip: %+v", got)
	}
}

func TestPredicatesAndErrors(t *testing.T) {
	r := New("a")
	r.Phase = Reverted
	r.Prev = &Prev{Files: map[string]string{"a": "h"}}
	if !r.UnfinishedRevert() || r.WatchingRestored() {
		t.Error("an unfinished revert")
	}
	r.Prev = &Prev{}
	r.Request = &Request{}
	if r.UnfinishedRevert() || !r.WatchingRestored() {
		t.Error("a settled revert with a watch")
	}
	now := time.Now()
	r.HealthySince = &now
	if r.WatchingRestored() {
		t.Error("a proven restored version is still watched")
	}
	r.Phase = "weird"
	if r.KnownPhase() {
		t.Error("unknown phase known")
	}
	s := Store{Dir: t.TempDir()}
	if _, err := s.Load("none"); err != ErrNone {
		t.Errorf("missing: %v", err)
	}
	os.MkdirAll(filepath.Join(s.Dir, "x"), 0o700)
	os.WriteFile(s.Path("x"), []byte(`{"schema":2,"component":"x"}`), 0o600)
	if _, err := s.Load("x"); err == nil || !strings.Contains(err.Error(), "schema 2") {
		t.Errorf("newer schema: %v", err)
	}
	os.WriteFile(s.Path("x"), []byte(`{"schema":1,"component":"y"}`), 0o600)
	if _, err := s.Load("x"); err == nil {
		t.Error("another component's record accepted")
	}
	os.WriteFile(s.Path("x"), []byte(`nope`), 0o600)
	if _, err := s.Load("x"); err == nil {
		t.Error("garbage accepted")
	}
}

func TestNestedUnknownFieldsAreKept(t *testing.T) {
	s := Store{Dir: t.TempDir()}
	os.MkdirAll(filepath.Join(s.Dir, "myapp"), 0o700)
	os.WriteFile(s.Path("myapp"), []byte(`{"schema":1,"component":"myapp","phase":"probation",
		"installed":{"version":"1.0.0","channel":"dev","manifest_sha256":"m","files":{"myapp":"h"},"installed_at":"2026-11-26T10:00:00Z","signer":"k1"},
		"prev":{"version":"0.9.0","files":{"myapp":"g"},"note":{"a":1}},
		"request":{"at":"2026-11-26T10:00:00Z","min_start_seq":3,"boot":"b1"},
		"watch":{"boot":"b1","unrequested_total":2,"extra":true},
		"bad":[{"version":"0.8.0","reason":"manual","at":"2026-11-26T10:00:00Z","who":"op"}]}`), 0o600)
	r, err := s.Load("myapp")
	if err != nil {
		t.Fatal(err)
	}
	r.LastError = "x"
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(s.Path("myapp"))
	for _, want := range []string{`"signer": "k1"`, `"note"`, `"boot": "b1"`, `"extra": true`, `"who": "op"`, `"min_start_seq": 3`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("%s lost:\n%s", want, b)
		}
	}
}

// encoding/json matches keys without regard to case; the record must not.
func TestKeysAreMatchedExactly(t *testing.T) {
	s := Store{Dir: t.TempDir()}
	os.MkdirAll(filepath.Join(s.Dir, "myapp"), 0o700)
	os.WriteFile(s.Path("myapp"), []byte(`{"schema":1,"component":"myapp","phase":"committed","Phase":"failed","NEEDS_MANUAL":"drift","Installed":{"version":"9"}}`), 0o600)
	r, err := s.Load("myapp")
	if err != nil {
		t.Fatal(err)
	}
	if r.Phase != Committed || r.NeedsManual != "" || r.Installed != nil {
		t.Fatalf("a key of another case was taken for a known one: %+v", r)
	}
	if err := s.Save(r); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(s.Path("myapp"))
	for _, want := range []string{`"Phase": "failed"`, `"NEEDS_MANUAL": "drift"`, `"Installed"`, `"phase": "committed"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("%s missing:\n%s", want, b)
		}
	}
}
