package grammar

import (
	"strings"
	"testing"
)

func TestLabel(t *testing.T) {
	good := []string{"a", "0", "sandbox", "a_b-c", "a" + strings.Repeat("b", 62)}
	bad := []string{"", "-a", "_a", "A", "a.b", "a b", "é", "a@b", strings.Repeat("a", 64)}
	for _, s := range good {
		if !Label(s) {
			t.Errorf("Label(%q) = false", s)
		}
	}
	for _, s := range bad {
		if Label(s) {
			t.Errorf("Label(%q) = true", s)
		}
	}
}

func TestRelayID(t *testing.T) {
	good := []string{"server", "sandbox.server", "a.b"}
	bad := []string{"", ".a", "a.", "a.b.c", "A.b", "a..b",
		strings.Repeat("a", 32) + "." + strings.Repeat("b", 32)} // 65 bytes
	for _, s := range good {
		if !RelayID(s) {
			t.Errorf("RelayID(%q) = false", s)
		}
	}
	for _, s := range bad {
		if RelayID(s) {
			t.Errorf("RelayID(%q) = true", s)
		}
	}
	// 64 bytes exactly fits.
	if !RelayID(strings.Repeat("a", 31) + "." + strings.Repeat("b", 32)) {
		t.Error("64 byte relay id refused")
	}
	if Root("a.b") != "b" || Root("b") != "b" || ChildLabel("a.b") != "a" || ChildLabel("b") != "" {
		t.Error("Root or ChildLabel wrong")
	}
}

func TestEntryName(t *testing.T) {
	good := []string{"alice", "_x", "a.b-c_d", "0", strings.Repeat("a", 64)}
	bad := []string{"", ".a", "-a", "A", "a@b", "a b", strings.Repeat("a", 65), "é"}
	for _, s := range good {
		if !EntryName(s) {
			t.Errorf("EntryName(%q) = false", s)
		}
	}
	for _, s := range bad {
		if EntryName(s) {
			t.Errorf("EntryName(%q) = true", s)
		}
	}
}

func TestServiceAndMethod(t *testing.T) {
	if !Service("chat") || Service("") || Service(strings.Repeat("a", 33)) || !Service(strings.Repeat("a", 32)) {
		t.Error("Service")
	}
	if !Method("deliver") || Method("De liver") || Method(strings.Repeat("a", 64)) || !Method(strings.Repeat("a", 63)) {
		t.Error("Method")
	}
}

func TestAddress(t *testing.T) {
	n, ns, ok := SplitAddress("alice@web.server")
	if !ok || n != "alice" || ns != "web.server" {
		t.Errorf("got %q %q %v", n, ns, ok)
	}
	for _, a := range []string{"alice", "@x", "alice@", "Alice@x", "a@b@c", "a@b.c.d"} {
		if _, _, ok := SplitAddress(a); ok {
			t.Errorf("SplitAddress(%q) ok", a)
		}
	}
	if JoinAddress("a", "b") != "a@b" {
		t.Error("JoinAddress")
	}
}

func TestAllow(t *testing.T) {
	for _, p := range []string{"*", "*.node", "sandbox.node", "node"} {
		if !AllowPattern(p) {
			t.Errorf("AllowPattern(%q) = false", p)
		}
	}
	for _, p := range []string{"", "*.", "*.a.b", "**", "a*", "A"} {
		if AllowPattern(p) {
			t.Errorf("AllowPattern(%q) = true", p)
		}
	}
	cases := []struct {
		pats   []string
		origin string
		want   bool
	}{
		{nil, "node", false},
		{[]string{"*"}, "x.node", true},
		{[]string{"*"}, "node", true},
		{[]string{"*.node"}, "x.node", true},
		{[]string{"*.node"}, "node", false}, // the node itself is not a child
		{[]string{"*.node"}, "x.other", false},
		{[]string{"*.node"}, "x.y.node", false},
		{[]string{"node"}, "node", true},
		{[]string{"node"}, "x.node", false},
		{[]string{"x.node", "y.node"}, "y.node", true},
	}
	for _, c := range cases {
		if got := Allowed(c.pats, c.origin); got != c.want {
			t.Errorf("Allowed(%v, %q) = %v", c.pats, c.origin, got)
		}
	}
}

func TestNamePatterns(t *testing.T) {
	if !NamePattern("sandbox-*") || NamePattern("") || NamePattern("[") {
		t.Error("NamePattern")
	}
	if !MatchNames(nil, "x") || !MatchNames([]string{"a*"}, "abc") || MatchNames([]string{"a*"}, "b") {
		t.Error("MatchNames")
	}
}

func TestCleanDetail(t *testing.T) {
	got := CleanDetail("a\nb\u202ec" + strings.Repeat("x", 300))
	if strings.ContainsAny(got, "\n\u202e") || len(got) > MaxDetail {
		t.Errorf("not cleaned: %q", got)
	}
	if len(NewID()) != 36 {
		t.Error("NewID")
	}
}
