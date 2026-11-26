// Package grammar holds the names Warren accepts and how it compares them:
// labels, relay ids, entry names, service and method names, addresses and
// allow patterns. These rules are public contract, so every other package
// asks here instead of making up its own check.
package grammar

import (
	"crypto/rand"
	"encoding/hex"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Limits that belong to the grammar.
const (
	MaxRelayID     = 64
	MaxServiceName = 32
	MaxMethod      = 64
	MaxEntryName   = 64
	MaxDetail      = 200
	// MaxCallBody is the most one call's request or response may carry.
	MaxCallBody = 1 << 20
)

// Label reports whether s is a label: [a-z0-9][a-z0-9_-]{0,62}.
func Label(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case (c == '_' || c == '-') && i > 0:
		default:
			return false
		}
	}
	return true
}

// RelayID reports whether s names a place: a node ("server") or a
// child ("sandbox.server"), at most 64 bytes. Namespaces have the same
// shape, so Namespace is the same check.
func RelayID(s string) bool {
	if len(s) > MaxRelayID {
		return false
	}
	label, node, ok := strings.Cut(s, ".")
	if !ok {
		return Label(s)
	}
	return Label(label) && Label(node)
}

// Namespace reports whether s is a namespace: a relay id, or a service
// namespace "<label>.<node>". They look alike on purpose.
func Namespace(s string) bool { return RelayID(s) }

// Root is the node part of a relay id or namespace: "server" for
// "sandbox.server" and for "server".
func Root(id string) string {
	if _, node, ok := strings.Cut(id, "."); ok {
		return node
	}
	return id
}

// Child builds the relay id of a child: "<label>.<node>".
func Child(label, node string) string { return label + "." + node }

// ChildLabel is the label of a child relay id, or "" for a node's own id.
func ChildLabel(id string) string {
	label, _, ok := strings.Cut(id, ".")
	if !ok {
		return ""
	}
	return label
}

// EntryName reports whether s is an entry name: [a-z0-9_][a-z0-9._-]{0,63}.
func EntryName(s string) bool {
	if s == "" || len(s) > MaxEntryName {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_':
		case (c == '.' || c == '-') && i > 0:
		default:
			return false
		}
	}
	return true
}

// Service reports whether s is a service name: a label of at most 32 bytes.
func Service(s string) bool { return len(s) <= MaxServiceName && Label(s) }

// Method reports whether s is a method name: label grammar, nothing more.
// Warren never interprets it.
func Method(s string) bool { return Label(s) }

// SplitAddress splits "name@namespace". Names cannot contain "@", so the
// first one splits. ok is false when either side is not valid.
func SplitAddress(a string) (name, ns string, ok bool) {
	name, ns, found := strings.Cut(a, "@")
	if !found || !EntryName(name) || !Namespace(ns) {
		return "", "", false
	}
	return name, ns, true
}

// JoinAddress builds "name@namespace".
func JoinAddress(name, ns string) string { return name + "@" + ns }

// AllowPattern reports whether p is an allow pattern: an exact relay id,
// "*.<node>" (every child of that node, not the node itself) or "*".
func AllowPattern(p string) bool {
	if p == "*" {
		return true
	}
	if node, ok := strings.CutPrefix(p, "*."); ok {
		return Label(node)
	}
	return RelayID(p)
}

// Allowed reports whether origin (a relay id) matches any pattern. No
// patterns means nobody.
func Allowed(patterns []string, origin string) bool {
	for _, p := range patterns {
		switch {
		case p == "*":
			return true
		case strings.HasPrefix(p, "*."):
			label, node, ok := strings.Cut(origin, ".")
			if ok && Label(label) && node == p[2:] {
				return true
			}
		case p == origin:
			return true
		}
	}
	return false
}

// NamePattern reports whether p is a usable pattern for entry names, such
// as "researcher" or "sandbox-*".
func NamePattern(p string) bool {
	if p == "" {
		return false
	}
	_, err := path.Match(p, "")
	return err == nil
}

// MatchNames reports whether name matches any pattern. No patterns means
// any name.
func MatchNames(patterns []string, name string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}
	return false
}

// CleanText drops control and invisible formatting characters (newlines,
// bidi overrides, zero-width joiners) and caps the length on a rune
// boundary.
func CleanText(s string, n int) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == utf8.RuneError {
			return -1
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if len(s) > n {
		cut := n
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = strings.TrimSpace(s[:cut])
	}
	return s
}

// CleanDetail is the text that goes in an error's detail field.
func CleanDetail(s string) string { return CleanText(s, MaxDetail) }

// NewID is a random UUID (version 4).
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
