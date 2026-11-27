package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PeterSR/warren-mesh/internal/pinned"
)

const sample = `
node: server
state_dir: /var/lib/warren
transport: tailscale
tailscale:
  tag: warren
  listen: ":7778"
  port: 7778
services:
  dir: /run/warren/services
  list:
    - name: chat
      allow: ["*"]
      namespaces: [web]
      run: [chat-svc]
      env_pass: [SECRET]
attach:
  dir: /run/warren/attach
  max_children: 64
  default:
    max_entries: 100
    services:
      - name: chat
        allow: ["*"]
  children:
    - name: host
      flat: true
      uid: 1000
      names: ["*"]
      services:
        - name: chat
          allow: ["*"]
`

func TestParseSample(t *testing.T) {
	c, err := Parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	if c.Transport.Name != "tailscale" || c.Services.List[0].Inflight != DefaultInflight {
		t.Errorf("defaults not applied: %+v", c)
	}
	if c.Attach.Children[0].MaxEntries != DefaultChildEntries || c.Attach.Default.MaxEntries != 100 {
		t.Error("max_entries defaults")
	}
}

func bad(t *testing.T, patch func(string) string, want string) {
	t.Helper()
	_, err := Parse([]byte(patch(sample)))
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("want error containing %q, got %v", want, err)
	}
}

func TestRefusals(t *testing.T) {
	rep := func(a, b string) func(string) string {
		return func(s string) string {
			if !strings.Contains(s, a) {
				t.Fatalf("sample lacks %q", a)
			}
			return strings.Replace(s, a, b, 1)
		}
	}
	bad(t, rep("node: server", "node: Home.Server"), "node")
	bad(t, rep("node: server", "node: server\nbogus: 1"), "bogus")
	bad(t, rep("flat: true", "flat: true\n      wat: 1"), "wat")
	bad(t, rep("namespaces: [web]", "namespaces: [host]"), "also a child label")
	bad(t, rep("namespaces: [web]", "namespaces: [web, web]"), "twice")
	bad(t, rep("namespaces: [web]", "namespaces: [Bad]"), "namespace")
	bad(t, rep("name: host", "name: Host"), "label")
	bad(t, rep("allow: [\"*\"]\n      namespaces", "allow: [\"*.a.b\"]\n      namespaces"), "allow")
	bad(t, rep("transport: tailscale", "transport: carrierpigeon"), "not known")
	bad(t, rep("transport: tailscale", "transport: {run: [x], link_a: /a, link_b: /b}"), "one of")
	bad(t, rep("transport: tailscale", "transport: {link_a: /a}"), "go together")
	bad(t, rep("transport: tailscale", "transport: {link_a: /a, link_b: /b}"), "uid is required")
	bad(t, rep("transport: tailscale", "transport: {link_a: /a, link_b: /b, uid: -1}"), "non-negative")
	bad(t, rep("transport: tailscale", "transport: {wat: 1}"), "unknown key")
	bad(t, rep("state_dir: /var/lib/warren", "state_dir: rel"), "absolute")
	bad(t, rep("names: [\"*\"]", "names: [\"[\"]"), "pattern")
	bad(t, rep("dir: /run/warren/services", "dir: /run/warren/"+strings.Repeat("x", 100)), "unix socket")
	bad(t, rep("dir: /run/warren/attach", "dir: /run/warren/services"), "must not be services.dir")
	bad(t, rep("max_children: 64", "max_children: -1"), "positive")
	bad(t, rep("- name: chat\n        allow: [\"*\"]\n  children", "- name: chat\n        run: [x]\n  children"), "only for services on the node")
	bad(t, rep("namespaces: [web]", "namespaces: [web]\n      env: {WARREN_SERVICE_SOCKET: x}"), "not allowed")
	bad(t, func(s string) string { return s + "update: {serve: true}\n" }, "unknown key")
	bad(t, func(s string) string { return s + "pinned: \"a\\nb\"\n" }, "one line")
	bad(t, func(s string) string { return s + "pinned: " + strings.Repeat("x", pinned.MaxLen+1) + "\n" }, "pinned")
	bad(t, rep("run: [chat-svc]", "run: [chat-svc]\n      uid: 4242"), "Warren's user")
	bad(t, rep("namespaces: [web]", "namespaces: [web]\n      inflight: 32\n      outflight: 30"), "must be at most 64")
	bad(t, rep("transport: tailscale", "transport: nats"), "not available in this release")
	bad(t, func(s string) string { return s + "nats: {url: x}\n" }, "not available in this release")
}

func TestPinned(t *testing.T) {
	c, err := Parse([]byte(sample + "pinned: docker image example:warren\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Pinned != "docker image example:warren" {
		t.Errorf("pinned %q", c.Pinned)
	}
	if c, _ := Parse([]byte(sample)); c.Pinned != "" {
		t.Errorf("pinned by default: %q", c.Pinned)
	}
}

func TestPinnedRefusesInvisibleAndBidiCharacters(t *testing.T) {
	for name, v := range map[string]string{
		"right-to-left override": "a\u202eb", "bidi isolate": "a\u2066b", "zero width space": "a\u200bb", "left-to-right mark": "a\u200eb",
		"line separator": "a\u2028b", "paragraph separator": "a\u2029b", "tab": "a\tb", "only spaces": "   ", "only an ideographic space": "\u3000",
	} {
		if _, err := Parse([]byte(sample + "pinned: \"" + v + "\"\n")); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := Parse([]byte(sample + "pinned: \"Docker image caf\u00e9:1\"\n")); err != nil {
		t.Errorf("a plain reason with an accent refused: %v", err)
	}
}

func TestMaxChildren(t *testing.T) {
	// one child with max_children 1 is fine; two are not.
	s := strings.Replace(sample, "max_children: 64", "max_children: 1", 1)
	s += "    - name: other\n"
	if _, err := Parse([]byte(s)); err == nil || !strings.Contains(err.Error(), "max_children") {
		t.Errorf("got %v", err)
	}
}

func TestEmpty(t *testing.T) {
	if _, err := Parse(nil); err == nil {
		t.Error("empty config accepted")
	}
}

func TestReloadRefusesRemovedChildWithDirectory(t *testing.T) {
	dir := t.TempDir()
	s := strings.ReplaceAll(sample, "/run/warren/attach", dir)
	old, err := Parse([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	next := *old
	next.Attach.Children = nil

	// No directory: removal is fine.
	if err := CheckReload(old, &next); err != nil {
		t.Errorf("removal without a directory refused: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "host"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := CheckReload(old, &next); err == nil || !strings.Contains(err.Error(), "still exists") {
		t.Errorf("removal with a directory allowed: %v", err)
	}
	// Keeping it (even with flat changed) is fine.
	keep := *old
	keep.Attach.Children = []Child{{Name: "host", Flat: false}}
	if err := CheckReload(old, &keep); err != nil {
		t.Errorf("flat change refused: %v", err)
	}
}

func TestReloadRefusesRestartSettings(t *testing.T) {
	old, _ := Parse([]byte(sample))
	for name, mut := range map[string]func(*Config){
		"node":      func(c *Config) { c.Node = "other" },
		"state_dir": func(c *Config) { c.StateDir = "/x" },
		"services":  func(c *Config) { c.Services.Dir = "/x" },
		"attach":    func(c *Config) { c.Attach.Dir = "/x" },
		"transport": func(c *Config) { c.Transport.Name = "other" },
	} {
		next, _ := Parse([]byte(sample))
		mut(next)
		if err := CheckReload(old, next); err == nil {
			t.Errorf("%s change allowed", name)
		}
	}
	same, _ := Parse([]byte(sample))
	same.Services.List[0].Allow = []string{"x.server"}
	if err := CheckReload(old, same); err != nil {
		t.Errorf("allow change refused: %v", err)
	}
}

func TestTemplateDefaultsAndControl(t *testing.T) {
	c, err := Parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	d := c.Attach.Default.Services[0]
	if d.Inflight != 4 || d.PerRoot != 2 || d.Outflight != 4 {
		t.Errorf("template defaults %+v", d)
	}
	h := c.Attach.Children[0].Services[0]
	if h.Inflight != 16 || h.PerRoot != 8 || h.Outflight != 16 {
		t.Errorf("child defaults %+v", h)
	}
	if c.Control != "/run/warren/control.sock" {
		t.Errorf("control default %q", c.Control)
	}
	// max_children defaults to 16.
	s := strings.Replace(sample, "  max_children: 64\n", "", 1)
	if c, err := Parse([]byte(s)); err != nil || c.Attach.MaxChildren != 16 {
		t.Errorf("max_children default: %v %v", err, c)
	}
	// An explicit control path wins; a relative one is refused.
	if c, err := Parse([]byte(sample + "control: /run/x.sock\n")); err != nil || c.Control != "/run/x.sock" {
		t.Errorf("control: %v", err)
	}
	if _, err := Parse([]byte(sample + "control: rel.sock\n")); err == nil {
		t.Error("relative control accepted")
	}
}

func TestDefaultMayNotBeFlat(t *testing.T) {
	bad(t, func(s string) string { return strings.Replace(s, "  default:\n", "  default:\n    flat: true\n", 1) }, "cannot be flat")
}

func TestOmittedAllowAndNames(t *testing.T) {
	c, err := Parse([]byte(strings.Replace(sample, "      allow: [\"*\"]\n      namespaces", "      namespaces", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Services.List[0].Allow) != 0 {
		t.Error("omitted allow should mean nobody")
	}
	if len(c.Attach.Children[0].Names) != 1 {
		t.Error("names kept")
	}
}

func TestReloadRefusesControlChange(t *testing.T) {
	old, _ := Parse([]byte(sample))
	next, _ := Parse([]byte(sample + "control: /run/other.sock\n"))
	if err := CheckReload(old, next); err == nil {
		t.Error("control change allowed")
	}
}

func TestVariablesAreExpanded(t *testing.T) {
	t.Setenv("WARREN_TEST_NODE", "boxy")
	s := strings.Replace(sample, "node: server", "node: ${WARREN_TEST_NODE}", 1)
	c, err := Parse([]byte(s))
	if err != nil || c.Node != "boxy" {
		t.Fatalf("%v %v", c, err)
	}
	s = strings.Replace(sample, "node: server", "node: ${WARREN_TEST_UNSET_NODE}", 1)
	if _, err := Parse([]byte(s)); err == nil || !strings.Contains(err.Error(), "WARREN_TEST_UNSET_NODE") || !strings.Contains(err.Error(), `"node"`) {
		t.Errorf("unset variable: %v", err)
	}
	// Unknown keys name their path.
	s = strings.Replace(sample, "      flat: true\n", "      flat: true\n      wat: 1\n", 1)
	if _, err := Parse([]byte(s)); err == nil || !strings.Contains(err.Error(), "attach.children[0].wat") {
		t.Errorf("key path: %v", err)
	}
}

func TestLoadResolvesAgainOnEveryLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	t.Setenv("WARREN_TEST_TAG", "one")
	os.WriteFile(path, []byte(strings.Replace(sample, "tag: warren", "tag: ${WARREN_TEST_TAG}", 1)), 0o600)
	c, err := Load(path)
	if err != nil || c.Tailscale.Tag != "one" {
		t.Fatalf("%v", err)
	}
	os.WriteFile(path, []byte(strings.Replace(sample, "tag: warren", "tag: ${WARREN_TEST_TAG}-two", 1)), 0o600)
	if c, err = Load(path); err != nil || c.Tailscale.Tag != "one-two" {
		t.Fatalf("%v", err)
	}
}

func TestAtMost32Services(t *testing.T) {
	build := func(n int) string {
		var b strings.Builder
		b.WriteString("node: n1\nstate_dir: /var/lib/warren\nservices:\n  dir: /run/w/s\n  list:\n")
		for i := 0; i < n; i++ {
			b.WriteString("    - name: s" + string(rune('a'+i/26)) + string(rune('a'+i%26)) + "\n      allow: [\"*\"]\n")
		}
		return b.String()
	}
	if _, err := Parse([]byte(build(32))); err != nil {
		t.Fatalf("32 services: %v", err)
	}
	if _, err := Parse([]byte(build(33))); err == nil || !strings.Contains(err.Error(), "at most 32") {
		t.Fatalf("33 services: %v", err)
	}
}

func TestTailscaleNeedsABlockWithATag(t *testing.T) {
	min := "node: n1\nstate_dir: /var/lib/warren\ntransport: tailscale\n"
	if _, err := Parse([]byte(min)); err == nil || !strings.Contains(err.Error(), "tailscale:") {
		t.Errorf("no block: %v", err)
	}
	if _, err := Parse([]byte(min + "tailscale:\n  port: 7778\n")); err == nil {
		t.Error("empty tag accepted")
	}
	if _, err := Parse([]byte(min + "tailscale:\n  tag: t\n")); err != nil {
		t.Error(err)
	}
}

func TestInflightIsCapped(t *testing.T) {
	y := func(n int) string {
		return fmt.Sprintf("node: n1\nstate_dir: /tmp/s\nservices:\n  dir: /tmp/s/d\n  list:\n    - {name: x, allow: ['*'], inflight: %d}\n", n)
	}
	if _, err := Parse([]byte(y(MaxInflight))); err != nil {
		t.Errorf("inflight %d: %v", MaxInflight, err)
	}
	if _, err := Parse([]byte(y(MaxInflight + 1))); err == nil || !strings.Contains(err.Error(), "inflight is at most") {
		t.Errorf("inflight %d: %v", MaxInflight+1, err)
	}
}

// The example shipped for `warren run` must load through the real loader.
func TestExampleConfigLoads(t *testing.T) {
	c, err := Load("../../examples/warren.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if c.Node != "server" || c.Transport.Name != "tailscale" || c.Tailscale == nil || c.Tailscale.Tag != "warren" {
		t.Errorf("unexpected node or transport: %+v", c)
	}
	if len(c.Services.List) != 1 || c.Attach.Default == nil || len(c.Attach.Children) != 1 || !c.Attach.Children[0].Flat {
		t.Errorf("unexpected services or attach block: %+v", c)
	}
}

// Nothing Warren serves to other users may be in services.dir, which is
// closed to them.
func TestServicesDirHoldsNothingElse(t *testing.T) {
	rep := func(from, to string) func(string) string {
		return func(s string) string { return strings.Replace(s, from, to, 1) }
	}
	bad(t, rep("dir: /run/warren/attach", "dir: /run/warren/services"), "services.dir")
	bad(t, rep("dir: /run/warren/attach", "dir: /run/warren/services/attach"), "services.dir")
	bad(t, rep("state_dir: /var/lib/warren\n", "state_dir: /var/lib/warren\ncontrol: /run/warren/services/control.sock\n"), "services.dir")
	bad(t, func(s string) string {
		s = strings.Replace(s, "tailscale:\n  tag: warren\n  listen: \":7778\"\n  port: 7778\n", "", 1)
		return strings.Replace(s, "transport: tailscale", "transport: {link_a: /run/warren/services/a.sock, link_b: /run/warren/b.sock, uid: 1001}", 1)
	}, "services.dir")
	// A sibling with the same prefix is not inside it.
	if _, err := Parse([]byte(rep("dir: /run/warren/attach", "dir: /run/warren/services-attach")(sample))); err != nil {
		t.Errorf("a sibling directory was refused: %v", err)
	}
}
