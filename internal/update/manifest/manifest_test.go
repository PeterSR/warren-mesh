package manifest

import (
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"

	"aead.dev/minisign"
)

type kp struct {
	pub  minisign.PublicKey
	priv minisign.PrivateKey
	key  Key
}

func newKey(t *testing.T) kp {
	t.Helper()
	pub, priv, err := minisign.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	k, err := ParseKey(pub.String())
	if err != nil {
		t.Fatal(err)
	}
	return kp{pub, priv, k}
}

func sample() *Manifest {
	f := File{Name: "myapp_1.4.0-dev.7_linux_amd64", Size: 100, SHA256: strings.Repeat("ab", 32)}
	return &Manifest{Format: Format, Component: "myapp", Version: "1.4.0-dev.7", Channel: Dev, Created: "2026-11-26T10:00:00Z",
		Platforms: map[string]Platform{"linux/amd64": {Files: map[string]File{"myapp": f}}, "linux/arm64": {Files: map[string]File{"myapp": f}}}}
}

func signed(t *testing.T, m *Manifest, k kp) (data, sig []byte) {
	t.Helper()
	data, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return data, Sign(k.priv, data, m)
}

func TestVerifyAccepts(t *testing.T) {
	k := newKey(t)
	data, sig := signed(t, sample(), k)
	m, err := Verify(data, sig, "myapp", Keys{Dev: {k.key}}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != "1.4.0-dev.7" || strings.Join(m.Targets(), ",") != "myapp" || m.Manual {
		t.Errorf("manifest %+v", m)
	}
}

func TestVerifyRefusals(t *testing.T) {
	k, other := newKey(t), newKey(t)
	good := func() (*Manifest, []byte, []byte) {
		m := sample()
		d, s := signed(t, m, k)
		return m, d, s
	}
	keys := Keys{Dev: {k.key}}
	check := func(name string, data, sig []byte, component string, keys Keys, want string) {
		t.Helper()
		_, err := Verify(data, sig, component, keys, 1<<20)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want %q", name, err, want)
		}
	}
	m, d, s := good()
	check("wrong key", d, s, "myapp", Keys{Dev: {other.key}}, "does not verify")
	check("no key for the channel", d, s, "myapp", Keys{Stable: {k.key}}, "holds no dev key")
	check("wrong component (binding)", d, s, "warren", keys, "not \"warren\"")

	// A tampered body.
	check("tampered", []byte(strings.Replace(string(d), "1.4.0-dev.7", "1.4.0-dev.8", 1)), s, "myapp", keys, "does not verify")
	check("truncated", d[:len(d)-5], s, "myapp", keys, "manifest")
	check("truncated signature", d, s[:len(s)-10], "myapp", keys, "signature")

	// Legacy (not prehashed) signatures are refused.
	check("legacy", d, signLegacy(k.priv, d, m), "myapp", keys, "prehashed")

	// The signed comment must repeat the body.
	bad := minisign.SignWithComments(k.priv, d, "warren-manifest/1 myapp 9.9.9-dev.1 dev", "x")
	_ = bad
	// (signed over the plain message, so it is also not prehashed)
	r := minisign.NewReader(strings.NewReader(string(d)))
	buf := make([]byte, len(d))
	r.Read(buf)
	sig2 := r.SignWithComments(k.priv, "warren-manifest/1 myapp 9.9.9-dev.1 dev", "x")
	check("comment mismatch", d, sig2, "myapp", keys, "comment")

	// The same key bytes registered under another id still verify by bytes only for the
	// configured component: a manifest for myapp fails for warren even with the right key.
	check("same key, other component", d, s, "warren-files", keys, "not \"warren-files\"")
}

func TestParseStrict(t *testing.T) {
	d, _ := sample().Marshal()
	var generic map[string]any
	json.Unmarshal(d, &generic)
	generic["notes"] = "hi"
	withExtra, _ := json.Marshal(generic)
	for name, data := range map[string][]byte{
		"unknown field":              withExtra,
		"trailing":                   append(append([]byte{}, d...), []byte(`{}`)...),
		"too large":                  []byte(`{"format":"` + strings.Repeat("x", MaxBytes) + `"}`),
		"not utf8":                   []byte("{\"format\":\"\xff\"}"),
		"not json":                   []byte("hello"),
		"duplicate key":              []byte(strings.Replace(string(d), `"component": "myapp"`, `"component": "warren", "component": "myapp"`, 1)),
		"duplicate target":           []byte(strings.Replace(string(d), `"myapp": {`, `"myapp": {"name": "x", "size": 1, "sha256": "`+strings.Repeat("0", 64)+`"}, "myapp": {`, 1)),
		"key of another case":        []byte(strings.Replace(string(d), `"component"`, `"Component"`, 1)),
		"nested key of another case": []byte(strings.Replace(string(d), `"sha256"`, `"SHA256"`, 1)),
	} {
		if _, err := Parse(data); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := Parse(d); err != nil {
		t.Fatal(err)
	}
}

func TestCheck(t *testing.T) {
	cases := map[string]func(m *Manifest){
		"format":             func(m *Manifest) { m.Format = "warren-manifest/2" },
		"component":          func(m *Manifest) { m.Component = "Myapp" },
		"component too long": func(m *Manifest) { m.Component = strings.Repeat("a", 33) },
		"dev with a release": func(m *Manifest) { m.Version = "1.4.0" },
		"stable with a pre":  func(m *Manifest) { m.Channel = Stable },
		"channel":            func(m *Manifest) { m.Channel = "beta" },
		"build metadata":     func(m *Manifest) { m.Version = "1.4.0-dev.7+x" },
		"created":            func(m *Manifest) { m.Created = "yesterday" },
		"no platforms":       func(m *Manifest) { m.Platforms = nil },
		"platform name":      func(m *Manifest) { m.Platforms["linux-amd64"] = m.Platforms["linux/amd64"] },
		"different targets": func(m *Manifest) {
			m.Platforms["linux/arm64"] = Platform{Files: map[string]File{"other": m.Platforms["linux/amd64"].Files["myapp"]}}
		},
		"zero size": func(m *Manifest) {
			f := m.Platforms["linux/amd64"].Files["myapp"]
			f.Size = 0
			m.Platforms["linux/amd64"].Files["myapp"] = f
		},
		"too big": func(m *Manifest) {
			f := m.Platforms["linux/amd64"].Files["myapp"]
			f.Size = 1 << 30
			m.Platforms["linux/amd64"].Files["myapp"] = f
		},
		"digest": func(m *Manifest) {
			f := m.Platforms["linux/amd64"].Files["myapp"]
			f.SHA256 = "AB"
			m.Platforms["linux/amd64"].Files["myapp"] = f
		},
		"name with a slash": func(m *Manifest) {
			f := m.Platforms["linux/amd64"].Files["myapp"]
			f.Name = "../x"
			m.Platforms["linux/amd64"].Files["myapp"] = f
		},
		"name with a dot start": func(m *Manifest) {
			f := m.Platforms["linux/amd64"].Files["myapp"]
			f.Name = ".x"
			m.Platforms["linux/amd64"].Files["myapp"] = f
		},
		"target with a slash": func(m *Manifest) { m.Platforms["linux/amd64"].Files["a/b"] = m.Platforms["linux/amd64"].Files["myapp"] },
	}
	for name, mod := range cases {
		m := sample()
		mod(m)
		if err := m.Check(1 << 20); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	m := sample()
	if err := m.Check(1 << 20); err != nil {
		t.Fatal(err)
	}
	m.Manual = true
	if err := m.Check(1 << 20); err != nil {
		t.Errorf("manual refused: %v", err)
	}
	// Nine platforms and nine targets are too many.
	m = sample()
	for _, p := range []string{"a/a", "a/b", "a/c", "a/d", "a/e", "a/f"} {
		m.Platforms[p] = m.Platforms["linux/amd64"]
	}
	if err := m.Check(1 << 20); err != nil {
		t.Fatalf("8 platforms refused: %v", err)
	}
	m.Platforms["a/h"] = m.Platforms["linux/amd64"]
	if err := m.Check(1 << 20); err == nil {
		t.Error("9 platforms accepted")
	}
}

func TestKeys(t *testing.T) {
	a := newKey(t)
	if _, err := ParseKey(a.key.Text + "A"); err == nil {
		t.Error("a long key accepted")
	}
	if _, err := ParseKey("untrusted comment: x\n" + a.key.Text); err == nil {
		t.Error("a key with a comment line accepted")
	}
	if _, err := ParseKey("RWQ"); err == nil {
		t.Error("a short key accepted")
	}
	b := newKey(t)
	if a.key.Same(b.key) || !a.key.Same(a.key) {
		t.Error("Same is wrong")
	}
	// The same 32 bytes under another id are the same key.
	raw := []byte(a.key.Text)
	_ = raw
}

// signLegacy makes a plain (Ed, not prehashed) signature, which must be refused.
func signLegacy(key minisign.PrivateKey, data []byte, m *Manifest) []byte {
	return minisign.SignWithComments(key, data, m.TrustedComment(), "legacy")
}
