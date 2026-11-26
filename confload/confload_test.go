package confload

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

type inner struct {
	Name string `yaml:"name"`
	Port int    `yaml:"port"`
}

type conf struct {
	Host   string            `yaml:"host"`
	URL    string            `yaml:"url"`
	Port   int               `yaml:"port"`
	On     bool              `yaml:"on"`
	Token  Secret            `yaml:"token"`
	Env    map[string]string `yaml:"env"`
	List   []inner           `yaml:"list"`
	Nested struct {
		Pass Secret `yaml:"pass"`
	} `yaml:"nested"`
	Opt *inner `yaml:"opt"`
}

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func load(t *testing.T, text string, vars map[string]string) (*conf, error) {
	t.Helper()
	var c conf
	err := parse([]byte(text), t.TempDir(), &c, env(vars))
	return &c, err
}

func TestExpansionForms(t *testing.T) {
	vars := map[string]string{"HOST": "example.org", "PORT": "8080", "EMPTY": ""}
	cases := map[string]string{
		"$HOST":                    "example.org",
		"${HOST}":                  "example.org",
		"http://${HOST}:${PORT}/x": "http://example.org:8080/x",
		"${MISSING:-fallback}":     "fallback",
		"${MISSING-fallback}":      "fallback",
		"${EMPTY:-fallback}":       "fallback", // :- also for empty
		"${EMPTY-fallback}":        "",         // - only for unset
		"${HOST:-fallback}":        "example.org",
		"${MISSING:-}":             "",        // an empty default is deliberate
		"$${A:+x}":                 "${A:+x}", // an escaped $ is a literal, not a refused form
		"${MISSING:-a$$b}":         "a$b",
		"${HOST:?needed}":          "example.org",
		"${HOST?needed}":           "example.org",
		"cost: $$5 and $${HOST}":   "cost: $5 and ${HOST}",
		"$$":                       "$",
		"plain text":               "plain text",
		"${EMPTY}":                 "", // set to empty is a value
	}
	for in, want := range cases {
		c, err := load(t, "host: '"+in+"'\n", vars)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if c.Host != want {
			t.Errorf("%q = %q, want %q", in, c.Host, want)
		}
	}
}

func TestUnsetWithoutDefaultIsAnError(t *testing.T) {
	_, err := load(t, "url: http://${NOPE}/\n", nil)
	if err == nil {
		t.Fatal("unset variable accepted")
	}
	for _, want := range []string{`"url"`, "line 1", "NOPE", "not set"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q: %v", want, err)
		}
	}
	if _, err := load(t, "host: $NOPE\n", nil); err == nil || !strings.Contains(err.Error(), "NOPE") {
		t.Errorf("bare $VAR: %v", err)
	}
}

func TestRequiredMessage(t *testing.T) {
	_, err := load(t, "host: ${NOPE:?set NOPE in the unit file}\n", nil)
	if err == nil || !strings.Contains(err.Error(), "NOPE") || !strings.Contains(err.Error(), "set NOPE in the unit file") || !strings.Contains(err.Error(), `"host"`) {
		t.Errorf("got %v", err)
	}
	// ${VAR?msg} fails only when unset; ${VAR:?msg} also when empty.
	if _, err := load(t, "host: ${E?m}\n", map[string]string{"E": ""}); err != nil {
		t.Errorf("empty with ?: %v", err)
	}
	if _, err := load(t, "host: ${E:?m}\n", map[string]string{"E": ""}); err == nil {
		t.Error("empty with :? accepted")
	}
}

func TestPlusFormsAndStrayDollarRefused(t *testing.T) {
	for _, in := range []string{"${A:+x}", "${A+x}"} {
		_, err := load(t, "host: '"+in+"'\n", map[string]string{"A": "1", "B": "2"})
		if err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Errorf("%q: %v", in, err)
		}
	}
	for _, in := range []string{"cost $5", "end$", "a $ b", "${}"} {
		if _, err := load(t, "host: '"+in+"'\n", nil); err == nil {
			t.Errorf("%q accepted", in)
		}
	}
}

func TestBcryptLikeLiteral(t *testing.T) {
	hash := "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"
	_, err := load(t, "host: '"+hash+"'\n", nil)
	if err == nil {
		t.Fatal("an unescaped bcrypt hash loaded")
	}
	if strings.Contains(err.Error(), "N9qo8u") && strings.Contains(err.Error(), "LJZdL17") {
		t.Errorf("error quotes the value: %v", err)
	}
	// Written with $$ it loads verbatim.
	c, err := load(t, "host: '"+strings.ReplaceAll(hash, "$", "$$")+"'\n", nil)
	if err != nil || c.Host != hash {
		t.Errorf("escaped hash: %q %v", c.Host, err)
	}
}

func TestSinglePassNoRecursion(t *testing.T) {
	// A variable whose value contains ${OTHER} stays literal.
	c, err := load(t, "host: ${A}\n", map[string]string{"A": "${OTHER}", "OTHER": "boom"})
	if err != nil || c.Host != "${OTHER}" {
		t.Errorf("got %q %v", c.Host, err)
	}
	// A value holding YAML cannot add structure.
	c, err = load(t, "host: ${A}\nport: 1\n", map[string]string{"A": "x\nport: 2\nhost: evil"})
	if err != nil || c.Port != 1 || c.Host != "x\nport: 2\nhost: evil" {
		t.Errorf("structure injected: %+v %v", c, err)
	}
	c, err = load(t, "host: ${A}\n", map[string]string{"A": "a: b # c"})
	if err != nil || c.Host != "a: b # c" {
		t.Errorf("special characters: %q %v", c.Host, err)
	}
}

func TestOnlyStringsAreExpanded(t *testing.T) {
	// A number or boolean written literally is left alone.
	c, err := load(t, "port: 8080\non: true\n", nil)
	if err != nil || c.Port != 8080 || !c.On {
		t.Errorf("%+v %v", c, err)
	}
	// A string that would expand to a number is not an int.
	if _, err := load(t, "port: ${P}\n", map[string]string{"P": "8080"}); err == nil {
		t.Error("an int field took an expanded string")
	}
	// Keys are not expanded.
	c, err = load(t, "env:\n  ${K}: v\n", map[string]string{"K": "x"})
	if err != nil || c.Env["${K}"] != "v" {
		t.Errorf("keys: %+v %v", c.Env, err)
	}
	// Nested values are, with their paths in errors.
	c, err = load(t, "list:\n  - name: ${N}\n    port: 1\n", map[string]string{"N": "svc"})
	if err != nil || c.List[0].Name != "svc" {
		t.Errorf("nested: %+v %v", c, err)
	}
	_, err = load(t, "list:\n  - name: a\n  - name: ${GONE}\n", nil)
	if err == nil || !strings.Contains(err.Error(), `"list[1].name"`) || !strings.Contains(err.Error(), "line 3") {
		t.Errorf("path: %v", err)
	}
	_, err = load(t, "env:\n  FOO: ${GONE}\n", nil)
	if err == nil || !strings.Contains(err.Error(), `"env.FOO"`) {
		t.Errorf("map path: %v", err)
	}
}

func TestStrictKeysNameThePath(t *testing.T) {
	for text, want := range map[string]string{
		"bogus: 1\n":                       `"bogus"`,
		"list:\n  - name: a\n    wat: 1\n": `"list[0].wat"`,
		"opt:\n  name: a\n  extra: 1\n":    `"opt.extra"`,
		"nested:\n  pass: x\n  more: 1\n":  `"nested.more"`,
	} {
		_, err := load(t, text, nil)
		if err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "unknown key") {
			t.Errorf("%q: %v", text, err)
		}
	}
	if _, err := load(t, "", nil); err == nil {
		t.Error("empty file accepted")
	}
}

func TestSecretFromEnvAndFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "tok"), []byte("filetoken\n\n"), 0o600)
	var c conf
	err := parse([]byte("token: ${TOK}\nnested:\n  pass: {file: tok}\n"), dir, &c, env(map[string]string{"TOK": "envtoken"}))
	if err != nil {
		t.Fatal(err)
	}
	// One trailing newline is trimmed.
	if c.Token.Reveal() != "envtoken" || c.Nested.Pass.Reveal() != "filetoken\n" {
		t.Errorf("secrets %q %q", c.Token.Reveal(), c.Nested.Pass.Reveal())
	}
	// Absolute paths work, and the path may use variables.
	c = conf{}
	err = parse([]byte("token: {file: '${D}/tok'}\n"), "/elsewhere", &c, env(map[string]string{"D": dir}))
	if err != nil || c.Token.Reveal() != "filetoken\n" {
		t.Errorf("absolute: %q %v", c.Token.Reveal(), err)
	}
}

func TestSecretErrors(t *testing.T) {
	dir := t.TempDir()
	var c conf
	err := parse([]byte("token: {file: missing}\n"), dir, &c, env(nil))
	if err == nil || !strings.Contains(err.Error(), `"token"`) || !strings.Contains(err.Error(), filepath.Join(dir, "missing")) {
		t.Errorf("missing file: %v", err)
	}
	if err := parse([]byte("token: ${E:-}\n"), dir, &conf{}, env(nil)); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("empty secret: %v", err)
	}
	os.WriteFile(filepath.Join(dir, "empty"), []byte("\n"), 0o600)
	if err := parse([]byte("token: {file: empty}\n"), dir, &conf{}, env(nil)); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("empty file: %v", err)
	}
	os.WriteFile(filepath.Join(dir, "big"), make([]byte, MaxSecretFile+1), 0o600)
	if err := parse([]byte("token: {file: big}\n"), dir, &conf{}, env(nil)); err == nil || !strings.Contains(err.Error(), "over") {
		t.Errorf("big file: %v", err)
	}
	// The limit itself is fine.
	os.WriteFile(filepath.Join(dir, "max"), []byte(strings.Repeat("x", MaxSecretFile)), 0o600)
	if err := parse([]byte("token: {file: max}\n"), dir, &conf{}, env(nil)); err != nil {
		t.Errorf("max file: %v", err)
	}
	if err := parse([]byte("token: {file: a, other: b}\n"), dir, &conf{}, env(nil)); err == nil {
		t.Error("odd secret mapping accepted")
	}
	if err := parse([]byte("token: [a]\n"), dir, &conf{}, env(nil)); err == nil {
		t.Error("list secret accepted")
	}
	// An unset variable in a secret names the variable, not a value.
	err = parse([]byte("token: ${SECRET_VAR}\n"), dir, &conf{}, env(nil))
	if err == nil || !strings.Contains(err.Error(), "SECRET_VAR") {
		t.Errorf("%v", err)
	}
}

func TestSecretIsRedactedEverywhere(t *testing.T) {
	const secret = "hunter2-very-secret"
	var c conf
	if err := parse([]byte("token: ${T}\nnested:\n  pass: ${T}\n"), t.TempDir(), &c, env(map[string]string{"T": secret})); err != nil {
		t.Fatal(err)
	}
	outputs := map[string]string{
		"%v":       fmt.Sprintf("%v", c.Token),
		"%+v":      fmt.Sprintf("%+v", c.Token),
		"%#v":      fmt.Sprintf("%#v", c.Token),
		"%s":       fmt.Sprintf("%s", c.Token),
		"%q":       fmt.Sprintf("%q", c.Token),
		"%x":       fmt.Sprintf("%x", c.Token),
		"%d":       fmt.Sprintf("%d", c.Token),
		"conf %v":  fmt.Sprintf("%v", c),
		"conf %+v": fmt.Sprintf("%+v", c),
		"conf %#v": fmt.Sprintf("%#v", &c),
		"ptr":      fmt.Sprint(&c.Token),
		"Sprint":   fmt.Sprint(c.Token),
	}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	outputs["json"] = string(b)
	y, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	outputs["yaml"] = string(y)
	for name, out := range outputs {
		if strings.Contains(out, secret) || strings.Contains(out, "hunter") {
			t.Errorf("%s leaks the secret: %s", name, out)
		}
	}
	if outputs["%v"] != "[secret]" || !strings.Contains(outputs["json"], `"[secret]"`) || !strings.Contains(outputs["yaml"], "[secret]") {
		t.Errorf("redaction forms: %v", outputs)
	}
	if c.Token.Reveal() != secret {
		t.Error("Reveal lost the value")
	}
}

func TestErrorsNeverContainResolvedValues(t *testing.T) {
	const secret = "s3cr3t-value-xyz"
	vars := map[string]string{"A": secret}
	texts := []string{
		"port: ${A}\n",              // type error after expansion
		"host: ${A}-${B}\n",         // missing B
		"host: ${A}\nbogus: ${A}\n", // unknown key
		"host: ${A}\nlist: ${A}\n",  // wrong shape
		"token: {file: '${A}'}\n",   // unreadable file named by its path only
	}
	for _, text := range texts {
		var c conf
		err := parse([]byte(text), t.TempDir(), &c, env(vars))
		if err == nil {
			continue
		}
		// A file path is named by its path (which the config wrote), so the
		// last case is allowed to show the path it was given.
		if strings.Contains(err.Error(), secret) && !strings.HasPrefix(text, "token:") {
			t.Errorf("%q: error shows a resolved value: %v", text, err)
		}
	}
}

func TestLoadFileAndRelativeSecret(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "pw"), []byte("pw-from-file\n"), 0o600)
	cfg := filepath.Join(dir, "c.yaml")
	os.WriteFile(cfg, []byte("host: ${H:-localhost}\nnested:\n  pass: {file: pw}\n"), 0o600)
	var c conf
	if err := Load(cfg, &c); err != nil {
		t.Fatal(err)
	}
	if c.Host != "localhost" || c.Nested.Pass.Reveal() != "pw-from-file" {
		t.Errorf("%+v", c)
	}
	// Reload resolves again: a rotated file takes effect.
	os.WriteFile(filepath.Join(dir, "pw"), []byte("rotated\n"), 0o600)
	var c2 conf
	if err := Load(cfg, &c2); err != nil || c2.Nested.Pass.Reveal() != "rotated" {
		t.Errorf("rotation: %v", err)
	}
	if err := Load(filepath.Join(dir, "nope.yaml"), &c); err == nil {
		t.Error("missing file accepted")
	}
	if err := Load(cfg, c); err == nil {
		t.Error("non-pointer accepted")
	}
}
