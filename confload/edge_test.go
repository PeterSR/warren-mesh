package confload

import (
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestNestedVariablesInDefaultsAreRefused(t *testing.T) {
	vars := map[string]string{"SECRETVAL": "hunter2secret"}
	for _, in := range []string{"${A:-${B}}", "${A:?${SECRETVAL}}", "${A-$B}", "${A?$SECRETVAL}", "pre ${A:-${B:+x}}"} {
		_, err := load(t, "host: '"+in+"'\n", vars)
		if err == nil || !strings.Contains(err.Error(), "cannot contain a variable") {
			t.Errorf("%q: %v", in, err)
		}
		if err != nil && strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%q leaked a value: %v", in, err)
		}
	}
}

func TestTypeErrorsNameTheKeyNotTheValue(t *testing.T) {
	type tc struct {
		Port int      `yaml:"port"`
		Flag bool     `yaml:"flag"`
		List []string `yaml:"list"`
		Sub  struct {
			N int `yaml:"n"`
		} `yaml:"sub"`
	}
	vars := map[string]string{"PORT": "hunter2secret"}
	for in, key := range map[string]string{
		"port: ${PORT}\n":          `"port"`,
		"flag: ${PORT}\n":          `"flag"`,
		"sub:\n  n: ${PORT}\n":     `"sub.n"`,
		"port: plain-text-value\n": `"port"`,
	} {
		var c tc
		err := parse([]byte(in), "", &c, env(vars))
		if err == nil {
			t.Errorf("%q accepted", in)
			continue
		}
		if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "plain-text-value") {
			t.Errorf("%q: the error holds a value: %v", in, err)
		}
		if !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), "line ") {
			t.Errorf("%q: the error lacks the key path %s or the line: %v", in, key, err)
		}
	}
}

func TestCustomTagsAreRefused(t *testing.T) {
	_, err := load(t, "host: !foo $HOST\n", map[string]string{"HOST": "x"})
	if err == nil || !strings.Contains(err.Error(), "custom YAML tags") {
		t.Errorf("custom tag: %v", err)
	}
	if c, err := load(t, "host: !!str $HOST\n", map[string]string{"HOST": "x"}); err != nil || c.Host != "x" {
		t.Errorf("!!str: %v", err)
	}
}

func TestTypeErrorInAFlowMappingNamesTheRightKey(t *testing.T) {
	var c struct {
		Sub struct {
			M int `yaml:"m"`
			N int `yaml:"n"`
		} `yaml:"sub"`
		List []int `yaml:"list"`
	}
	err := parse([]byte("sub: {m: 1, n: \"${PORT}\"}\n"), "", &c, env(map[string]string{"PORT": "hunter2secret"}))
	if err == nil || !strings.Contains(err.Error(), `"sub.n"`) || strings.Contains(err.Error(), "sub.m") || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("flow mapping: %v", err)
	}
	err = parse([]byte("list: [1, \"${PORT}\", 3]\n"), "", &c, env(map[string]string{"PORT": "hunter2secret"}))
	if err == nil || !strings.Contains(err.Error(), `"list[1]"`) || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("flow sequence: %v", err)
	}
}

func TestCustomTagsOnCollectionsAreRefused(t *testing.T) {
	for _, in := range []string{"env: !foo {a: b}\n", "list: !foo [{name: x}]\n"} {
		if _, err := load(t, in, nil); err == nil || !strings.Contains(err.Error(), "custom YAML tags") {
			t.Errorf("%q: %v", in, err)
		}
	}
}

type base struct {
	Region string `yaml:"region"`
	Key    Secret `yaml:"key"`
}

type withInline struct {
	base  `yaml:",inline"`
	Extra string `yaml:"extra"`
}

// The keys of an inlined struct are keys of its parent, not unknown ones, and
// are expanded and resolved like any other.
func TestInlineStructKeysAreKnown(t *testing.T) {
	var c struct {
		withInline `yaml:",inline"`
	}
	vars := map[string]string{"R": "eu", "K": "s3cret"}
	err := parse([]byte("region: ${R}\nextra: x\nkey: ${K}\n"), t.TempDir(), &c, env(vars))
	if err != nil {
		t.Fatal(err)
	}
	if c.Region != "eu" || c.Extra != "x" || c.Key.Reveal() != "s3cret" {
		t.Errorf("got %q %q %v", c.Region, c.Extra, c.Key.IsSet())
	}
	err = parse([]byte("region: eu\nbogus: 1\n"), t.TempDir(), &c, env(vars))
	if err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Errorf("an unknown key next to inlined ones: %v", err)
	}
	// A Secret that is an empty string in an inlined struct is still refused,
	// naming the key.
	err = parse([]byte("region: eu\nkey: ''\n"), t.TempDir(), &c, env(vars))
	if err == nil || !strings.Contains(err.Error(), `"key"`) {
		t.Errorf("an empty inlined secret: %v", err)
	}
}

// A Secret decoded into a value that already holds one starts from nothing:
// a key that is absent the second time does not keep the first value.
func TestSecretUnmarshalResets(t *testing.T) {
	s := Secret{value: "old", file: "f", set: true}
	if err := s.UnmarshalYAML(&yaml.Node{Kind: yaml.ScalarNode, Value: "new"}); err != nil {
		t.Fatal(err)
	}
	if s.Reveal() != "new" || s.file != "" {
		t.Errorf("not reset: %q %q", s.value, s.file)
	}
	if err := s.UnmarshalYAML(&yaml.Node{Kind: yaml.SequenceNode}); err == nil || s.IsSet() {
		t.Errorf("a bad node left the old secret: %v %v", err, s.IsSet())
	}
}
