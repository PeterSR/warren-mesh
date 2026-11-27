// Package confload is the one way every Warren configuration, and the
// configuration of its transports and services, is loaded.
//
// The file is read as YAML into a node tree and decoded strictly: an unknown
// key is an error that names its key path. Then every string-typed value is
// expanded with Docker Compose syntax (the template package of
// github.com/compose-spec/compose-go/v2): $VAR, ${VAR}, ${VAR:-default},
// ${VAR-default}, ${VAR:?message}, ${VAR?message}, and $$ for a literal $.
// Numbers and booleans are never expanded. Expansion happens after parsing,
// in a single pass: a value that holds a $ or YAML (a bcrypt hash, a token)
// is never scanned again and cannot add structure.
//
//   - A bare $VAR or ${VAR} that is unset is an error, never an empty
//     string. An empty default (${VAR:-}) is a deliberate empty value.
//   - The ${VAR:+alt} forms are refused, and so is a stray $ (one that is not
//     followed by $, { or a letter or underscore): a literal $ is written $$.
//   - A default or an error message cannot hold a variable (${A:-${B}} is
//     refused), and custom YAML tags are refused.
//   - Errors name the key path and the line, and the variable when one is
//     missing. They never contain a resolved value, and neither the template
//     package's error text nor the YAML library's type errors, which quote
//     the value, are passed on.
//
// Values that must not live in the file are Secret fields: a string
// (normally "${VAR}") or {file: path}, relative to the config file's
// directory. A Secret prints as [secret] everywhere.
//
// Load and reload resolve everything again, so a rotated secret file takes
// effect when the program reloads. A changed environment variable needs the
// program restarted, since its environment is fixed at start.
//
// The template package logs through logrus; this package sends logrus to
// nowhere, process wide. A program that logs with logrus itself should know.
package confload

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/compose-spec/compose-go/v2/template"
	"github.com/sirupsen/logrus"
	"go.yaml.in/yaml/v3"
)

func init() { logrus.SetOutput(io.Discard) }

// MaxSecretFile is the most a {file: ...} secret may hold.
const MaxSecretFile = 64 << 10

// Load reads the YAML file at path into cfg (a pointer to a struct).
func Load(path string, cfg any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return Parse(data, filepath.Dir(path), cfg)
}

// Parse is Load for data already read. dir is what relative secret file
// paths are relative to ("" means the working directory).
func Parse(data []byte, dir string, cfg any) error {
	return parse(data, dir, cfg, os.LookupEnv)
}

func parse(data []byte, dir string, cfg any, env func(string) (string, bool)) error {
	rv := reflect.ValueOf(cfg)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("confload: cfg must be a non-nil pointer")
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if doc.Kind == 0 || len(doc.Content) == 0 {
		return errors.New("config is empty")
	}
	root := doc.Content[0]
	if err := checkKeys(root, rv.Elem().Type(), ""); err != nil {
		return err
	}
	if err := expand(root, "", env); err != nil {
		return err
	}
	// Decode the expanded tree strictly. Encoding it back makes a plain
	// decoder of it; expanded values are written quoted, so what they hold
	// can never be read as structure.
	// Block style puts every value on a line of its own, so that a type
	// error's line number names one key, even where the file used {a: 1, b: 2}.
	blockStyle(root)
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	if err := enc.Encode(root); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	enc.Close()
	encoded := append([]byte(nil), buf.Bytes()...)
	dec := yaml.NewDecoder(&buf)
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		var te *yaml.TypeError
		if errors.As(err, &te) {
			return typeError(te, encoded, root)
		}
		// An error a type's UnmarshalYAML marked with Public is its own text,
		// which names no value.
		var pe *publicError
		if errors.As(err, &pe) {
			return fmt.Errorf("config: %w", pe)
		}
		// The library's own structural errors that are known to carry no value
		// are cheap to report as they are. Others (an invalid map key quotes
		// the key's value, which an alias may have taken from an expanded
		// one) are not.
		if safeYAMLError.MatchString(err.Error()) {
			return fmt.Errorf("config: %w", err)
		}
		// Anything else the decoder says (a value a type's own parser refused,
		// say) may quote the value, which may be a secret that "${VAR}"
		// expanded to: none of it is passed on, only where it happened.
		return decodeError(root, rv.Elem().Type())
	}
	return resolveSecrets(reflect.ValueOf(cfg).Elem(), "", dir)
}

func blockStyle(n *yaml.Node) {
	if n.Kind == yaml.MappingNode || n.Kind == yaml.SequenceNode {
		n.Style &^= yaml.FlowStyle
	}
	for _, c := range n.Content {
		blockStyle(c)
	}
}

// typeError turns the YAML library's type errors, which quote the value that
// did not fit (and so could print a secret that "${VAR}" expanded to), into
// messages built from the key path alone. The library's line numbers are
// those of the re-encoded text, so the key path is found there and the line
// is then taken from the original tree.
func typeError(te *yaml.TypeError, encoded []byte, orig *yaml.Node) error {
	var doc yaml.Node
	_ = yaml.Unmarshal(encoded, &doc)
	var msgs []string
	for _, e := range te.Errors {
		rest, ok := strings.CutPrefix(e, "line ")
		num, after, ok2 := strings.Cut(rest, ": ")
		line, err := strconv.Atoi(num)
		if !ok || !ok2 || err != nil {
			msgs = append(msgs, "a value has the wrong type")
			continue
		}
		want := ""
		if i := strings.LastIndex(after, " into "); i >= 0 && strings.HasPrefix(after, "cannot unmarshal") {
			want = ", want " + after[i+len(" into "):]
		}
		var path []any
		if len(doc.Content) > 0 {
			path, _ = pathAtLine(doc.Content[0], line, nil)
		}
		if path == nil {
			msgs = append(msgs, "a value has the wrong type"+want)
			continue
		}
		origLine := 0
		if n := nodeAt(orig, path); n != nil {
			origLine = n.Line
		}
		msgs = append(msgs, fmt.Sprintf("config key %q (line %d): the value has the wrong type%s", pathString(path), origLine, want))
	}
	return errors.New(strings.Join(msgs, "; "))
}

// safeYAMLError matches the messages of the YAML library that name no value
// (an anchor's name is the file's own text, not an expanded value).
var safeYAMLError = regexp.MustCompile(`^yaml: (document contains excessive aliasing|unknown anchor '[^']*' referenced|anchor '[^']*' value contains itself|map merge requires map or sequence of maps as the value)$`)

type publicError struct{ err error }

func (e *publicError) Error() string { return e.err.Error() }
func (e *publicError) Unwrap() error { return e.err }

// Public marks an error returned by a type's UnmarshalYAML as one whose text
// is the type's own and names no value that was read (a key is not a value, a
// secret is). Such an error is shown as it is; every other decode error is
// replaced by the key path alone.
func Public(err error) error {
	if err == nil {
		return nil
	}
	return &publicError{err}
}

// decodeError is the error for a value that the decoder refused without a
// position: the key path is found by decoding the document again with one
// key (or one element) at a time, and the message says nothing of the value.
func decodeError(orig *yaml.Node, rt reflect.Type) error {
	budget := maxResolvedNodes
	r := resolved(orig, 0, &budget)
	if budget < 0 {
		return errors.New("config: a value could not be decoded")
	}
	fails := func(path []any) bool {
		var buf bytes.Buffer
		if yaml.NewEncoder(&buf).Encode(only(r, path)) != nil {
			return false
		}
		dec := yaml.NewDecoder(&buf)
		dec.KnownFields(true)
		return dec.Decode(reflect.New(rt).Interface()) != nil
	}
	var walk func(n *yaml.Node, path []any) []any
	walk = func(n *yaml.Node, path []any) []any {
		var kids []*yaml.Node
		var segs []any
		switch n.Kind {
		case yaml.MappingNode:
			for i := 0; i+1 < len(n.Content); i += 2 {
				kids, segs = append(kids, n.Content[i+1]), append(segs, n.Content[i].Value)
			}
		case yaml.SequenceNode:
			for i, c := range n.Content {
				kids, segs = append(kids, c), append(segs, i)
			}
		}
		for i, k := range kids {
			p := append(append([]any(nil), path...), segs[i])
			if fails(p) {
				if deeper := walk(k, p); deeper != nil {
					return deeper
				}
				return p
			}
		}
		return nil
	}
	path := walk(r, nil)
	if path == nil {
		return errors.New("config: a value could not be decoded")
	}
	line := 0
	if n := nodeAt(orig, path); n != nil {
		line = n.Line
	}
	return fmt.Errorf("config key %q (line %d): the value could not be decoded", pathString(path), line)
}

// maxResolvedNodes is the most nodes resolved makes: aliases can name aliases,
// so a small document can stand for a tree of any size.
const maxResolvedNodes = 100_000

// resolved is a copy of n with every alias replaced by a copy of what it
// names, so that a part of the tree stands alone. It makes at most
// *budget nodes; one more makes the budget negative and the copy is useless.
func resolved(n *yaml.Node, depth int, budget *int) *yaml.Node {
	if *budget < 0 {
		return n
	}
	if n.Kind == yaml.AliasNode && n.Alias != nil && depth < 64 {
		return resolved(n.Alias, depth+1, budget)
	}
	if *budget--; *budget < 0 {
		return n
	}
	c := *n
	c.Anchor, c.Alias, c.Content = "", nil, nil
	if depth < 64 {
		for _, x := range n.Content {
			c.Content = append(c.Content, resolved(x, depth+1, budget))
		}
	}
	return &c
}

// only is a document that holds just the value at path of n.
func only(n *yaml.Node, path []any) *yaml.Node {
	doc := &yaml.Node{Kind: yaml.DocumentNode}
	var build func(n *yaml.Node, path []any) *yaml.Node
	build = func(n *yaml.Node, path []any) *yaml.Node {
		if len(path) == 0 {
			return n
		}
		c := *n
		c.Content = nil
		switch seg := path[0].(type) {
		case string:
			for i := 0; i+1 < len(n.Content); i += 2 {
				if n.Content[i].Value == seg {
					c.Content = []*yaml.Node{n.Content[i], build(n.Content[i+1], path[1:])}
					break
				}
			}
		case int:
			if seg < len(n.Content) {
				c.Content = []*yaml.Node{build(n.Content[seg], path[1:])}
			}
		}
		return &c
	}
	doc.Content = []*yaml.Node{build(n, path)}
	return doc
}

// pathAtLine finds the path of the first value node that starts on line.
func pathAtLine(n *yaml.Node, line int, path []any) ([]any, bool) {
	switch n.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			p := append(append([]any(nil), path...), n.Content[i].Value)
			v := n.Content[i+1]
			if v.Kind == yaml.ScalarNode && v.Line == line {
				return p, true
			}
			if got, ok := pathAtLine(v, line, p); ok {
				return got, true
			}
		}
	case yaml.SequenceNode:
		for i, v := range n.Content {
			p := append(append([]any(nil), path...), i)
			if v.Kind == yaml.ScalarNode && v.Line == line {
				return p, true
			}
			if got, ok := pathAtLine(v, line, p); ok {
				return got, true
			}
		}
	}
	return nil, false
}

// nodeAt follows a path made by pathAtLine.
func nodeAt(n *yaml.Node, path []any) *yaml.Node {
	for _, seg := range path {
		switch s := seg.(type) {
		case string:
			var next *yaml.Node
			if n.Kind == yaml.MappingNode {
				for i := 0; i+1 < len(n.Content); i += 2 {
					if n.Content[i].Value == s {
						next = n.Content[i+1]
						break
					}
				}
			}
			if next == nil {
				return nil
			}
			n = next
		case int:
			if n.Kind != yaml.SequenceNode || s >= len(n.Content) {
				return nil
			}
			n = n.Content[s]
		}
	}
	return n
}

func pathString(path []any) string {
	var b strings.Builder
	for _, seg := range path {
		switch s := seg.(type) {
		case string:
			if b.Len() > 0 {
				b.WriteByte('.')
			}
			b.WriteString(s)
		case int:
			fmt.Fprintf(&b, "[%d]", s)
		}
	}
	return b.String()
}

// ---- expansion ----

var varName = regexp.MustCompile(`^\$\{?([_A-Za-z][_A-Za-z0-9]*)`)

// expand walks the tree and expands every string value in place.
func expand(n *yaml.Node, path string, env func(string) (string, bool)) error {
	if n.Kind == yaml.MappingNode || n.Kind == yaml.SequenceNode {
		if tag := n.ShortTag(); strings.HasPrefix(tag, "!") && !strings.HasPrefix(tag, "!!") {
			return fmt.Errorf("config key %q (line %d): custom YAML tags are not supported", path, n.Line)
		}
	}
	switch n.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			key := n.Content[i].Value
			p := key
			if path != "" {
				p = path + "." + key
			}
			// Keys are not expanded; values are.
			if err := expand(n.Content[i+1], p, env); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		for i, c := range n.Content {
			if err := expand(c, fmt.Sprintf("%s[%d]", path, i), env); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		if tag := n.ShortTag(); strings.HasPrefix(tag, "!") && !strings.HasPrefix(tag, "!!") {
			return fmt.Errorf("config key %q (line %d): custom YAML tags are not supported", path, n.Line)
		}
		if n.ShortTag() != "!!str" || !strings.Contains(n.Value, "$") {
			return nil
		}
		v, err := expandString(n.Value, env)
		if err != nil {
			return fmt.Errorf("config key %q (line %d): %w", path, n.Line, err)
		}
		n.Value = v
		n.Tag = "!!str"
		n.Style = yaml.DoubleQuotedStyle
	}
	return nil
}

// checkSyntax refuses what this package does not support, before the
// template package sees the string: a stray $, the ${VAR:+alt} forms, and a
// variable inside a default or an error message (${A:-${B}}), which would
// be expanded by the template package's own lookups and could print a value
// the author did not mean to. Its errors name no value.
func checkSyntax(s string) error {
	for i := 0; i < len(s); i++ {
		if s[i] != '$' {
			continue
		}
		next := byte(0)
		if i+1 < len(s) {
			next = s[i+1]
		}
		switch {
		case next == '$':
			i++ // an escaped $
		case next == '_', next >= 'a' && next <= 'z', next >= 'A' && next <= 'Z':
		case next == '{':
			j := i + 2
			for j < len(s) && (s[j] == '_' || s[j] >= 'a' && s[j] <= 'z' || s[j] >= 'A' && s[j] <= 'Z' || s[j] >= '0' && s[j] <= '9') {
				j++
			}
			rest := s[j:]
			if strings.HasPrefix(rest, "+") || strings.HasPrefix(rest, ":+") {
				return errors.New("the ${VAR:+alt} and ${VAR+alt} forms are not supported")
			}
			if j == i+2 {
				return errors.New("invalid ${...} expression")
			}
			for _, op := range []string{":-", "-", ":?", "?"} {
				if !strings.HasPrefix(rest, op) {
					continue
				}
				// Scan the default or message up to its closing brace.
				depth := 1
				for k := j + len(op); k < len(s) && depth > 0; k++ {
					switch s[k] {
					case '$':
						if k+1 < len(s) && s[k+1] == '$' {
							k++
						} else {
							return errors.New("a default or message after :- or :? cannot contain a variable; write $$ for a literal $")
						}
					case '{':
						depth++
					case '}':
						depth--
					}
				}
				break
			}
		default:
			return errors.New("contains a stray $ (write $$ for a literal $)")
		}
	}
	return nil
}

// ExpandString expands the variables in one string with the process
// environment, under the same rules as a config value.
func ExpandString(s string) (string, error) {
	if !strings.Contains(s, "$") {
		return s, nil
	}
	return expandString(s, os.LookupEnv)
}

// expandString expands one string. Its errors name the variable at most.
func expandString(s string, env func(string) (string, bool)) (string, error) {
	if err := checkSyntax(s); err != nil {
		return "", err
	}
	out, err := template.SubstituteWithOptions(s, env, template.WithoutLogging,
		template.WithReplacementFunction(func(sub string, m template.Mapping, cfg *template.Config) (string, error) {
			v, applied, err := template.DefaultReplacementAppliedFunc(sub, m, cfg)
			if err != nil {
				return "", err
			}
			if !applied {
				// Unset with no default: never an empty string.
				name := "?"
				if g := varName.FindStringSubmatch(sub); g != nil {
					name = g[1]
				}
				return "", &unsetError{name}
			}
			return v, nil
		}))
	if err != nil {
		var u *unsetError
		if errors.As(err, &u) {
			return "", fmt.Errorf("variable %s is not set (give it a value, or a default with ${%s:-default})", u.name, u.name)
		}
		// The template package returns its error by pointer, or by value.
		var req template.MissingRequiredError
		var reqp *template.MissingRequiredError
		if errors.As(err, &reqp) {
			req = *reqp
		}
		if req.Variable != "" || errors.As(err, &req) {
			// The message is the config author's own text, written for this.
			if req.Reason != "" {
				return "", fmt.Errorf("required variable %s is missing a value: %s", req.Variable, req.Reason)
			}
			return "", fmt.Errorf("required variable %s is missing a value", req.Variable)
		}
		return "", errors.New("invalid ${...} expression")
	}
	return out, nil
}

type unsetError struct{ name string }

func (e *unsetError) Error() string { return "variable " + e.name + " is not set" }

// ---- strict keys with key paths ----

var (
	unmarshalerType = reflect.TypeOf((*yaml.Unmarshaler)(nil)).Elem()
	secretType      = reflect.TypeOf(Secret{})
)

// checkKeys refuses keys that the target type does not have, naming the key
// path. Types that decode themselves (Secret, and anything with an
// UnmarshalYAML method) check their own keys.
func checkKeys(n *yaml.Node, t reflect.Type, path string) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Implements(unmarshalerType) || reflect.PointerTo(t).Implements(unmarshalerType) {
		return nil
	}
	switch n.Kind {
	case yaml.MappingNode:
		switch t.Kind() {
		case reflect.Struct:
			fields := yamlFields(t)
			for i := 0; i+1 < len(n.Content); i += 2 {
				k := n.Content[i]
				p := k.Value
				if path != "" {
					p = path + "." + k.Value
				}
				ft, ok := fields[k.Value]
				if !ok {
					return fmt.Errorf("config key %q (line %d): unknown key", p, k.Line)
				}
				if err := checkKeys(n.Content[i+1], ft, p); err != nil {
					return err
				}
			}
		case reflect.Map:
			for i := 0; i+1 < len(n.Content); i += 2 {
				p := n.Content[i].Value
				if path != "" {
					p = path + "." + p
				}
				if err := checkKeys(n.Content[i+1], t.Elem(), p); err != nil {
					return err
				}
			}
		}
	case yaml.SequenceNode:
		if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
			for i, c := range n.Content {
				if err := checkKeys(c, t.Elem(), fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// yamlFields maps YAML key names to field types, following yaml.v3's rules:
// the tag name, else the lower-cased field name. The fields of a struct (or
// pointer to struct) tagged ",inline" are merged in, recursively, as if they
// were fields of t itself.
func yamlFields(t reflect.Type) map[string]reflect.Type {
	out := map[string]reflect.Type{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() && !f.Anonymous { // yaml.v3 also takes embedded unexported structs
			continue
		}
		name := strings.ToLower(f.Name)
		tag := f.Tag.Get("yaml")
		if tag == "-" {
			continue
		}
		n, flags, _ := strings.Cut(tag, ",")
		if inlineStruct(f, flags) {
			ft := f.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			for k, v := range yamlFields(ft) {
				out[k] = v
			}
			continue
		}
		if n != "" {
			name = n
		}
		out[name] = f.Type
	}
	return out
}

// inlineStruct reports whether field f is a struct that the tag flags say to
// inline into its parent.
func inlineStruct(f reflect.StructField, flags string) bool {
	if !slices.Contains(strings.Split(flags, ","), "inline") {
		return false
	}
	ft := f.Type
	for ft.Kind() == reflect.Pointer {
		ft = ft.Elem()
	}
	return ft.Kind() == reflect.Struct
}

// ---- secrets ----

// Secret is a config value that must not live in the file: a string
// (normally "${VAR}") or {file: path}. It prints as [secret] everywhere.
//
// Keep Secrets in exported fields of the config structs: unexported fields
// are not decoded, so the value would silently stay empty.
type Secret struct {
	value string
	file  string // set until resolved
	set   bool
}

const redacted = "[secret]"

// Reveal returns the secret's value.
func (s Secret) Reveal() string { return s.value }

// IsSet reports whether the field was given in the file.
func (s Secret) IsSet() bool { return s.set }

func (s Secret) String() string               { return redacted }
func (s Secret) GoString() string             { return redacted }
func (s Secret) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, redacted) }
func (s Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }
func (s Secret) MarshalText() ([]byte, error) { return []byte(redacted), nil }
func (s Secret) MarshalYAML() (any, error)    { return redacted, nil }

// UnmarshalYAML reads the string or {file: path} form. The value is checked
// (and a file read) after the whole file is decoded, where the key path and
// the config's directory are known.
//
// Errors name the key path and not a line: the node this method sees comes
// from the text confload re-encoded after expansion, whose line numbers are
// not the file's.
func (s *Secret) UnmarshalYAML(n *yaml.Node) error {
	*s = Secret{}
	switch n.Kind {
	case yaml.ScalarNode:
		s.value, s.set = n.Value, true
		return nil
	case yaml.MappingNode:
		if len(n.Content) == 2 && n.Content[0].Value == "file" && n.Content[1].Kind == yaml.ScalarNode {
			s.file, s.set = n.Content[1].Value, true
			return nil
		}
	}
	return Public(errors.New("a secret is a string or {file: path}"))
}

// resolveSecrets walks cfg, reads secret files and refuses empty secrets.
func resolveSecrets(v reflect.Value, path, dir string) error {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return resolveSecrets(v.Elem(), path, dir)
	case reflect.Struct:
		if v.Type() == secretType {
			return resolveOne(v.Addr().Interface().(*Secret), path, dir)
		}
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() && !f.Anonymous { // yaml.v3 also takes embedded unexported structs
				continue
			}
			name := strings.ToLower(f.Name)
			n, flags, _ := strings.Cut(f.Tag.Get("yaml"), ",")
			// An inlined struct's keys sit in this struct's own mapping, so it
			// keeps this struct's path.
			p := path
			if !inlineStruct(f, flags) {
				if n != "" && n != "-" {
					name = n
				}
				p = name
				if path != "" {
					p = path + "." + name
				}
			}
			if err := resolveSecrets(v.Field(i), p, dir); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if err := resolveSecrets(v.Index(i), fmt.Sprintf("%s[%d]", path, i), dir); err != nil {
				return err
			}
		}
	case reflect.Map:
		for _, k := range v.MapKeys() {
			// Map values are not addressable: resolve a copy and put it back.
			c := reflect.New(v.Type().Elem()).Elem()
			c.Set(v.MapIndex(k))
			if err := resolveSecrets(c, fmt.Sprintf("%s.%v", path, k), dir); err != nil {
				return err
			}
			v.SetMapIndex(k, c)
		}
	}
	return nil
}

func resolveOne(s *Secret, path, dir string) error {
	if !s.set {
		return nil
	}
	if s.file != "" {
		p := s.file
		if !filepath.IsAbs(p) {
			p = filepath.Join(dir, p)
		}
		f, err := os.Open(p)
		if err != nil {
			return fmt.Errorf("config key %q: cannot read the secret file %s", path, p)
		}
		defer f.Close()
		b, err := io.ReadAll(io.LimitReader(f, MaxSecretFile+1))
		if err != nil {
			return fmt.Errorf("config key %q: cannot read the secret file %s", path, p)
		}
		if len(b) > MaxSecretFile {
			return fmt.Errorf("config key %q: the secret file %s is over %d bytes", path, p, MaxSecretFile)
		}
		s.value = strings.TrimSuffix(string(b), "\n")
		s.file = ""
	}
	if s.value == "" {
		return fmt.Errorf("config key %q: the secret is empty", path)
	}
	return nil
}
