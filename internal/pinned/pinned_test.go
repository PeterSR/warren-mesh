package pinned

import (
	"strings"
	"testing"
)

func TestCheck(t *testing.T) {
	for _, ok := range []string{"", "docker image example:warren", "managed by apt", "café", strings.Repeat("x", MaxLen)} {
		if err := Check(ok); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for name, bad := range map[string]string{
		"too long": strings.Repeat("x", MaxLen+1), "invalid utf-8": "a\xffb", "newline": "a\nb", "tab": "a\tb", "escape": "a\x1bb",
		"right-to-left override": "a\u202eb", "bidi isolate": "a\u2066b", "zero width space": "a\u200bb",
		"line separator": "a\u2028b", "paragraph separator": "a\u2029b", "only spaces": "   ", "only an ideographic space": "　",
	} {
		if err := Check(bad); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
