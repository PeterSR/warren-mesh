// Package pinned checks the reason given with a `pinned:` setting, in Warren's
// config and in the updater's. The reason is free text that is shown in
// terminals and logs, so it is one short line of visible text.
package pinned

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxLen is the most bytes a reason may have.
const MaxLen = 200

// Check refuses a reason that is longer than MaxLen bytes, is not valid UTF-8,
// holds a control (Cc), format (Cf, such as bidirectional marks and zero width
// characters), line separator (Zl) or paragraph separator (Zp) character, or is
// only white space. The empty string is valid: it means not pinned.
func Check(s string) error {
	if len(s) > MaxLen {
		return fmt.Errorf("pinned is %d bytes; the most is %d", len(s), MaxLen)
	}
	if !utf8.ValidString(s) {
		return errors.New("pinned must be valid UTF-8")
	}
	for _, r := range s {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) {
			return errors.New("pinned must be one line of plain text")
		}
	}
	if s != "" && strings.TrimSpace(s) == "" {
		return errors.New("pinned must not be only white space")
	}
	return nil
}
