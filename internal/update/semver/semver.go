// Package semver parses and orders Semantic Versions 2.0 without build
// metadata, which is all a release manifest may carry.
package semver

import (
	"errors"
	"fmt"
	"strings"
)

// MaxLen is the longest version string accepted.
const MaxLen = 64

// Version is a parsed version.
type Version struct {
	Major, Minor, Patch uint64
	Pre                 []string // pre-release identifiers
}

// Parse reads MAJOR.MINOR.PATCH[-PRERELEASE]. Build metadata (+...), a leading
// "v", leading zeros in numbers and empty identifiers are refused.
func Parse(s string) (Version, error) {
	var v Version
	if s == "" || len(s) > MaxLen {
		return v, errors.New("version must be 1 to 64 bytes")
	}
	if strings.Contains(s, "+") {
		return v, errors.New("version must not carry build metadata")
	}
	core, pre, hasPre := strings.Cut(s, "-")
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return v, errors.New("version must be MAJOR.MINOR.PATCH")
	}
	nums := [3]*uint64{&v.Major, &v.Minor, &v.Patch}
	for i, p := range parts {
		n, err := parseNum(p)
		if err != nil {
			return Version{}, err
		}
		*nums[i] = n
	}
	if hasPre {
		for _, id := range strings.Split(pre, ".") {
			if id == "" {
				return Version{}, errors.New("empty pre-release identifier")
			}
			for _, c := range id {
				if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '-') {
					return Version{}, fmt.Errorf("invalid character %q in pre-release", c)
				}
			}
			if isNum(id) && len(id) > 1 && id[0] == '0' {
				return Version{}, errors.New("numeric pre-release identifier with a leading zero")
			}
			v.Pre = append(v.Pre, id)
		}
	}
	return v, nil
}

func isNum(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return s != ""
}

func parseNum(s string) (uint64, error) {
	if !isNum(s) {
		return 0, fmt.Errorf("%q is not a number", s)
	}
	if len(s) > 1 && s[0] == '0' {
		return 0, fmt.Errorf("%q has a leading zero", s)
	}
	if len(s) > 9 {
		return 0, fmt.Errorf("%q is too large", s)
	}
	var n uint64
	for _, c := range s {
		n = n*10 + uint64(c-'0')
	}
	return n, nil
}

// IsPre reports whether the version is a pre-release.
func (v Version) IsPre() bool { return len(v.Pre) > 0 }

// Compare orders by semver precedence: -1, 0 or 1.
func Compare(a, b Version) int {
	for _, p := range [][2]uint64{{a.Major, b.Major}, {a.Minor, b.Minor}, {a.Patch, b.Patch}} {
		if p[0] != p[1] {
			if p[0] < p[1] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a.Pre) == 0 && len(b.Pre) == 0:
		return 0
	case len(a.Pre) == 0:
		return 1 // a release is above its pre-releases
	case len(b.Pre) == 0:
		return -1
	}
	for i := 0; i < len(a.Pre) && i < len(b.Pre); i++ {
		if c := compareID(a.Pre[i], b.Pre[i]); c != 0 {
			return c
		}
	}
	switch {
	case len(a.Pre) < len(b.Pre):
		return -1
	case len(a.Pre) > len(b.Pre):
		return 1
	}
	return 0
}

func compareID(a, b string) int {
	an, bn := isNum(a), isNum(b)
	switch {
	case an && bn:
		// No leading zeros, so a longer number is the larger one.
		if len(a) != len(b) {
			if len(a) < len(b) {
				return -1
			}
			return 1
		}
		return strings.Compare(a, b)
	case an:
		return -1 // numeric identifiers sort below alphanumeric ones
	case bn:
		return 1
	}
	return strings.Compare(a, b)
}

// CompareStrings parses both and compares them.
func CompareStrings(a, b string) (int, error) {
	va, err := Parse(a)
	if err != nil {
		return 0, err
	}
	vb, err := Parse(b)
	if err != nil {
		return 0, err
	}
	return Compare(va, vb), nil
}
