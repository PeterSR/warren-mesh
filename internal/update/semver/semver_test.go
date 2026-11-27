package semver

import "testing"

func TestParseRefuses(t *testing.T) {
	for _, s := range []string{"", "1", "1.2", "1.2.3.4", "v1.2.3", "01.2.3", "1.02.3", "1.2.3+b", "1.2.3-", "1.2.3-a..b", "1.2.3-01", "1.2.3-a_b", "1.2.x", "1.2.3-é", "9999999999.0.0"} {
		if _, err := Parse(s); err == nil {
			t.Errorf("%q accepted", s)
		}
	}
	long := "1.0.0-"
	for len(long) <= MaxLen {
		long += "a"
	}
	if _, err := Parse(long); err == nil {
		t.Error("overlong version accepted")
	}
}

func TestPrecedence(t *testing.T) {
	// The ordering of the semver 2.0 specification, item 11.
	ordered := []string{"1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-alpha.beta", "1.0.0-beta", "1.0.0-beta.2", "1.0.0-beta.11", "1.0.0-rc.1", "1.0.0", "1.0.1", "1.1.0", "2.0.0"}
	for i := range ordered {
		for j := range ordered {
			got, err := CompareStrings(ordered[i], ordered[j])
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if i < j {
				want = -1
			} else if i > j {
				want = 1
			}
			if got != want {
				t.Errorf("Compare(%s, %s) = %d, want %d", ordered[i], ordered[j], got, want)
			}
		}
	}
	if v, _ := Parse("1.4.0-dev.7"); !v.IsPre() {
		t.Error("a dev version is not a pre-release")
	}
	if v, _ := Parse("1.4.0"); v.IsPre() {
		t.Error("a release is a pre-release")
	}
	if c, _ := CompareStrings("1.0.0-dev.9", "1.0.0-dev.10"); c != -1 {
		t.Error("numeric identifiers must compare as numbers")
	}
}
