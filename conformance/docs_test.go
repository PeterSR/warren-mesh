package conformance_test

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/link"
	"github.com/PeterSR/warren-mesh/internal/service"
)

// TestServiceDocMatchesTheCode keeps the numbers of docs/service-api.md and
// docs/transport-api.md equal to the code's: the error table, the limits and
// the timers.
func TestServiceDocMatchesTheCode(t *testing.T) {
	b, err := os.ReadFile("../docs/service-api.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	row := regexp.MustCompile("(?m)^\\| `([a-z-]+)` \\| (\\d+) \\|")
	got := map[string]int{}
	for _, m := range row.FindAllStringSubmatch(doc, -1) {
		n, _ := strconv.Atoi(m[2])
		got[m[1]] = n
	}
	for _, code := range apierr.Codes() {
		if got[code] != apierr.Status(code) {
			t.Errorf("error table: %s is %d in the doc, %d in the code", code, got[code], apierr.Status(code))
		}
	}
	if len(got) != len(apierr.Codes()) {
		t.Errorf("the doc's error table has %d codes, the code %d", len(got), len(apierr.Codes()))
	}
	for _, want := range []string{
		fmt.Sprintf("**%d s (the lease grace)**", int(service.LeaseGrace.Seconds())),
		fmt.Sprintf("| lease grace | %d s |", int(service.LeaseGrace.Seconds())),
		fmt.Sprintf("| stale window | %d s |", int(service.StaleFor.Seconds())),
		fmt.Sprintf("| `timeout_ms` | 1 to %d, default %d |", service.MaxTimeoutMs, service.DefaultTimeoutMs),
		fmt.Sprintf("| view polls per socket | %d |", service.MaxViewPolls),
		fmt.Sprintf("data %d KiB per entry", service.MaxEntryData>>10),
		fmt.Sprintf("%d entries; %d reserved names", service.MaxPutEntries, service.MaxPutReserved),
		fmt.Sprintf("| `PUT` body | %d MiB;", service.MaxPutBody>>20),
		fmt.Sprintf("| call body, answer body | %d MiB each |", service.MaxCallBody>>20),
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("service-api.md lacks %q", want)
		}
	}
	tb, err := os.ReadFile("../docs/transport-api.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{fmt.Sprintf("(%d bytes)", link.MinMaxBody), fmt.Sprintf("at least **%d concurrent HTTP/2 streams**", link.MinStreams)} {
		if !strings.Contains(string(tb), want) {
			t.Errorf("transport-api.md lacks %q", want)
		}
	}
}
