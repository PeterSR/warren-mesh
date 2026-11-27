package conformance_test

import (
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/PeterSR/warren-mesh/conformance"
)

var (
	binary = flag.String("warren", "", "run the suite against this warren binary (it must support `warren dev`) instead of this repository's Warren")
	real   = flag.Bool("real", false, "run with the contract's real timers (a few minutes) instead of the fast ones")
)

// TestServiceConformance runs the whole /v1/service suite against the Warren
// of this repository, in this process, or against the binary named by
// -warren.
func TestServiceConformance(t *testing.T) {
	var l conformance.Launcher = conformance.InProcess{}
	if *binary != "" {
		l = conformance.Binary{Path: *binary}
	}
	conformance.Run(t, l, conformance.Options{RealTimers: *real})
}

// TestServiceConformanceBinary builds cmd/warren and runs the suite against
// the binary, the way a third party runs it with -warren.
func TestServiceConformanceBinary(t *testing.T) {
	if testing.Short() || *binary != "" {
		t.Skip("builds and runs the binary (skipped with -short, and when -warren is given)")
	}
	dir, err := os.MkdirTemp("", "wcbin")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	exe := filepath.Join(dir, "warren")
	if out, err := exec.Command("go", "build", "-o", exe, "github.com/PeterSR/warren-mesh/cmd/warren").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	conformance.Run(t, conformance.Binary{Path: exe}, conformance.Options{RealTimers: *real})
}
