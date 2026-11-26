package supervise

import (
	"testing"

	"github.com/PeterSR/warren-mesh/internal/platform"
)

func TestHarden(t *testing.T) {
	if err := Harden(); err != nil {
		t.Fatal(err)
	}
	defer platform.SetDumpable()
	if d, err := platform.Dumpable(); err != nil || d {
		t.Errorf("still dumpable: %v %v", d, err)
	}
}
