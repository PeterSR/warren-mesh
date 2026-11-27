package service

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/PeterSR/warren-mesh/internal/apierr"
)

// allocated is how many bytes f allocates in all.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// A body with far more entries than a PUT may hold is refused without
// building a list of them first.
func TestOversizedPutIsRefusedWithoutAllocatingItsEntries(t *testing.T) {
	body := append([]byte(`{"entries":[`), bytes.Repeat([]byte(`{},`), 199999)...)
	body = append(body, []byte(`{}]}`)...)
	var err *apierr.Error
	got := allocated(func() { _, err = parsePut(body, "n1") })
	if err == nil || err.Code != apierr.BadRequest || !strings.Contains(err.Detail, "entries") {
		t.Fatalf("want the entry count refused, got %v", err)
	}
	if got > 4<<20 {
		t.Errorf("refusing the body allocated %d bytes", got)
	}
	// Too many reserved names, and data over its size, the same way.
	body = []byte(`{"reserved":[` + strings.Repeat(`"a",`, 100000) + `"a"]}`)
	if _, err = parsePut(body, "n1"); err == nil || !strings.Contains(err.Detail, "reserved") {
		t.Errorf("reserved names: %v", err)
	}
	body = []byte(`{"entries":[{"name":"a","data":"` + strings.Repeat("x", MaxEntryData) + `"}]}`)
	if _, err = parsePut(body, "n1"); err == nil || !strings.Contains(err.Detail, "data is over") {
		t.Errorf("data: %v", err)
	}
}

// The list of a peer is read with its limits applied as it goes.
func TestOversizedPeerListIsCutWithoutAllocatingItsEntries(t *testing.T) {
	body := append([]byte(`{"node":"n1","version":"v","service":"echo","entries":[`), bytes.Repeat([]byte(`{},`), 199999)...)
	body = append(body, []byte(`{}]}`)...)
	var l *peerList
	var err error
	dropped := 0
	got := allocated(func() {
		l, err = decodePeerList(body, "n1", func(string, string) { dropped++ })
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Entries) != 0 || dropped != 200000 {
		t.Errorf("%d kept, %d dropped", len(l.Entries), dropped)
	}
	// Building the list of 200000 entries takes about 64 MiB. The bound is
	// loose because TotalAlloc also counts what other goroutines of the
	// package's earlier tests allocate meanwhile.
	if got > 32<<20 {
		t.Errorf("reading the list allocated %d bytes", got)
	}

	// More valid entries than a list may hold: the first MaxListEntries stay.
	var b bytes.Buffer
	b.WriteString(`{"node":"n1","version":"v","service":"echo","entries":[`)
	for i := 0; i < MaxListEntries+500; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"address":"e%d@n1","origin":"n1","data":null}`, i)
	}
	b.WriteString(`]}`)
	cut := 0
	l, err = decodePeerList(b.Bytes(), "n1", func(_, reason string) {
		if reason == "cut" {
			cut++
		}
	})
	if err != nil || len(l.Entries) != MaxListEntries || cut != 500 || !l.HasEntries {
		t.Errorf("%v: %d kept, %d cut", err, len(l.Entries), cut)
	}
}

// A repeated key counts once, the last time, and values for other
// namespaces are still checked to be lists of names.
func TestPeerListRepeatedKeysAndForeignReserved(t *testing.T) {
	drop := func(string, string) {}
	l, err := decodePeerList([]byte(`{"node":"n1","version":"v","service":"s","entries":[{"address":"a@n1","origin":"n1"}],"entries":[],"reserved":{"n1":["a"]},"reserved":{"n1":["b"]}}`), "n1", drop)
	if err != nil || len(l.Entries) != 0 || len(l.Reserved) != 1 || l.Reserved[0] != "b" {
		t.Errorf("%v %+v", err, l)
	}
	if _, err := decodePeerList([]byte(`{"node":"n1","version":"v","service":"s","entries":[],"reserved":{"other":5}}`), "n1", drop); err == nil {
		t.Error("a foreign reserved value that is not a list was accepted")
	}
	req, err := decodePut([]byte(`{"entries":[{"name":"a"}],"entries":[{"name":"b"}],"reserved":["x"],"reserved":["y"]}`))
	if err != nil || len(req.Entries) != 1 || req.Entries[0].Name != "b" || req.Reserved[0] != "y" {
		t.Errorf("%v %+v", err, req)
	}
}
