package service

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
)

type refusal struct{ Name, Namespace, Reason string }

func refused(t *testing.T, r resp) []refusal {
	t.Helper()
	expectOK(t, r)
	var out struct {
		Refused []refusal `json:"refused"`
	}
	r.json(t, &out)
	if out.Refused == nil {
		t.Fatalf("refused must be a list, got %s", r.body)
	}
	return out.Refused
}

func hasReason(rs []refusal, name, reason string) bool {
	for _, r := range rs {
		if r.Name == name && r.Reason == reason {
			return true
		}
	}
	return false
}

func TestPutAndView(t *testing.T) {
	e := newEnv(t, "")
	e.mkChild("dyn")
	kit, node, dyn := e.childSock("kit", "echo"), e.nodeSock("echo"), e.childSock("dyn", "echo")
	lk, _ := kit.serve(1, false)
	ln, _ := node.serve(1, false)
	ld, _ := dyn.serve(1, false)

	if rs := refused(t, kit.put(lk, `[{"name":"alice","data":{"role":"dev"}}]`, "")); len(rs) != 0 {
		t.Fatalf("refused %v", rs)
	}
	refused(t, node.put(ln, `[{"name":"bob","data":null},{"name":"carol","namespace":"chat.n1","data":[1,2]},{"name":"dave","namespace":"n1"}]`, ""))
	refused(t, dyn.put(ld, `[{"name":"eve","data":"s"}]`, ""))

	// Every socket sees the merged entries of the service.
	v := dyn.view()
	if !reflect.DeepEqual(v.addrs(), []string{"bob@n1", "carol@chat.n1", "dave@n1", "alice@kit.n1", "eve@dyn.n1"}) {
		t.Errorf("addresses %v", v.addrs())
	}
	for _, en := range v.Entries {
		if en.Stale {
			t.Errorf("%s stale", en.Address)
		}
		want := map[string]string{"bob@n1": "n1", "carol@chat.n1": "n1", "alice@kit.n1": "kit.n1", "eve@dyn.n1": "dyn.n1"}[en.Address]
		if want != "" && en.Origin != want {
			t.Errorf("%s origin %s", en.Address, en.Origin)
		}
	}
	if !v.Ready || v.Boot != e.node.Boot() || v.Version == "" || !strings.HasPrefix(v.Version, e.node.Boot()[:8]) {
		t.Errorf("view header %+v", v)
	}
	// Entries are per service: the other service sees none of them.
	if o := e.nodeSock("other").view(); len(o.Entries) != 0 {
		t.Errorf("entries leaked across services: %v", o.addrs())
	}
	// A PUT replaces the previous list.
	time.Sleep(2 * PutInterval)
	refused(t, kit.put(lk, `[{"name":"zed"}]`, ""))
	if v := dyn.view(); v.has("alice@kit.n1") || !v.has("zed@kit.n1") {
		t.Errorf("replace: %v", v.addrs())
	}
}

func TestPutValidation(t *testing.T) {
	e := newEnv(t, "", func() { LeaseGrace = 5 * time.Second })
	kit, node := e.childSock("kit", "echo"), e.nodeSock("echo")
	lk, _ := kit.serve(1, false)
	ln, _ := node.serve(1, false)
	next := func() { time.Sleep(2 * PutInterval) }

	rs := refused(t, kit.put(lk, `[{"name":"Bad Name"},{"name":"ok","namespace":"n1"},{"name":"ok2","namespace":"chat.n1"},{"name":"fine"}]`, ""))
	if !hasReason(rs, "Bad Name", "grammar") || !hasReason(rs, "ok", "namespace") || !hasReason(rs, "ok2", "namespace") || len(rs) != 3 {
		t.Errorf("refusals %v", rs)
	}
	if v := kit.view(); !v.has("fine@kit.n1") || len(v.Entries) != 1 {
		t.Errorf("view %v", v.addrs())
	}
	next()
	// Whole-PUT failures.
	expect(t, kit.put(lk, `[{"name":"a"},{"name":"a"}]`, ""), apierr.BadRequest)
	next()
	expect(t, kit.put(lk, `[{"name":"a","namespace":"kit.n1"},{"name":"a"}]`, ""), apierr.BadRequest) // same address twice
	next()
	big := make([]string, 1001)
	for i := range big {
		big[i] = fmt.Sprintf(`{"name":"e%d"}`, i)
	}
	expect(t, kit.put(lk, "["+strings.Join(big, ",")+"]", ""), apierr.BadRequest)
	next()
	expect(t, kit.put(lk, `[{"name":"a","data":"`+strings.Repeat("x", MaxEntryData)+`"}]`, ""), apierr.BadRequest)
	next()
	expect(t, kit.do("PUT", "/v1/service/entries?lease="+lk, []byte("not json")), apierr.BadRequest)
	next()
	// Only node services reserve: a child's reserved names are ignored and
	// reported as not-allowed.
	if rs := refused(t, kit.put(lk, "[]", `["x","y"]`)); len(rs) != 2 || !hasReason(rs, "x", "not-allowed") || !hasReason(rs, "y", "not-allowed") {
		t.Errorf("child reserved: %v", rs)
	}
	if v := kit.view(); len(v.Reserved) != 0 {
		t.Errorf("child reserved names took effect: %v", v.Reserved)
	}
	next()
	var many []string
	for i := 0; i < 65; i++ {
		many = append(many, fmt.Sprintf(`"r%d"`, i))
	}
	expect(t, node.put(ln, "[]", "["+strings.Join(many, ",")+"]"), apierr.BadRequest)
	next()
	expect(t, node.put(ln, "[]", `["Bad Name"]`), apierr.BadRequest)
}

func TestPutLeaseAndRateAreCheckedBeforeTheBody(t *testing.T) {
	e := newEnv(t, "", func() { PutInterval = 400 * time.Millisecond; LeaseGrace = 5 * time.Second })
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	// A bad lease is refused without reading the body (only headers sent).
	r := kit.headerOnlyM("PUT", "/v1/service/entries?lease=nope", 3<<20)
	expect(t, r, apierr.Gone)
	// Over 4 MiB: too-large, again before reading.
	expect(t, kit.headerOnlyM("PUT", "/v1/service/entries?lease="+l, MaxPutBody+1), apierr.TooLarge)
	time.Sleep(PutInterval + 50*time.Millisecond)
	refused(t, kit.put(l, "[]", ""))
	// One PUT per second per socket (here per PutInterval): more get busy,
	// before their body is read.
	expect(t, kit.headerOnlyM("PUT", "/v1/service/entries?lease="+l, 3<<20), apierr.Busy)
}

func TestOnePutBodyAtATime(t *testing.T) {
	e := newEnv(t, "", func() { PutInterval = 0 })
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	// The first PUT starts its body and stalls.
	conn, err := net.Dial("unix", kit.path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "PUT /v1/service/entries?lease=%s HTTP/1.1\r\nHost: x\r\nX-Warren: 1\r\nContent-Length: 100\r\n\r\n{", l)
	eventually(t, "the body read to start", func() bool {
		return e.node.sockState("kit.n1", "echo", func(s *sock) int { return b2i(s.putBusy) }) == 1
	})
	expect(t, kit.put(l, "[]", ""), apierr.Busy)
	conn.Close()
	// Once it is gone, PUTs work again.
	eventually(t, "put slot free", func() bool { return kit.put(l, "[]", "").status == 200 })
}

func TestPrecedenceAndReservations(t *testing.T) {
	e := newEnv(t, "")
	node, host, host2 := e.nodeSock("echo"), e.childSock("host", "echo"), e.childSock("host2", "echo")
	ln, _ := node.serve(1, false)
	lh, _ := host.serve(1, false)
	l2, _ := host2.serve(1, false)

	// Node first: it lists alice and reserves default.
	refused(t, node.put(ln, names("alice"), `["default","admin"]`))
	rs := refused(t, host.put(lh, names("alice", "bob", "default"), ""))
	if !hasReason(rs, "alice", "held-by-node") || !hasReason(rs, "default", "reserved") || len(rs) != 2 {
		t.Errorf("host refusals %v", rs)
	}
	rs = refused(t, host2.put(l2, names("bob", "carol", "admin"), ""))
	if !hasReason(rs, "bob", "held-by-earlier-child") || !hasReason(rs, "admin", "reserved") || len(rs) != 2 {
		t.Errorf("host2 refusals %v", rs)
	}
	v := host2.view()
	if got := v.addrs(); !reflect.DeepEqual(got, []string{"alice@n1", "bob@n1", "carol@n1"}) {
		t.Errorf("merged %v", got)
	}
	origin := map[string]string{}
	for _, en := range v.Entries {
		origin[en.Address] = en.Origin
	}
	if origin["alice@n1"] != "n1" || origin["bob@n1"] != "host.n1" || origin["carol@n1"] != "host2.n1" {
		t.Errorf("origins %v", origin)
	}
	if !reflect.DeepEqual(v.Reserved["n1"], []string{"default", "admin"}) {
		t.Errorf("reserved %v", v.Reserved)
	}
	// lost lists the socket's own displaced entries.
	if r := v.lostReason("bob"); r != "held-by-earlier-child" {
		t.Errorf("host2 lost bob: %q (%v)", r, v.Lost)
	}
	if hv := host.view(); hv.lostReason("alice") != "held-by-node" || hv.lostReason("default") != "reserved" {
		t.Errorf("host lost %v", hv.Lost)
	}

	// from follows the same rules.
	kit := e.childSock("kit", "echo")
	startEcho(t, kit)
	expect(t, host.call("kit.n1", "m", "", "&from=alice"), apierr.Refused)
	expect(t, host.call("kit.n1", "m", "", "&from=default"), apierr.Refused)
	expect(t, host2.call("kit.n1", "m", "", "&from=bob"), apierr.Refused)
	expectOK(t, host2.call("kit.n1", "m", "", "&from=carol"))
	// The node's own reservation is its own to use.
	expectOK(t, node.call("kit.n1", "m", "", "&from=default"))
	// A name nobody holds need not be listed to send from.
	expectOK(t, host.call("kit.n1", "m", "", "&from=newname"))

	// Displacement after the fact: host holds zed, then the node lists it.
	time.Sleep(2 * PutInterval)
	refused(t, host.put(lh, names("bob", "zed"), ""))
	if !host2.view().has("zed@n1") {
		t.Fatal("zed not listed")
	}
	time.Sleep(2 * PutInterval)
	refused(t, node.put(ln, names("alice", "zed"), `["default","admin"]`))
	v = host.view()
	if v.has("zed@n1") && v.Entries[0].Origin != "n1" {
		t.Errorf("zed still shown for host: %v", v.addrs())
	}
	if v.lostReason("zed") != "held-by-node" {
		t.Errorf("zed not lost: %v", v.Lost)
	}
	expect(t, host.call("kit.n1", "m", "", "&from=zed"), apierr.Refused)
	// Dropping the claim frees the name again, without another PUT by host.
	time.Sleep(2 * PutInterval)
	refused(t, node.put(ln, names("alice"), `["default","admin"]`))
	if v := host.view(); v.lostReason("zed") != "" || !v.has("zed@n1") {
		t.Errorf("zed not back: %v lost %v", v.addrs(), v.Lost)
	}
}

func TestChildNamesPatterns(t *testing.T) {
	y := strings.Replace(baseYAML, "    - name: kit\n", "    - name: kit\n      names: [\"agent-*\", fixed]\n", 1)
	e := newEnv(t, y)
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	rs := refused(t, kit.put(l, names("agent-1", "fixed", "other"), ""))
	if len(rs) != 1 || !hasReason(rs, "other", "not-allowed") {
		t.Errorf("refusals %v", rs)
	}
	expect(t, kit.call("kit.n1", "m", "", "&from=other"), apierr.Refused) // names patterns limit from too
	expect(t, kit.call("kit.n1", "m", "", "&from=agent-9"), apierr.NoService)
}

func TestNodeListOrderAndCutting(t *testing.T) {
	// Precedence order of the node's list: node services, flat children in
	// config order, other configured children in config order, template
	// children sorted by label. Cut from the tail.
	y := strings.Replace(baseYAML, "  children:\n    - name: host\n      flat: true\n",
		"  children:\n    - name: host\n      flat: true\n      max_entries: 2\n", 1)
	e := newEnv(t, y)
	e.mkChild("zz")
	e.mkChild("aa")
	socks := map[string]*cl{
		"n1": e.nodeSock("echo"), "host": e.childSock("host", "echo"), "host2": e.childSock("host2", "echo"),
		"kit": e.childSock("kit", "echo"), "zz": e.childSock("zz", "echo"), "aa": e.childSock("aa", "echo"),
	}
	leases := map[string]string{}
	// Publish in the opposite of the expected order.
	for _, k := range []string{"zz", "aa", "kit", "host2", "host", "n1"} {
		leases[k], _ = socks[k].serve(1, false)
	}
	pub := func(k string, ns ...string) []refusal {
		return refused(t, socks[k].put(leases[k], names(ns...), ""))
	}
	pub("zz", "z1")
	pub("aa", "a1")
	pub("kit", "k1")
	pub("host2", "h2a")
	// host has max_entries 2: its third entry is cut.
	rs := pub("host", "h1", "h2", "h3")
	if len(rs) != 1 || !hasReason(rs, "h3", "cut") {
		t.Errorf("host refusals %v", rs)
	}
	pub("n1", "n1a")
	want := []string{"n1a@n1", "h1@n1", "h2@n1", "h2a@n1", "k1@kit.n1", "a1@aa.n1", "z1@zz.n1"}
	if got := e.node.NodeList("echo"); !reflect.DeepEqual(got, want) {
		t.Errorf("node list\n got %v\nwant %v", got, want)
	}

	// Now a global cut: only the first 4 survive, from the tail.
	e.node.mu.Lock()
	e.node.maxList = 4
	e.node.rebuildAll()
	e.node.mu.Unlock()
	want = want[:4]
	if got := e.node.NodeList("echo"); !reflect.DeepEqual(got, want) {
		t.Errorf("cut list\n got %v\nwant %v", got, want)
	}
	// What was cut is reported as lost, to its own socket, and can still send.
	for k, name := range map[string]string{"kit": "k1", "aa": "a1", "zz": "z1"} {
		if r := socks[k].view().lostReason(name); r != "cut" {
			t.Errorf("%s: %s lost reason %q", k, name, r)
		}
	}
	startEcho(t, e.childSock("host", "echo")) // a target
	expectOK(t, socks["zz"].call("host.n1", "m", "", "&from=z1@zz.n1"))
}

func TestStaleEntries(t *testing.T) {
	e := newEnv(t, "")
	kit, host := e.childSock("kit", "echo"), e.childSock("host", "echo")
	l, _ := kit.serve(1, false)
	refused(t, kit.put(l, names("alice"), ""))
	lh, _ := host.serve(1, false)
	_ = lh

	if v := host.view(); v.Entries[0].Stale {
		t.Fatal("stale while the lease lives")
	}
	kit.post("/v1/service/release?lease="+l, "")
	v := host.view()
	if len(v.Entries) != 1 || !v.Entries[0].Stale {
		t.Fatalf("after release: %+v", v.Entries)
	}
	// A new lease starts with no published list of its own: the names stay
	// stale until it PUTs, and the PUT revives them.
	l2, _ := kit.serve(1, false)
	if v := host.view(); len(v.Entries) != 1 || !v.Entries[0].Stale {
		t.Fatalf("new lease before its PUT: %+v", v.Entries)
	}
	time.Sleep(2 * PutInterval)
	refused(t, kit.put(l2, names("alice"), ""))
	if v := host.view(); len(v.Entries) != 1 || v.Entries[0].Stale {
		t.Fatalf("after the PUT: %+v", v.Entries)
	}
	// Calls to a stale entry fail with no-service.
	kit.post("/v1/service/release?lease="+l2, "")
	expect(t, host.call("alice@kit.n1", "m", "", ""), apierr.NoService)
	// And after StaleFor they are dropped.
	eventually(t, "entry dropped", func() bool { return len(host.view().Entries) == 0 })
	// The target's lease check comes before the entry check.
	expect(t, host.call("alice@kit.n1", "m", "", ""), apierr.NoService)
}

// withRun makes the node's echo a managed service, which is what the boot
// hold waits for.
func withRun(y string) string {
	return strings.Replace(y, "    - name: echo\n      allow: [\"*\"]\n      namespaces: [chat]", "    - name: echo\n      allow: [\"*\"]\n      namespaces: [chat]\n      run: [/bin/true]", 1)
}

// The scan loop writes names.json again after a failure, without waiting for
// another PUT.
func TestFailedNamesWriteIsRetriedByTheScan(t *testing.T) {
	e := newEnv(t, withRun(baseYAML), func() { LeaseGrace = 5 * time.Second })
	tmp := filepath.Join(e.cfg.StateDir, "names.json.tmp")
	if err := os.Mkdir(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	node := e.nodeSock("echo")
	ln, _ := node.serve(1, false)
	refused(t, node.put(ln, names("alice"), ""))
	e.node.flushNames()
	os.Remove(tmp)
	eventually(t, "the scan to write names.json", func() bool {
		e.node.flushNames()
		data, err := os.ReadFile(filepath.Join(e.cfg.StateDir, "names.json"))
		return err == nil && strings.Contains(string(data), "alice")
	})
}

func TestPersistedNamesAfterRestart(t *testing.T) {
	e := newEnv(t, withRun(baseYAML), func() { LeaseGrace = 5 * time.Second })
	node := e.nodeSock("echo")
	ln, _ := node.serve(1, false)
	refused(t, node.put(ln, names("alice"), `["default"]`))
	e.node.flushNames()
	data, err := os.ReadFile(filepath.Join(e.cfg.StateDir, "names.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Version  int
		Services map[string]struct{ Names, Reserved []string }
	}
	json.Unmarshal(data, &f)
	if f.Version != 1 || !reflect.DeepEqual(f.Services["echo"].Names, []string{"alice"}) || !reflect.DeepEqual(f.Services["echo"].Reserved, []string{"default"}) {
		t.Fatalf("names.json: %s", data)
	}
	if fi, _ := os.Stat(filepath.Join(e.cfg.StateDir, "names.json")); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v", fi.Mode().Perm())
	}

	// Restart. A flat child publishes before the node service has: its PUT
	// is held, and the names are still the node's.
	e.restart()
	host := e.childSock("host", "echo")
	lh, _ := host.serve(1, false)
	if host.view().Ready {
		t.Fatal("ready before the node service published")
	}
	type out struct {
		rs  []refusal
		dur time.Duration
	}
	done := make(chan out, 1)
	start := time.Now()
	go func() {
		rs := refused(t, host.put(lh, names("alice", "default", "bob"), ""))
		done <- out{rs, time.Since(start)}
	}()
	select {
	case <-done:
		t.Fatal("the flat child's PUT was not held")
	case <-time.After(100 * time.Millisecond):
	}
	// The node service publishes again, a new list: alice stays, default gone.
	node = e.nodeSock("echo")
	ln, _ = node.serve(1, false)
	refused(t, node.put(ln, names("alice"), "[]"))
	select {
	case o := <-done:
		if !hasReason(o.rs, "alice", "held-by-node") || hasReason(o.rs, "default", "reserved") || hasReason(o.rs, "bob", "held-by-node") {
			t.Errorf("refusals %v", o.rs)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("held PUT never released")
	}
	if !host.view().Ready {
		t.Error("not ready after publishing")
	}
}

func TestHeldPutGivesUpWaitingAfterBootHold(t *testing.T) {
	e := newEnv(t, withRun(baseYAML), func() { LeaseGrace = 5 * time.Second })
	node := e.nodeSock("echo")
	ln, _ := node.serve(1, false)
	refused(t, node.put(ln, names("alice"), `["default"]`))
	e.restart()
	host := e.childSock("host", "echo")
	lh, _ := host.serve(1, false)
	start := time.Now()
	// The node service never comes back: the PUT waits out BootHold, and
	// the persisted names still hold.
	rs := refused(t, host.put(lh, names("alice", "default", "bob"), ""))
	if d := time.Since(start); d < BootHold-time.Since(e.node.bootAt)-60*time.Millisecond || d > BootHold+2*time.Second {
		t.Errorf("held for %v", d)
	}
	if !hasReason(rs, "alice", "held-by-node") || !hasReason(rs, "default", "reserved") {
		t.Errorf("persisted names not honored: %v", rs)
	}
	if !host.view().Ready {
		t.Error("not ready after the hold")
	}
}

func TestPersistedNamesInOtherFormatAreDiscarded(t *testing.T) {
	e := newEnv(t, "")
	e.node.Close()
	os.WriteFile(filepath.Join(e.cfg.StateDir, "names.json"), []byte(`{"version":99,"services":{"echo":{"names":["alice"]}}}`), 0o600)
	e.restart()
	if names := e.node.persisted["echo"].Names; len(names) != 0 {
		t.Errorf("kept names from another format: %v", names)
	}
	os.WriteFile(filepath.Join(e.cfg.StateDir, "names.json"), []byte(`garbage`), 0o600)
	e.restart()
	if len(e.node.persisted) != 0 {
		t.Error("kept garbage")
	}
}

func TestViewPolling(t *testing.T) {
	e := newEnv(t, "", func() { LeaseGrace = 5 * time.Second })
	kit, host := e.childSock("kit", "echo"), e.childSock("host", "echo")
	l, _ := kit.serve(1, false)
	eventually(t, "ready", func() bool { return host.view().Ready }) // the boot hold's end bumps the version
	v0 := host.view()
	// since == current: waits for a change.
	got := make(chan view, 1)
	go func() {
		r := host.get("/v1/service/view?wait=25&since=" + v0.Version)
		var v view
		r.json(t, &v)
		got <- v
	}()
	select {
	case <-got:
		t.Fatal("answered without a change")
	case <-time.After(100 * time.Millisecond):
	}
	refused(t, kit.put(l, names("alice"), ""))
	select {
	case v := <-got:
		if !v.has("alice@kit.n1") || v.Version == v0.Version {
			t.Errorf("view %+v", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("poll not woken")
	}
	// A timeout returns the same version.
	cur := host.view()
	start := time.Now()
	r := host.get("/v1/service/view?wait=1&since=" + cur.Version)
	var again view
	r.json(t, &again)
	if again.Version != cur.Version || time.Since(start) < 80*time.Millisecond {
		t.Errorf("timeout poll: %v after %v", again.Version, time.Since(start))
	}
	// since from another boot, or empty, answers at once.
	start = time.Now()
	host.get("/v1/service/view?wait=25&since=otherboot.1")
	host.get("/v1/service/view?wait=25")
	if time.Since(start) > time.Second {
		t.Error("view with a foreign since waited")
	}

	// At most 4 polls per socket.
	for i := 0; i < 4; i++ {
		go host.get("/v1/service/view?wait=25&since=" + cur.Version)
	}
	eventually(t, "four view polls to be open", func() bool {
		return e.node.sockState("host.n1", "echo", func(s *sock) int { return s.viewPolls }) == 4
	})
	expect(t, host.get("/v1/service/view?wait=25&since="+cur.Version), apierr.Busy)
	expect(t, host.get("/v1/service/view?wait=99"), apierr.BadRequest)
}

func TestViewSerializedOncePerVersion(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	refused(t, kit.put(l, names("alice", "bob"), ""))
	e.node.mu.Lock()
	svc := e.node.svc("echo")
	a := &svc.sharedEntries[0]
	e.node.mu.Unlock()
	for i := 0; i < 3; i++ {
		kit.view()
	}
	e.node.mu.Lock()
	b := &svc.sharedEntries[0]
	e.node.mu.Unlock()
	if a != b {
		t.Error("the serialized entries were rebuilt without a change")
	}
	// An identical PUT does not bump the version.
	v1 := kit.view().Version
	time.Sleep(2 * PutInterval)
	refused(t, kit.put(l, names("alice", "bob"), ""))
	if v2 := kit.view().Version; v1 != v2 {
		t.Errorf("version bumped by an identical PUT: %s -> %s", v1, v2)
	}
}

func TestNoHoldWithoutARunService(t *testing.T) {
	// A node service that is not managed (no run:) is not waited for.
	e := newEnv(t, "")
	host := e.childSock("host", "echo")
	l, _ := host.serve(1, false)
	start := time.Now()
	refused(t, host.put(l, names("a"), ""))
	if time.Since(start) > BootHold/2 || !host.view().Ready {
		t.Errorf("PUT held for %v", time.Since(start))
	}
}

func TestEntriesDroppedAfterStaleWindowCanBeRevivedOnlyByAPutWithinIt(t *testing.T) {
	e := newEnv(t, "")
	kit, host := e.childSock("kit", "echo"), e.childSock("host", "echo")
	l, _ := kit.serve(1, false)
	refused(t, kit.put(l, names("alice"), ""))
	kit.post("/v1/service/release?lease="+l, "")
	eventually(t, "dropped", func() bool { return len(host.view().Entries) == 0 })
	// A new lease after the window does not bring the old entries back.
	l2, _ := kit.serve(1, false)
	if v := host.view(); len(v.Entries) != 0 {
		t.Errorf("old entries came back: %v", v.addrs())
	}
	refused(t, kit.put(l2, names("alice"), ""))
	if !host.view().has("alice@kit.n1") {
		t.Error("PUT after the window did not publish")
	}
}

func TestNamesFileIsNotRewrittenByScans(t *testing.T) {
	e := newEnv(t, "")
	p := filepath.Join(e.cfg.StateDir, "names.json")
	if _, err := os.Stat(p); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := os.Chtimes(p, old, old); err != nil {
		t.Fatal(err)
	}
	time.Sleep(6 * ScanEvery)
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if !fi.ModTime().Equal(old) {
		t.Errorf("names.json was rewritten by scans: %v", fi.ModTime())
	}
	// A new child directory is a change in sockets, still no change in names.
	e.mkChild("fresh")
	time.Sleep(6 * ScanEvery)
	fi, _ = os.Stat(p)
	if !fi.ModTime().Equal(old) {
		t.Errorf("names.json was rewritten for an unchanged name set: %v", fi.ModTime())
	}
}

func TestViewIsWrittenFromTheSharedBytes(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	refused(t, kit.put(l, names("alice"), ""))
	var want view
	kit.get("/v1/service/view?wait=0").json(t, &want)
	if !want.has("alice."+"kit.n1") && len(want.addrs()) == 0 {
		t.Fatalf("view %+v", want)
	}
	e.node.mu.Lock()
	svc := e.node.svc("echo")
	parts := e.node.viewParts(svc, e.node.socks[sockKey{"kit.n1", "echo"}])
	same := &parts.entries[0] == &svc.sharedEntries[0]
	e.node.mu.Unlock()
	if !same {
		t.Error("the view copied the shared entries")
	}
}

func TestServeLoopWithoutPutDoesNotKeepOldEntriesAlive(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	refused(t, kit.put(l, names("alice"), ""))
	node := e.nodeSock("echo")
	// Each takeover ends a lease that published nothing, but the first one
	// ends the lease that did.
	end := time.Now().Add(StaleFor + 300*time.Millisecond)
	for time.Now().Before(end) {
		kit.serve(1, true)
		time.Sleep(StaleFor / 10)
	}
	if node.view().has("alice@kit.n1") {
		t.Error("old entries outlived StaleFor because of a serve loop")
	}
}

func TestReloadRemovingANamespaceDropsItsStoredEntries(t *testing.T) {
	e := newEnv(t, "")
	node := e.nodeSock("echo")
	l, _ := node.serve(1, false)
	refused(t, node.put(l, `[{"name":"x","namespace":"chat.n1","data":null}]`, ""))
	if !node.view().has("x@chat.n1") {
		t.Fatal("setup")
	}
	y := strings.Replace(e.yaml, "      namespaces: [chat]\n", "", 1)
	if err := e.node.Reload(parseYAML(t, e.dir, y)); err != nil {
		t.Fatal(err)
	}
	if v := node.view(); v.has("x@chat.n1") || v.lostReason("x") != reasonNamespace {
		t.Errorf("the node's entry in the removed namespace stays: %v lost=%v", v.addrs(), v.Lost)
	}
	// A template child now has the label; it publishes x in its own namespace.
	e.mkChild("chat")
	chat := e.childSock("chat", "echo")
	lc, _ := chat.serve(1, false)
	refused(t, chat.put(lc, names("x"), ""))
	n := 0
	for _, a := range node.view().addrs() {
		if a == "x@chat.n1" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("x@chat.n1 listed %d times, want exactly the child's", n)
	}
}

func TestFailedNamesWriteIsRetried(t *testing.T) {
	e := newEnv(t, withRun(baseYAML), func() { LeaseGrace = 5 * time.Second })
	if err := os.Chmod(e.cfg.StateDir, 0o500); err != nil { // the temporary file cannot be made
		t.Fatal(err)
	}
	defer os.Chmod(e.cfg.StateDir, 0o700)
	node := e.nodeSock("echo")
	ln, _ := node.serve(1, false)
	refused(t, node.put(ln, names("alice"), ""))
	e.node.flushNames()
	if data, _ := os.ReadFile(filepath.Join(e.cfg.StateDir, "names.json")); strings.Contains(string(data), "alice") {
		t.Fatal("names.json written though the write must fail")
	}
	if err := os.Chmod(e.cfg.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * PutInterval)
	// The same names again: without the fix this is skipped as unchanged.
	refused(t, node.put(ln, names("alice"), ""))
	e.node.flushNames()
	if data, err := os.ReadFile(filepath.Join(e.cfg.StateDir, "names.json")); err != nil || !strings.Contains(string(data), "alice") {
		t.Errorf("the snapshot was not retried: %s %v", data, err)
	}
}

// A body that stalls is answered bad-request when the read deadline passes,
// and does not hang the request for ever.
func TestStalledBodyIsABadRequest(t *testing.T) {
	e := newEnv(t, "", func() {
		BodyReadTimeout = 300 * time.Millisecond
		t.Cleanup(func() { BodyReadTimeout = 30 * time.Second })
	})
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	conn, err := net.Dial("unix", kit.path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "PUT /v1/service/entries?lease=%s HTTP/1.1\r\nHost: x\r\nX-Warren: 1\r\nContent-Length: 1000\r\n\r\n0123456789", l)
	conn.SetReadDeadline(time.Now().Add(BodyReadTimeout + 3*time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no answer to a stalled body: %v", err)
	}
	if resp.StatusCode != 400 || resp.Header.Get("X-Warren-Error") != apierr.BadRequest {
		t.Errorf("a stalled body: %d %v", resp.StatusCode, resp.Header)
	}
}
