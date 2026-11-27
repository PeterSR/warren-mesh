package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/link"
)

func TestMergedViewAcrossTwoNodes(t *testing.T) {
	net := link.NewNet()
	a, b := meshNode(t, net, "a", "", nil), meshNode(t, net, "b", "", nil)
	both := []*mn{a, b}
	waitUp(t, both, "a", "b")
	ca, cb, kb := a.nodeSock("echo"), b.nodeSock("echo"), b.childSock("kit", "echo")
	publish(t, ca, "alice")
	lb, _ := cb.serve(1, true)
	refused(t, cb.put(lb, names("bob"), `["default","admin"]`))
	publish(t, kb, "carl")

	waitAddr(t, ca, "bob@b", true)
	v := waitAddr(t, ca, "carl@kit.b", true)
	if o, stale, _ := v.entry("bob@b"); o != "b" || stale {
		t.Errorf("bob@b: origin %q stale %v", o, stale)
	}
	if o, stale, _ := v.entry("carl@kit.b"); o != "kit.b" || stale {
		t.Errorf("carl@kit.b: origin %q stale %v", o, stale)
	}
	// This node's entries come first, then the other node's in its own order.
	if got := v.addrs(); got[0] != "alice@a" {
		t.Errorf("own entries are not first: %v", got)
	}
	if got := v.Reserved["b"]; !slices.Equal(got, []string{"default", "admin"}) {
		t.Errorf("reserved of b: %v", v.Reserved)
	}
	if !waitAddr(t, cb, "alice@a", true).has("bob@b") {
		t.Error("b's view lacks its own entry")
	}
	// A change on b reaches a, and so does a removal.
	refused(t, cb.put(lb, names("bob2"), ""))
	waitAddr(t, ca, "bob2@b", true)
	waitAddr(t, ca, "bob@b", false)
	// Another service is a separate list: nobody published it.
	if v := a.nodeSock("other").view(); len(v.Entries) != 0 {
		t.Errorf("other service: %v", v.addrs())
	}
}

func TestCallsAcrossNodes(t *testing.T) {
	net := link.NewNet()
	a, b := meshNode(t, net, "a", "", nil), meshNode(t, net, "b", "", nil)
	waitUp(t, []*mn{a, b}, "a", "b")
	echoB := startEcho(t, b.nodeSock("echo"))
	refused(t, b.nodeSock("echo").put(echoB.lease, names("alice"), ""))
	kit := a.childSock("kit", "echo")
	waitAddr(t, a.nodeSock("echo"), "alice@b", true)

	r := kit.call("alice@b", "hi", "payload", "&from=bob")
	expectOK(t, r)
	if string(r.body) != "hi:payload" {
		t.Fatalf("answer %q", r.body)
	}
	h := echoB.last()
	if h.Get("X-Warren-Origin") != "kit.a" || h.Get("X-Warren-From") != "bob@kit.a" || h.Get("X-Warren-To") != "alice@b" || h.Get("X-Warren-Method") != "hi" {
		t.Errorf("the service saw %v", h)
	}
	// By relay id, and the node itself.
	expectOK(t, kit.call("b", "m", "", ""))
	// A node never reaches into a child of another node that has no such service.
	expect(t, kit.call("host.b", "m", "", ""), apierr.NoService)
	// Codes of the target come back as they are.
	expect(t, kit.post("/v1/service/call?to=b&method=m&version=7", ""), apierr.Version)
	// A service that has no lease at the target.
	expect(t, a.nodeSock("other").call("b", "m", "", ""), apierr.NoService)
}

func TestOutgoingCallErrors(t *testing.T) {
	net := link.NewNet()
	var fc *failCalls
	a := meshNode(t, net, "a", "", func(o *Options) { fc = &failCalls{Link: o.Link}; o.Link = fc })
	b := meshNode(t, net, "b", "", nil)
	waitUp(t, []*mn{a, b}, "b")
	waitUp(t, []*mn{b}, "a")
	cb := b.nodeSock("echo")
	lb, _ := cb.serve(1, true) // a lease with no poll: calls are held
	_ = lb
	kit := a.childSock("kit", "echo")

	// Not sent: the call never left, so it is offline.
	fc.set(link.NotSent)
	expect(t, kit.call("b", "m", "", ""), apierr.Offline)
	// Lost: sent, outcome unknown.
	fc.set(link.Lost)
	expect(t, kit.call("b", "m", "", ""), apierr.Unknown)
	// An answer too big to carry back: the call was sent, so unknown, not
	// too-large (that code is for a body the caller sent).
	fc.set("too-big")
	expect(t, kit.call("b", "m", "", ""), apierr.Unknown)
	fc.set("")
	// The target's own timeout is passed on.
	expect(t, kit.call("b", "m", "", "&timeout_ms=100"), apierr.Timeout)
	// A node that is not on the network is no target; a node that left is offline.
	expect(t, kit.call("x.nowhere", "m", "", ""), apierr.NoTarget)
	net.SetDown("b", true)
	eventually(t, "b to go", func() bool { return kit.call("b", "m", "", "").code() == apierr.Offline })
}

// failCalls makes carrying a call fail the way a test says.
type failCalls struct {
	link.Link
	mu   sync.Mutex
	kind string
}

func (f *failCalls) set(kind string) { f.mu.Lock(); f.kind = kind; f.mu.Unlock() }

func (f *failCalls) Carry(ctx context.Context, to string, r *link.Request) (*link.Response, error) {
	f.mu.Lock()
	kind := f.kind
	f.mu.Unlock()
	if kind == "too-big" && strings.Contains(r.URI, "/call?") {
		return nil, &link.CarryError{Kind: link.Lost, Detail: "injected", TooBig: true}
	}
	if kind != "" && strings.Contains(r.URI, "/call?") {
		return nil, &link.CarryError{Kind: kind, Detail: "injected"}
	}
	return f.Link.Carry(ctx, to, r)
}

func TestCarryDeadlineIsTimeout(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	x.mu.Lock()
	x.onCall = func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }
	x.mu.Unlock()
	eventually(t, "x up", func() bool { return a.peerUp("x") })
	kit := a.childSock("kit", "echo")
	start := time.Now()
	expect(t, kit.call("x", "m", "", "&timeout_ms=200"), apierr.Timeout)
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("took %v", d)
	}
}

func TestIncomingCallsAreVouchedFor(t *testing.T) {
	net := link.NewNet()
	b := meshNode(t, net, "b", "", nil)
	evil := net.Join("evil")
	startEcho(t, b.nodeSock("echo"))
	call := func(q string, hdr map[string]string) string {
		r, err := raw(t, evil, "b", "POST", "/v1/peer/call?service=echo&version=1&method=m&"+q, hdr, "x")
		return rawCode(t, r, err)
	}
	// Origins of other nodes, and from names outside evil's namespace.
	for _, q := range []string{
		"to=b&origin=kit.c",
		"to=b&origin=c",
		"to=b&origin=b",
		"to=b&origin=evil&from=alice@b",
		"to=b&origin=evil&from=alice@kit.c",
		"to=b&origin=kit.evil&from=x@host.evil",
		"to=b&origin=kit.evil&from=x@chat.evil",
		"to=b&origin=a.b.evil",
	} {
		if c := call(q, nil); c != apierr.Refused && c != apierr.BadRequest {
			t.Errorf("%s: answered %q, want a refusal", q, c)
		}
	}
	// What evil may say passes vouching and is answered by the target.
	for _, q := range []string{"to=b&origin=evil", "to=b&origin=kit.evil&from=x@kit.evil", "to=b&origin=kit.evil&from=x@evil", "to=b&origin=evil&from=x@chat.evil"} {
		r, err := raw(t, evil, "b", "POST", "/v1/peer/call?service=echo&version=1&method=m&"+q, nil, "x")
		if err != nil || r.Status != 200 {
			t.Errorf("%s: %v %v", q, r, err)
		}
	}
	// B never passes a call on to a third node.
	if c := call("to=x@c&origin=evil", nil); c != apierr.NoTarget {
		t.Errorf("to a third node: %q", c)
	}
	if c := call("to=c&origin=evil", nil); c != apierr.NoTarget {
		t.Errorf("to a third node: %q", c)
	}
}

func TestPeerProtocolRefusals(t *testing.T) {
	net := link.NewNet()
	_ = meshNode(t, net, "b", "", nil)
	evil := net.Join("evil")
	get := func(uri string, hdr map[string]string) *link.Response {
		r, err := raw(t, evil, "b", "GET", uri, hdr, "")
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	// Every answer says who answered, even a refusal.
	for _, c := range []struct {
		uri  string
		hdr  map[string]string
		code string
	}{
		{"/v1/peer/hello", map[string]string{"X-Warren-Peer": ""}, apierr.BadRequest},
		{"/v1/peer/hello", map[string]string{"X-Warren-Peer": "2"}, apierr.BadRequest},
		{"/v1/peer/hello", map[string]string{"X-Warren-Peer": "x"}, apierr.BadRequest},
		{"/v2/peer/hello", map[string]string{"X-Warren-Peer": "2"}, apierr.Unsupported},
		{"/v1/peer/nothing", nil, apierr.Unsupported},
		{"/v1/peer/call", nil, apierr.Unsupported}, // wrong method
		{"/v1/peer/entries?service=", nil, apierr.BadRequest},
		{"/v1/peer/entries?service=nope&wait=0", nil, apierr.Refused},
		{"/v1/peer/entries?service=echo&wait=99", nil, apierr.BadRequest},
		{"/v1/peer/hello", nil, ""},
	} {
		r := get(c.uri, c.hdr)
		if got := r.Header.Get(apierr.Header); got != c.code {
			t.Errorf("%s %v: %q, want %q", c.uri, c.hdr, got, c.code)
		}
		if r.Header.Get("X-Warren-Node") != "b" || r.Header.Get("X-Warren-Version") != Version {
			t.Errorf("%s: answer without the node's headers: %v", c.uri, r.Header)
		}
	}
	h := get("/v1/peer/hello", nil)
	if !strings.Contains(string(h.Body), `"peer_protocol":[1]`) || h.Header.Get("X-Warren-Peer") != "1" {
		t.Errorf("hello: %s %v", h.Body, h.Header)
	}
}

func TestListsAreVouchedFor(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	x.mu.Lock()
	x.reserved = `{"x":["res1","BAD NAME"],"y":["stolen"],"a":["mine"]}`
	x.mu.Unlock()
	x.set(list(
		entryJSON("alice@x", "x"),
		entryJSON("bob@kit.x", "kit.x"),
		entryJSON("carl@x", "host.x"),
		entryJSON("s@chat.x", "x"),
		entryJSON("m@y", "x"),          // another node's namespace
		entryJSON("n@kit.x", "host.x"), // another child's namespace
		entryJSON("o@a", "x"),          // our namespace
		entryJSON("p@x", "y"),          // origin of another node
		entryJSON("q@x", "kit.y"),      // origin under another node
		entryJSON("r@chat.x", "kit.x"), // a service namespace from a child
		entryJSON("alice@x", "host.x"), // the same address twice
		entryJSON("Bad Name@x", "x"),   // grammar
		`{"address":"t@x","origin":"x","data":"`+strings.Repeat("z", 4000)+`"}`, // too big
	))
	ca := a.nodeSock("echo")
	v := waitAddr(t, ca, "alice@x", true)
	want := []string{"alice@x", "bob@kit.x", "carl@x", "s@chat.x"}
	var got []string
	for _, ad := range v.addrs() {
		if strings.HasSuffix(ad, "x") {
			got = append(got, ad)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("view has %v, want %v", got, want)
	}
	if o, _, _ := v.entry("alice@x"); o != "x" {
		t.Errorf("the earlier duplicate must win, origin %q", o)
	}
	if r := v.Reserved; len(r) != 1 || !slices.Equal(r["x"], []string{"res1"}) {
		t.Errorf("reserved %v", r)
	}
	// Each dropped entry is logged once, however often the list comes back.
	// One summary per list, not a line per entry.
	logged := a.log.count("dropped entries of a peer")
	if logged != 1 || !a.log.hasText("namespace=") || !a.log.hasText("origin=") {
		t.Errorf("%d summaries logged: %s", logged, a.log.String())
	}
	// The same list again is not logged again.
	x.set(x.entries)
	time.Sleep(200 * time.Millisecond)
	if got := a.log.count("dropped entries of a peer"); got != logged {
		t.Errorf("the same drops were logged again: %d then %d", logged, got)
	}
	x.set(list(entryJSON("alice@x", "x"), entryJSON("m@y", "x"), entryJSON("zed@x", "x")))
	waitAddr(t, ca, "zed@x", true)
}

func TestNodeHeaderMustMatchTheNodeReached(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	x.set(list(entryJSON("alice@x", "x")))
	ca := a.nodeSock("echo")
	waitAddr(t, ca, "alice@x", true)
	// An answer from another node: the peer is down, its entries are stale,
	// whatever the body says.
	x.mu.Lock()
	x.nodeHdr = "somebody"
	x.mu.Unlock()
	x.set(list(entryJSON("evil@x", "x")))
	eventually(t, "x down", func() bool { return !a.peerUp("x") })
	v := ca.view()
	if v.has("evil@x") {
		t.Error("an entry from an answer with the wrong node header was used")
	}
	if _, stale, ok := v.entry("alice@x"); !ok || !stale {
		t.Errorf("alice@x must stay, stale: ok=%v stale=%v", ok, stale)
	}
	kit := a.childSock("kit", "echo")
	expect(t, kit.call("x", "m", "", ""), apierr.Offline)
	// The same for a list that says it is another node's, a wrong protocol
	// header, a malformed list and a list without entries.
	for _, step := range []struct {
		name string
		set  func()
	}{
		{"body node", func() { x.mu.Lock(); x.nodeHdr, x.listNode = "", "other"; x.mu.Unlock() }},
		{"protocol", func() { x.mu.Lock(); x.nodeHdr, x.listNode, x.protoHdr = "", "", "2"; x.mu.Unlock() }},
		{"malformed", func() { x.mu.Lock(); x.protoHdr, x.rawList = "", "{not json"; x.mu.Unlock() }},
		{"no entries", func() { x.mu.Lock(); x.rawList = `{"node":"x","version":"v1","service":"echo"}`; x.mu.Unlock() }},
		{"good again", func() { x.mu.Lock(); x.rawList = ""; x.mu.Unlock() }},
	} {
		name, set := step.name, step.set
		set()
		x.set(list(entryJSON("alice@x", "x")))
		if name == "good again" {
			eventually(t, "x up again", func() bool { return a.peerUp("x") })
			// Stale until the new session's first answer, then fresh.
			eventually(t, "alice fresh after recovery", func() bool { _, stale, ok := ca.view().entry("alice@x"); return ok && !stale })
			continue
		}
		eventually(t, name+": x down", func() bool { return !a.peerUp("x") })
		// A refused list is never read as an empty one.
		if _, _, ok := ca.view().entry("alice@x"); !ok {
			t.Errorf("%s: the entries vanished", name)
		}
	}
}

func TestFairnessPerOriginRoot(t *testing.T) {
	net := link.NewNet()
	quick(t)
	y := strings.Replace(baseYAML, "namespaces: [chat]", "namespaces: [chat]\n      per_root: 2", 1)
	b := meshNode(t, net, "b", y, nil)
	a, c := meshNode(t, net, "a", "", nil), meshNode(t, net, "c", "", nil)
	waitUp(t, []*mn{a, b, c}, "a", "b", "c")
	cb := b.nodeSock("echo")
	lease, _ := cb.serve(1, true) // no poll: calls are held
	_ = lease
	held := func() int {
		return b.node.sockState("b", "echo", func(s *sock) int {
			if s.lease == nil {
				return 0
			}
			return len(s.lease.held)
		})
	}
	var wg sync.WaitGroup
	callFrom := func(m *mn, child string) resp {
		return m.childSock(child, "echo").call("b", "m", "", "&timeout_ms=1500")
	}
	// Two nodes' children and the node itself are one root: a has 2 places.
	results := make(chan resp, 8)
	for _, ch := range []string{"kit", "host"} {
		wg.Add(1)
		go func(ch string) { defer wg.Done(); results <- callFrom(a, ch) }(ch)
	}
	eventually(t, "two held", func() bool { return held() == 2 })
	expect(t, callFrom(a, "host2"), apierr.Busy) // a's share is used up
	// Other roots still have their own share: c, and b's own child.
	wg.Add(2)
	go func() { defer wg.Done(); results <- callFrom(c, "kit") }()
	go func() {
		defer wg.Done()
		results <- b.childSock("kit", "echo").call("b", "m", "", "&timeout_ms=1500")
	}()
	eventually(t, "four held", func() bool { return held() == 4 })
	wg.Wait()
	close(results)
	for r := range results {
		expect(t, r, apierr.Timeout)
	}
}

func TestPoolKeys(t *testing.T) {
	if poolKey("kit.b", false) != "b" || poolKey("b", false) != "b" || poolKey("kit.a", true) != "kit.a" {
		t.Error("pool keys")
	}
}

func TestThreeNodes(t *testing.T) {
	net := link.NewNet()
	a, b, c := meshNode(t, net, "a", "", nil), meshNode(t, net, "b", "", nil), meshNode(t, net, "c", "", nil)
	all := []*mn{a, b, c}
	waitUp(t, all, "a", "b", "c")
	publish(t, a.nodeSock("echo"), "alice")
	publish(t, b.childSock("kit", "echo"), "bob")
	ec := startEcho(t, c.nodeSock("echo"))
	refused(t, c.nodeSock("echo").put(ec.lease, names("cara"), ""))
	waitAddr(t, a.nodeSock("echo"), "cara@c", true)
	waitAddr(t, a.nodeSock("echo"), "bob@kit.b", true)
	v := a.nodeSock("echo").view()
	if got := v.addrs(); !slices.Equal(got, []string{"alice@a", "bob@kit.b", "cara@c"}) {
		t.Errorf("a's view: %v", got)
	}
	// Each node sees the other two.
	waitAddr(t, c.nodeSock("echo"), "alice@a", true)
	waitAddr(t, c.nodeSock("echo"), "bob@kit.b", true)
	// A call goes from its node to the target's node, whichever the two.
	expectOK(t, b.childSock("kit", "echo").call("cara@c", "m", "x", ""))
	// A call from a is not passed on by b to c: b refuses what is not its own.
	r, err := raw(t, a.f, "b", "POST", "/v1/peer/call?service=echo&version=1&method=m&to=cara@c&origin=a", nil, "")
	if code := rawCode(t, r, err); code != apierr.NoTarget {
		t.Errorf("b passed a call on: %q", code)
	}
	// c's view of its own entries has no copies from the others.
	if got := c.nodeSock("echo").view().addrs(); len(got) != 3 {
		t.Errorf("c's view: %v", got)
	}
}

func TestStaleEntriesOfAnUnreachableNode(t *testing.T) {
	quick(t)
	net := link.NewNet()
	a, b := meshNode(t, net, "a", "", nil), meshNode(t, net, "b", "", nil)
	defer func() {
		if t.Failed() {
			t.Log(a.log.String())
		}
	}()
	waitUp(t, []*mn{a, b}, "a", "b")
	publish(t, b.nodeSock("echo"), "bob")
	ca := a.nodeSock("echo")
	waitAddr(t, ca, "bob@b", true)
	net.SetDown("b", true)
	eventually(t, "bob stale", func() bool { _, stale, ok := ca.view().entry("bob@b"); return ok && stale })
	// Dropped after StaleFor.
	waitAddr(t, ca, "bob@b", false)
	if p := a.peerInfo("b"); p != nil && p["up"] == true {
		t.Error("b is up")
	}
	// Back again: fresh.
	net.SetDown("b", false)
	eventually(t, "bob back fresh", func() bool { _, stale, ok := ca.view().entry("bob@b"); return ok && !stale })
	// Its service losing the lease makes the entries stale there, and the
	// staleness is passed on.
	lb, _ := b.nodeSock("echo").serve(1, true)
	refused(t, b.nodeSock("echo").put(lb, names("bob"), ""))
	waitAddr(t, ca, "bob@b", true)
	b.nodeSock("echo").post("/v1/service/release?lease="+lb, "")
	eventually(t, "bob stale on b's say", func() bool { _, stale, ok := ca.view().entry("bob@b"); return ok && stale })
	waitAddr(t, ca, "bob@b", false)
}

func TestStaleBeforeDropWhileDown(t *testing.T) {
	// A peer that never came back after a transport failure is dropped from
	// the status once its entries are.
	net := link.NewNet()
	a, b := meshNode(t, net, "a", "", nil), meshNode(t, net, "b", "", nil)
	waitUp(t, []*mn{a, b}, "a", "b")
	defer func() {
		if t.Failed() {
			t.Log(a.log.String())
		}
	}()
	b.node.Close()
	b.f.Close()
	eventually(t, "b gone from a's status", func() bool { return a.peerInfo("b") == nil })

}

func TestClockJumpRestartsPolls(t *testing.T) {
	net := link.NewNet()
	var offset atomic.Int64
	start := time.Now()
	clock := func() (time.Duration, time.Duration) {
		d := time.Since(start)
		return d + time.Duration(offset.Load()), d
	}
	quick(t)
	ClockTick = 20 * time.Millisecond
	a := meshNode(t, net, "a", "", func(o *Options) { o.Clock = clock })
	b := meshNode(t, net, "b", "", nil)
	waitUp(t, []*mn{a, b}, "a", "b")
	publish(t, b.nodeSock("echo"), "bob")
	waitAddr(t, a.nodeSock("echo"), "bob@b", true)
	hellos := carried(a.f, "/hello")
	time.Sleep(100 * time.Millisecond)
	if got := carried(a.f, "/hello"); got != hellos {
		t.Fatalf("hello was sent again without a jump: %d then %d", hellos, got)
	}
	offset.Add(int64(time.Hour)) // resume from suspend
	eventually(t, "polls restarted", func() bool { return carried(a.f, "/hello") > hellos })
	if !a.log.hasText("restarting polls") {
		t.Error("the restart is not logged")
	}
	// Still working afterwards.
	publish(t, b.nodeSock("echo"), "bob3")
	waitAddr(t, a.nodeSock("echo"), "bob3@b", true)
	// A small shift (a clock set by a second) does not restart anything.
	hellos = carried(a.f, "/hello")
	offset.Add(int64(time.Second))
	time.Sleep(100 * time.Millisecond)
	if got := carried(a.f, "/hello"); got != hellos {
		t.Errorf("a one second step restarted the polls")
	}
}

func (l *logBuf) hasText(s string) bool { return l.count(s) > 0 }

func TestChangesToPeersAreDebounced(t *testing.T) {
	net := link.NewNet()
	quick(t)
	PeerDebounce = 200 * time.Millisecond
	a, b := meshNode(t, net, "a", "", nil), meshNode(t, net, "b", "", nil)
	waitUp(t, []*mn{a, b}, "a", "b")
	cb := b.nodeSock("echo")
	lb, _ := cb.serve(1, true)
	waitAddrCalls := carried(a.f, "entries?service=echo")
	for i := 0; i < 6; i++ {
		refused(t, cb.put(lb, names("n"+strings.Repeat("x", i+1)), ""))
		time.Sleep(25 * time.Millisecond)
	}
	waitAddr(t, a.nodeSock("echo"), "nxxxxxx@b", true)
	if d := carried(a.f, "entries?service=echo") - waitAddrCalls; d > 2 {
		t.Errorf("six quick PUTs made %d list answers at the peer", d)
	}
	// The peer got nothing before the debounce passed.
	refused(t, cb.put(lb, names("later"), ""))
	time.Sleep(60 * time.Millisecond)
	if a.nodeSock("echo").view().has("later@b") {
		t.Error("a change reached the peer before the debounce")
	}
	waitAddr(t, a.nodeSock("echo"), "later@b", true)
}

func TestLongPollsPerPeerAndList(t *testing.T) {
	net := link.NewNet()
	b := meshNode(t, net, "b", "", nil)
	evil := net.Join("evil")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poll := func(uri string) string {
		h := http.Header{}
		h.Set("X-Warren-Peer", "1")
		r, err := evil.Carry(ctx, "b", &link.Request{Method: "GET", URI: uri, Header: h, Deadline: time.Now().Add(10 * time.Second)})
		if err != nil {
			return "err"
		}
		return r.Header.Get(apierr.Header)
	}
	// since differs from the current version, so a poll answers at once; to hold
	// one open the version must be the current one.
	cur := func(uri string) string {
		h := http.Header{}
		h.Set("X-Warren-Peer", "1")
		r, _ := evil.Carry(ctx, "b", &link.Request{Method: "GET", URI: uri, Header: h, Deadline: time.Now().Add(5 * time.Second)})
		var v struct{ Version string }
		_ = json.Unmarshal(r.Body, &v)
		return v.Version
	}
	svcURI := "/v1/peer/services?wait=0"
	entURI := "/v1/peer/entries?service=echo&wait=0"
	for _, c := range []struct{ base, key string }{{svcURI, "evil|services"}, {entURI, "evil|entries|echo"}} {
		ver := cur(c.base)
		long := strings.Replace(c.base, "wait=0", "wait=25&since="+ver, 1)
		for i := 0; i < MaxListPolls; i++ {
			go poll(long)
		}
		key := c.key
		eventually(t, "two polls open on "+key, func() bool {
			b.node.mu.Lock()
			defer b.node.mu.Unlock()
			return b.node.pollSlots[key] == MaxListPolls
		})
		if got := poll(long); got != apierr.Busy {
			t.Errorf("%s: the third poll got %q", key, got)
		}
	}
	// Another list, and the same list asked by nobody else, are not affected.
	if got := poll("/v1/peer/entries?service=other&wait=0"); got != "" {
		t.Errorf("another list: %q", got)
	}
}

func TestAtMost32ServicesOfAPeerAreUsed(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	var svcs []string
	for i := 0; i < 40; i++ {
		svcs = append(svcs, "s"+string(rune('a'+i/26))+string(rune('a'+i%26)))
	}
	// "echo" is past the 32nd, so this node never polls it.
	x.mu.Lock()
	x.svcs = append(append([]string(nil), svcs...), "echo")
	x.mu.Unlock()
	x.set(list(entryJSON("alice@x", "x")))
	eventually(t, "x up", func() bool { return a.peerUp("x") })
	time.Sleep(200 * time.Millisecond)
	if a.nodeSock("echo").view().has("alice@x") {
		t.Error("a service beyond the 32nd was used")
	}
	// Within the limit it is.
	x.mu.Lock()
	x.svcs = []string{"echo"}
	x.mu.Unlock()
	x.set(list(entryJSON("alice@x", "x")))
	waitAddr(t, a.nodeSock("echo"), "alice@x", true)
	// And when the peer stops listing it, its entries go.
	x.mu.Lock()
	x.svcs = []string{"other"}
	x.mu.Unlock()
	x.set(list(entryJSON("alice@x", "x")))
	waitAddr(t, a.nodeSock("echo"), "alice@x", false)
}

func TestMixedPeerProtocols(t *testing.T) {
	net := link.NewNet()
	old := meshNode(t, net, "old", "", nil) // speaks 1
	// A node that announces 1 and 2 talks 1 to it; the entries flow.
	both := meshNode(t, net, "both", "", func(o *Options) {
		o.PeerProtocols = []int{1, 2}
		o.PeerHandlers = map[int]http.Handler{2: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})}
	})
	waitUp(t, []*mn{old, both}, "old", "both")
	publish(t, old.nodeSock("echo"), "oldie")
	waitAddr(t, both.nodeSock("echo"), "oldie@old", true)
	if p := old.peerInfo("both"); p == nil || !slices.Equal(p["peer_protocol"].([]int), []int{1, 2}) {
		t.Errorf("old's view of both: %v", p)
	}
	// The node's own status shows the protocols it was given, not the build's.
	var st struct {
		Warren struct {
			PeerProtocol []int `json:"peer_protocol"`
		} `json:"warren"`
	}
	sr := both.nodeSock("echo").get("/v1/service/status")
	expectOK(t, sr)
	sr.json(t, &st)
	if !slices.Equal(st.Warren.PeerProtocol, []int{1, 2}) {
		t.Errorf("status shows peer protocols %v, want [1 2]", st.Warren.PeerProtocol)
	}
	if carried(both.f, "/v2/") != 0 || carried(old.f, "/v2/") != 0 {
		t.Error("version 2 was used with a node that does not speak it")
	}
	// A node that speaks only 2 shares nothing with old: refused loudly,
	// its entries never shown, calls to it are offline, nothing decoded as empty.
	future := meshNode(t, net, "future", "", func(o *Options) {
		o.PeerProtocols = []int{2}
		o.PeerHandlers = map[int]http.Handler{2: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Warren-Peer", "2")
			w.Write([]byte(`{"node":"future","version":"1","peer_protocol":[2],"features":[]}`))
		})}
	})
	eventually(t, "old to refuse future", func() bool {
		p := old.peerInfo("future")
		return p != nil && p["up"] == false && p["error"] != nil
	})
	if !old.log.hasText("no common peer protocol") && !old.log.hasText("speaks none of") {
		t.Error("the refusal is not logged")
	}
	expect(t, old.childSock("kit", "echo").call("future", "m", "", ""), apierr.Offline)
	// future asks old on version 2 and is told it is unsupported, as a path.
	r, err := raw(t, future.f, "old", "GET", "/v2/peer/hello", map[string]string{"X-Warren-Peer": "2"}, "")
	if code := rawCode(t, r, err); code != apierr.Unsupported {
		t.Errorf("old on /v2: %q", code)
	}
	// Its hello on version 1 is refused too: the header says 2.
	r, err = raw(t, future.f, "old", "GET", "/v1/peer/hello", map[string]string{"X-Warren-Peer": "2"}, "")
	if code := rawCode(t, r, err); code != apierr.BadRequest {
		t.Errorf("old on /v1 with version 2: %q", code)
	}
	if future.peerInfo("old") == nil {
		t.Log("future has no view of old (it cannot ask on version 1)")
	}
}

// TestProtocolWindow runs the one-release window: both nodes announce 1 and
// 2, and the highest they share and implement is used.
func TestProtocolWindow(t *testing.T) {
	quick(t)
	implementedProtocols[2] = true
	defer delete(implementedProtocols, 2)
	net := link.NewNet()
	var nodes [2]*Node
	var mu sync.Mutex
	// Version 2 is version 1 under another path, which is all the test needs
	// to show the choice, the headers and the paths.
	v2 := func(i int) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			n := nodes[i]
			mu.Unlock()
			r2 := r.Clone(r.Context())
			r2.URL.Path = strings.Replace(r.URL.Path, "/v2/", "/v1/", 1)
			n.servePeerV1(w, r2)
		})
	}
	mk := func(name string, i int) *mn {
		m := meshNode(t, net, name, "", func(o *Options) {
			o.PeerProtocols = []int{1, 2}
			o.PeerHandlers = map[int]http.Handler{2: v2(i)}
		})
		mu.Lock()
		nodes[i] = m.node
		mu.Unlock()
		return m
	}
	a, b := mk("a", 0), mk("b", 1)
	waitUp(t, []*mn{a, b}, "a", "b")
	publish(t, b.nodeSock("echo"), "bob")
	waitAddr(t, a.nodeSock("echo"), "bob@b", true)
	if carried(a.f, "/v2/peer/entries") == 0 || carried(a.f, "/v1/peer/entries") != 0 {
		t.Errorf("entries were not asked on version 2: %v", a.f.Carried())
	}
	if carried(a.f, "/v1/peer/hello") == 0 {
		t.Error("hello must go on the lowest version")
	}
	startEcho(t, b.nodeSock("echo"))
	waitAddr(t, b.nodeSock("echo"), "bob@b", true)
	expectOK(t, a.childSock("kit", "echo").call("b", "m", "", ""))
	if carried(a.f, "/v2/peer/call") == 0 {
		t.Error("the call did not go on version 2")
	}
}

func TestFeaturesAreOnlyUsedWhenAnnounced(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x") // announces x-test
	eventually(t, "x up", func() bool { return a.peerUp("x") })
	if !a.node.peerFeature("x", "x-test") || a.node.peerFeature("x", "other") || a.node.peerFeature("nobody", "x-test") {
		t.Error("peerFeature")
	}
	if p := a.peerInfo("x"); p["version"] != "9.9.9" {
		t.Errorf("version in status: %v", p)
	}
	_ = x
}

func TestPeerRefusedThatIsThisNode(t *testing.T) {
	// A transport that lists this node among its peers does not make a peer of it.
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	a.node.mu.Lock()
	a.node.peers = []string{"a", "Bad Name"}
	a.node.syncPeers()
	n := len(a.node.peerStates)
	a.node.mu.Unlock()
	if n != 0 {
		t.Errorf("%d peers", n)
	}
}

func TestPeerCallForAnUnknownServiceMakesNoState(t *testing.T) {
	net := link.NewNet()
	b := meshNode(t, net, "b", "", nil)
	evil := net.Join("evil")
	count := func() int { b.node.mu.Lock(); defer b.node.mu.Unlock(); return len(b.node.svcs) }
	before := count()
	for i := 0; i < 100; i++ {
		r, err := raw(t, evil, "b", "POST", fmt.Sprintf("/v1/peer/call?service=s%d&version=1&method=m&to=b&origin=evil", i), nil, "x")
		if c := rawCode(t, r, err); c != apierr.Refused {
			t.Fatalf("service s%d: %q", i, c)
		}
		r, err = raw(t, evil, "b", "GET", fmt.Sprintf("/v1/peer/entries?service=s%d&wait=0", i), nil, "")
		if c := rawCode(t, r, err); c != apierr.Refused {
			t.Fatalf("entries s%d: %q", i, c)
		}
	}
	if got := count(); got != before {
		t.Errorf("peer input made service state: %d then %d", before, got)
	}
}

func TestWindowOneTwoAgainstTwoOnly(t *testing.T) {
	quick(t)
	implementedProtocols[2] = true
	defer delete(implementedProtocols, 2)
	net := link.NewNet()
	var mu sync.Mutex
	nodes := map[string]*Node{}
	v2 := func(name string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			n := nodes[name]
			mu.Unlock()
			r2 := r.Clone(r.Context())
			r2.URL.Path = strings.Replace(r.URL.Path, "/v2/", "/v1/", 1)
			n.servePeerV1(w, r2)
		})
	}
	mk := func(name string, protos []int) *mn {
		m := meshNode(t, net, name, "", func(o *Options) {
			o.PeerProtocols = protos
			o.PeerHandlers = map[int]http.Handler{2: v2(name)}
		})
		mu.Lock()
		nodes[name] = m.node
		mu.Unlock()
		return m
	}
	a, c := mk("a", []int{1, 2}), mk("c", []int{2})
	waitUp(t, []*mn{a, c}, "a", "c")
	publish(t, a.nodeSock("echo"), "alice")
	publish(t, c.nodeSock("echo"), "cara")
	waitAddr(t, a.nodeSock("echo"), "cara@c", true)
	waitAddr(t, c.nodeSock("echo"), "alice@a", true)
	if carried(a.f, "/v1/peer/entries") != 0 || carried(a.f, "/v2/peer/entries") == 0 {
		t.Error("the highest common version was not used")
	}
}

func TestServiceRemovedAndAddedAgainWithinTheDebounceIsPolledAgain(t *testing.T) {
	quick(t)
	PeerDebounce = 600 * time.Millisecond
	net := link.NewNet()
	a, b := meshNode(t, net, "a", "", nil), meshNode(t, net, "b", "", nil)
	waitUp(t, []*mn{a, b}, "a", "b")
	publish(t, b.nodeSock("other"), "zed")
	waitAddr(t, a.nodeSock("other"), "zed@b", true)
	without := strings.Replace(baseYAML, "    - name: other\n      allow: [\"*\"]\n", "", 1)
	if without == baseYAML {
		t.Fatal("test config not changed")
	}
	if err := a.node.Reload(parseYAML(t, a.dir, strings.Replace(without, "node: n1", "node: a", 1))); err != nil {
		t.Fatal(err)
	}
	if err := a.node.Reload(parseYAML(t, a.dir, strings.Replace(baseYAML, "node: n1", "node: a", 1))); err != nil {
		t.Fatal(err)
	}
	waitAddr(t, a.nodeSock("other"), "zed@b", true)
}

func TestRefusedEntriesPollKeepsThePeerUp(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	x.set(list(entryJSON("alice@x", "x")))
	ca := a.nodeSock("echo")
	waitAddr(t, ca, "alice@x", true)
	x.mu.Lock()
	x.entryErr = apierr.Refused
	x.mu.Unlock()
	x.set(list(entryJSON("alice@x", "x"))) // wakes the poll, which is now refused
	waitAddr(t, ca, "alice@x", false)
	time.Sleep(300 * time.Millisecond)
	if !a.peerUp("x") {
		t.Error("the peer was marked down for a service it dropped")
	}
	if a.log.hasText("peer down") {
		t.Error("the peer flapped")
	}
}

func TestDuplicateServiceNamesAreCheap(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	x.mu.Lock()
	x.svcs = make([]string, 50000)
	for i := range x.svcs {
		x.svcs[i] = "echo"
	}
	x.mu.Unlock()
	x.set(list(entryJSON("alice@x", "x")))
	waitAddr(t, a.nodeSock("echo"), "alice@x", true)
}

func TestServiceDroppedByAPeerWhileItWasAwayIsNotKeptLive(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	x.set(list(entryJSON("alice@x", "x")))
	ca := a.nodeSock("echo")
	waitAddr(t, ca, "alice@x", true)
	net.SetDown("x", true)
	eventually(t, "alice stale", func() bool { _, stale, ok := ca.view().entry("alice@x"); return ok && stale })
	x.mu.Lock()
	x.svcs = []string{"other"} // dropped while it was away
	x.mu.Unlock()
	x.set(list(entryJSON("alice@x", "x")))
	net.SetDown("x", false)
	eventually(t, "x up", func() bool { return a.peerUp("x") })
	waitAddr(t, ca, "alice@x", false)
}

// busyPolls makes carrying a list poll fail with a kind while set.
type busyPolls struct {
	link.Link
	kind atomic.Value
}

func (b *busyPolls) Carry(ctx context.Context, to string, r *link.Request) (*link.Response, error) {
	if k, _ := b.kind.Load().(string); k != "" && strings.Contains(r.URI, "/entries?") {
		return nil, &link.CarryError{Kind: k, Detail: "injected"}
	}
	return b.Link.Carry(ctx, to, r)
}

func TestPollsTheTransportCouldNotCarryAreRetriedWithoutMarkingThePeerDown(t *testing.T) {
	for _, kind := range []string{link.Busy, link.NotSent} {
		net := link.NewNet()
		var bp *busyPolls
		a := meshNode(t, net, "a", "", func(o *Options) { bp = &busyPolls{Link: o.Link}; bp.kind.Store(kind); o.Link = bp })
		x := newScripted(t, net, "x")
		x.set(list(entryJSON("alice@x", "x")))
		eventually(t, "x up", func() bool { return a.peerUp("x") })
		time.Sleep(300 * time.Millisecond)
		if !a.peerUp("x") || a.log.hasText("peer down") {
			t.Fatalf("%s: the peer was marked down for a refused poll", kind)
		}
		bp.kind.Store("")
		waitAddr(t, a.nodeSock("echo"), "alice@x", true)
		if !a.peerUp("x") || a.log.hasText("peer down") {
			t.Errorf("%s: the peer flapped", kind)
		}
	}
}

func TestBusyTransportIsBusyForCallers(t *testing.T) {
	net := link.NewNet()
	var fc *failCalls
	a := meshNode(t, net, "a", "", func(o *Options) { fc = &failCalls{Link: o.Link}; o.Link = fc })
	b := meshNode(t, net, "b", "", nil)
	waitUp(t, []*mn{a, b}, "b")
	fc.set(link.Busy)
	expect(t, a.childSock("kit", "echo").call("b", "m", "", ""), apierr.Busy)
}

func TestDroppedEntriesLogIsRateLimitedPerPeer(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	ca := a.nodeSock("echo")
	for i := 1; i <= 8; i++ {
		var es []string
		for j := 0; j < i; j++ { // another count of drops each time
			es = append(es, entryJSON(fmt.Sprintf("m%d@y", j), "x"))
		}
		es = append(es, entryJSON(fmt.Sprintf("ok%d@x", i), "x"))
		x.set(list(es...))
		waitAddr(t, ca, fmt.Sprintf("ok%d@x", i), true)
	}
	if n := a.log.count("dropped entries of a peer"); n != 1 {
		t.Errorf("%d summaries for one peer, want 1", n)
	}
}

func TestOverlongVersionIsRefusedAndUnchangedListsDoNotRebuild(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	x.set(list(entryJSON("alice@x", "x")))
	ca := a.nodeSock("echo")
	waitAddr(t, ca, "alice@x", true)
	v1 := ca.view().Version
	// The same list under a new version: the merged view is not rebuilt.
	x.set(list(entryJSON("alice@x", "x")))
	time.Sleep(400 * time.Millisecond)
	if v2 := ca.view().Version; v2 != v1 {
		t.Errorf("an unchanged list moved the view version: %s then %s", v1, v2)
	}
	x.mu.Lock()
	x.rawList = fmt.Sprintf(`{"node":"x","version":%q,"service":"echo","entries":[]}`, strings.Repeat("v", 200))
	x.mu.Unlock()
	x.set(list(entryJSON("alice@x", "x")))
	eventually(t, "x down", func() bool { return !a.peerUp("x") })
}

func TestEntryDataIsServedAsCheckedAndListsFit(t *testing.T) {
	net := link.NewNet()
	a, b := meshNode(t, net, "a", "", nil), meshNode(t, net, "b", "", nil)
	waitUp(t, []*mn{a, b}, "a", "b")
	cb := b.nodeSock("echo")
	lb, _ := cb.serve(1, true)
	// 3000 bytes of characters that json.Marshal would escape to six each.
	html := `{"x":"` + strings.Repeat("<", 3000) + `"}`
	padded := `{"y":` + strings.Repeat(" ", 5000) + `1}` // over 3 KiB as sent, tiny when compact
	body := `{"entries":[{"name":"h","data":` + html + `},{"name":"p","data":` + padded + `}],"reserved":[]}`
	if r := cb.do("PUT", "/v1/service/entries?lease="+lb, []byte(body)); r.status != 200 || strings.Contains(string(r.body), `"reason"`) {
		t.Fatalf("PUT: %d %s", r.status, r.body)
	}
	v := waitAddr(t, a.nodeSock("echo"), "h@b", true)
	waitAddr(t, a.nodeSock("echo"), "p@b", true)
	for _, e := range v.Entries {
		if e.Address == "h@b" && string(e.Data) != html {
			t.Errorf("the data came back changed: %d bytes for %d", len(e.Data), len(html))
		}
		if e.Address == "p@b" && string(e.Data) != `{"y":1}` {
			t.Errorf("padded data not compact: %s", e.Data)
		}
	}
	if raw := b.nodeSock("echo").get("/v1/service/view?wait=0"); strings.Contains(string(raw.body), "\\u003c") {
		t.Error("the view escapes HTML characters")
	}
	// A full list of the largest entries fits what peers accept.
	maxEntry := listed{Address: strings.Repeat("a", 64) + "@" + strings.Repeat("n", 63) + "." + strings.Repeat("m", 63), Origin: strings.Repeat("o", 64), Data: make([]byte, MaxEntryData)}
	if MaxListEntries*listedSize(maxEntry) > MaxListBytes || MaxListBytes >= MaxListBody {
		t.Errorf("1000 largest entries (%d bytes) do not fit a list (%d, answer %d)", MaxListEntries*listedSize(maxEntry), MaxListBytes, MaxListBody)
	}
}

func TestOversizedListDropsOnlyThatListAndKeepsTheSession(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	x.set(list(entryJSON("alice@x", "x")))
	ca := a.nodeSock("echo")
	waitAddr(t, ca, "alice@x", true)
	x.mu.Lock()
	x.rawList = `{"node":"x","version":"big","service":"echo","entries":[],"pad":"` + strings.Repeat("z", MaxListBody) + `"}`
	x.mu.Unlock()
	x.set(list(entryJSON("alice@x", "x")))
	waitAddr(t, ca, "alice@x", false)
	if !a.peerUp("x") || a.log.hasText("peer down") {
		t.Error("the session ended for an oversized list")
	}
	x.mu.Lock()
	x.rawList = ""
	x.mu.Unlock()
	waitAddr(t, ca, "alice@x", true)
}

func TestPeerWhosePollsFailBacksOffAndDoesNotFloodTheLog(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	x.mu.Lock()
	x.rawList = "{not json"
	x.mu.Unlock()
	time.Sleep(1200 * time.Millisecond)
	x.mu.Lock()
	hellos := x.hellos
	x.mu.Unlock()
	if hellos > 20 {
		t.Errorf("%d hellos in 1.2 s: the backoff was reset by every hello", hellos)
	}
	if n := a.log.count("peer up") + a.log.count("peer down"); n > 2 {
		t.Errorf("%d up/down lines for one peer", n)
	}
	// The real reason is what is logged.
	if !a.log.hasText("malformed answer") {
		t.Errorf("the reason of the failure is not logged: %s", a.log.String())
	}
}

func TestStaleListsSurviveAnUnlistedSessionAndRefusedPollsComeBack(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	x.set(list(entryJSON("alice@x", "x")))
	ca := a.nodeSock("echo")
	waitAddr(t, ca, "alice@x", true)
	// Before the peer's list of services arrives nothing is deleted.
	a.node.mu.Lock()
	s := a.node.peerStates["x"].sess
	s.listed = false
	a.node.reconcilePollers(s)
	kept := a.node.peerStates["x"].lists["echo"] != nil
	s.listed = true
	a.node.mu.Unlock()
	if !kept {
		t.Error("a list was deleted before the peer's services were known")
	}
	// A refused poll for a service the peer lists again at once: the list
	// comes back without a change of the services list.
	x.mu.Lock()
	x.entryErr = apierr.Refused
	x.mu.Unlock()
	x.set(list(entryJSON("alice@x", "x")))
	waitAddr(t, ca, "alice@x", false)
	x.mu.Lock()
	x.entryErr = ""
	x.mu.Unlock()
	waitAddr(t, ca, "alice@x", true)
	if !a.peerUp("x") {
		t.Error("peer down")
	}
}

func TestPeerStateLogEndsOnTheRealState(t *testing.T) {
	quick(t)
	DropLogEvery = 400 * time.Millisecond
	net := link.NewNet()
	a, x := meshNode(t, net, "a", "", nil), meshNode(t, net, "x", "", nil)
	waitUp(t, []*mn{a, x}, "x")
	// The peer comes and goes inside one window and ends up down.
	for i := 0; i < 2; i++ {
		net.SetDown("x", true)
		eventually(t, "x down", func() bool { return !a.peerUp("x") })
		if i == 0 {
			net.SetDown("x", false)
			eventually(t, "x up", func() bool { return a.peerUp("x") })
		}
	}
	time.Sleep(900 * time.Millisecond) // the window ends
	s := a.log.String()
	up, down := strings.LastIndex(s, "msg=\"peer up\""), strings.LastIndex(s, "msg=\"peer down\"")
	if down < up || down < 0 {
		t.Errorf("the log ends on the wrong state (last up at %d, last down at %d):\n%s", up, down, s)
	}
	if !a.log.hasText("held_back_changes") {
		t.Error("held back changes are not counted")
	}
}

func TestNoPeerRequestsBeforeAHelloHasPassed(t *testing.T) {
	net := link.NewNet()
	f := net.Join("n1")
	var fl *flaky
	e := newEnv(t, "", func() { fl = &flaky{Link: f}; fl.set(true, ""); testLink = fl })
	_ = e
	evil := net.Join("evil")
	r, err := raw(t, evil, "n1", "GET", "/v1/peer/hello", nil, "")
	if c := rawCode(t, r, err); c != apierr.NotReady {
		t.Errorf("before a hello passed: %q", c)
	}
	fl.set(false, "")
	eventually(t, "the peer API to open", func() bool {
		r, err := raw(t, evil, "n1", "GET", "/v1/peer/hello", nil, "")
		return err == nil && r.Status == 200
	})
}

// bigAnswers makes carrying an entries poll fail as an answer too big.
type bigAnswers struct {
	link.Link
	on atomic.Bool
}

func (b *bigAnswers) Carry(ctx context.Context, to string, r *link.Request) (*link.Response, error) {
	if b.on.Load() && strings.Contains(r.URI, "/entries?") {
		return nil, &link.CarryError{Kind: link.Lost, Detail: "too big", TooBig: true}
	}
	return b.Link.Carry(ctx, to, r)
}

func TestAnswerOverWhatATransportCarriesDropsThatListOnly(t *testing.T) {
	net := link.NewNet()
	var bg *bigAnswers
	a := meshNode(t, net, "a", "", func(o *Options) { bg = &bigAnswers{Link: o.Link}; o.Link = bg })
	x := newScripted(t, net, "x")
	x.set(list(entryJSON("alice@x", "x")))
	ca := a.nodeSock("echo")
	waitAddr(t, ca, "alice@x", true)
	bg.on.Store(true)
	x.set(list(entryJSON("alice@x", "x")))
	waitAddr(t, ca, "alice@x", false)
	if !a.peerUp("x") || a.log.hasText("peer down") {
		t.Error("the session ended")
	}
	if !a.log.hasText("dropped a peer's list") {
		t.Error("the drop is not logged")
	}
	bg.on.Store(false)
	waitAddr(t, ca, "alice@x", true)
}

// viaTransport carries over a real link A: a link.Client talking to
// link.TransportHandler, whose backend is the fake network, except that an
// entries poll answers as an answer too big to carry while on is set.
type viaTransport struct {
	link.Link
	c  *link.Client
	on atomic.Bool
}

type tooBigBackend struct {
	f  link.Link
	vt *viaTransport
}

func (b *tooBigBackend) Hello(ctx context.Context) link.Hello { h, _ := b.f.Hello(ctx); return h }
func (b *tooBigBackend) Peers(done <-chan struct{}, since string, wait time.Duration) link.PeerList {
	return link.PeerList{}
}
func (b *tooBigBackend) Carry(r *http.Request, to string, req *link.Request) (*link.Response, error) {
	if b.vt.on.Load() && strings.Contains(req.URI, "/entries?") {
		return nil, &link.CarryError{Kind: link.Lost, Detail: "too big", TooBig: true}
	}
	return b.f.Carry(r.Context(), to, req)
}

func TestTooBigSurvivesLinkAAndDropsOnlyThatList(t *testing.T) {
	net := link.NewNet()
	var vt *viaTransport
	a := meshNode(t, net, "a", "", func(o *Options) {
		vt = &viaTransport{Link: o.Link}
		ln, err := nettest()
		if err != nil {
			t.Fatal(err)
		}
		go link.ServeListener(ln, link.TransportHandler(&tooBigBackend{f: o.Link, vt: vt}), nil)
		vt.c = link.NewClient(func(ctx context.Context) (netConn, error) { return dialTCP(ctx, ln.Addr().String()) })
		o.Link = vt
	})
	x := newScripted(t, net, "x")
	x.set(list(entryJSON("alice@x", "x")))
	ca := a.nodeSock("echo")
	waitAddr(t, ca, "alice@x", true)
	vt.on.Store(true)
	x.set(list(entryJSON("alice@x", "x")))
	waitAddr(t, ca, "alice@x", false)
	if !a.peerUp("x") || a.log.hasText("peer down") {
		t.Error("the session ended for an answer too big")
	}
	vt.on.Store(false)
	waitAddr(t, ca, "alice@x", true)
}

func (v *viaTransport) Carry(ctx context.Context, to string, r *link.Request) (*link.Response, error) {
	return v.c.Carry(ctx, to, r)
}

// A peer that answers a call with "unsupported" (an older Warren with no such
// endpoint) is reported as offline: the call did not run. The code is not
// passed on, since "unsupported" would read as a missing feature of the
// caller's own socket.
func TestRemoteUnsupportedOnACallIsOffline(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	x := newScripted(t, net, "x")
	x.mu.Lock()
	x.onCall = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Warren-Error", apierr.Unsupported)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"unsupported","detail":"no such endpoint"}`))
	}
	x.mu.Unlock()
	eventually(t, "x up", func() bool { return a.peerUp("x") })
	kit := a.childSock("kit", "echo")
	expect(t, kit.call("x", "m", "", ""), apierr.Offline)
}

// Peers that come and go leave nothing behind once StaleFor has passed:
// the departed are told apart from the never-known only for that long.
func TestDepartedPeersAreForgotten(t *testing.T) {
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	var names []string
	for i := 0; i < 50; i++ {
		names = append(names, fmt.Sprintf("p%d", i))
	}
	a.node.mu.Lock()
	a.node.peers = names
	a.node.syncPeers()
	for _, ps := range a.node.peerStates {
		ps.up = true // as if they had come up
	}
	a.node.peers = nil
	a.node.syncPeers()
	_, e := a.node.foreign("p7")
	left := len(a.node.peerStates)
	a.node.mu.Unlock()
	if e == nil || e.Code != apierr.Offline {
		t.Errorf("a peer that just left is %v, want offline", e)
	}
	if left == 0 {
		t.Errorf("the peers were forgotten at once")
	}
	eventually(t, "the departed peers to be forgotten", func() bool {
		a.node.mu.Lock()
		defer a.node.mu.Unlock()
		return len(a.node.peerStates) == 0
	})
	a.node.mu.Lock()
	_, e = a.node.foreign("p7")
	a.node.mu.Unlock()
	if e == nil || e.Code != apierr.NoTarget {
		t.Errorf("a forgotten peer is %v, want no-target", e)
	}
}

// However many peers come and go within StaleFor, only MaxGonePeers of the
// departed are kept, the most recent ones.
func TestDepartedPeersAreCapped(t *testing.T) {
	// Set before the node starts and restored after it has stopped (cleanups
	// run last in, first out), so its goroutines never read it while it changes.
	old := MaxGonePeers
	MaxGonePeers = 5
	t.Cleanup(func() { MaxGonePeers = old })
	net := link.NewNet()
	a := meshNode(t, net, "a", "", nil)
	var names []string
	for i := 0; i < 30; i++ {
		names = append(names, fmt.Sprintf("p%d", i))
	}
	a.node.mu.Lock()
	defer a.node.mu.Unlock()
	a.node.peers = names
	a.node.syncPeers()
	for i, name := range names {
		ps := a.node.peerStates[name]
		ps.up = true
		ps.downSince = time.Now().Add(time.Duration(i) * time.Millisecond)
	}
	a.node.peers = nil
	a.node.syncPeers()
	if got := len(a.node.peerStates); got != 5 {
		t.Fatalf("%d departed peers kept, want 5", got)
	}
}
