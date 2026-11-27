package service

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/link"

	"github.com/PeterSR/warren-mesh/internal/apierr"
)

func TestSelf(t *testing.T) {
	e := newEnv(t, "")
	e.mkChild("dyn")
	cases := []struct {
		c          *cl
		relay, ns  string
		flat       bool
		namespaces []string
	}{
		{e.nodeSock("echo"), "n1", "n1", false, []string{"chat.n1"}},
		{e.childSock("host", "echo"), "host.n1", "n1", true, nil},
		{e.childSock("kit", "echo"), "kit.n1", "kit.n1", false, nil},
		{e.childSock("dyn", "echo"), "dyn.n1", "dyn.n1", false, nil},
	}
	for _, c := range cases {
		r := c.c.get("/v1/service/self")
		expectOK(t, r)
		var s struct {
			Relay, Node, Service, EntryNamespace, Boot string
			Flat                                       bool
			Namespaces                                 []string
			MaxCallBody                                int
		}
		r.json(t, &s)
		// Field names in the JSON are snake case; decode by hand.
		var raw map[string]any
		r.json(t, &raw)
		if raw["relay"] != c.relay || raw["entry_namespace"] != c.ns || raw["flat"] != c.flat ||
			raw["node"] != "n1" || raw["service"] != "echo" || raw["max_call_body"] != float64(1<<20) || raw["boot"] != e.node.Boot() {
			t.Errorf("%s: %v", c.relay, raw)
		}
		got := raw["namespaces"].([]any)
		if len(got) != len(c.namespaces) {
			t.Errorf("%s: namespaces %v", c.relay, got)
		}
	}
}

func TestStatusReportsVersionAndFeatures(t *testing.T) {
	e := newEnv(t, "")
	var st struct {
		Node   string `json:"node"`
		Boot   string `json:"boot"`
		Warren struct {
			Version      string   `json:"version"`
			PeerProtocol []int    `json:"peer_protocol"`
			Features     []string `json:"features"`
		} `json:"warren"`
		Children []struct {
			Label string `json:"label"`
			Flat  bool   `json:"flat"`
		} `json:"children"`
		Services []struct {
			Relay string `json:"relay"`
			Name  string `json:"name"`
			Lease bool   `json:"lease"`
		} `json:"services"`
	}
	c := e.nodeSock("echo")
	r := c.get("/v1/service/status")
	expectOK(t, r)
	r.json(t, &st)
	if st.Warren.Version == "" || len(st.Warren.PeerProtocol) != 1 || st.Warren.Features == nil || st.Node != "n1" {
		t.Errorf("status: %+v", st)
	}
	if len(st.Children) != 3 || len(st.Services) != 5 {
		t.Errorf("children %v services %v", st.Children, st.Services)
	}
	c.serve(1, false)
	r = c.get("/v1/service/status")
	r.json(t, &st)
	leased := 0
	for _, s := range st.Services {
		if s.Lease {
			leased++
		}
	}
	if leased != 1 {
		t.Errorf("leases shown: %d", leased)
	}
}

func TestUnsupportedAndConventions(t *testing.T) {
	e := newEnv(t, "")
	c := e.nodeSock("echo")
	for _, p := range []string{"/v1/service/nothing", "/v2/service/self", "/", "/v1/peer/hello", "/v1/transport/hello"} {
		expect(t, c.get(p), apierr.Unsupported)
	}
	// A known path with another method is just as unknown.
	expect(t, c.post("/v1/service/self", ""), apierr.Unsupported)

	// X-Warren: 1 is required.
	req, _ := http.NewRequest("GET", "http://warren/v1/service/self", nil)
	r, err := c.hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.Header.Get(apierr.Header) != apierr.BadRequest {
		t.Errorf("missing X-Warren: %d", r.StatusCode)
	}

	// Chunked bodies are refused.
	conn, err := net.Dial("unix", c.path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write([]byte("POST /v1/service/call?to=n1&method=m&version=1 HTTP/1.1\r\nHost: x\r\nX-Warren: 1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n"))
	rr, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if rr.Header.Get(apierr.Header) != apierr.BadRequest {
		t.Errorf("chunked body: %d %q", rr.StatusCode, rr.Header.Get(apierr.Header))
	}
}

func TestErrorBodyShape(t *testing.T) {
	e := newEnv(t, "")
	r := e.nodeSock("echo").get("/v1/service/calls?lease=nope")
	expect(t, r, apierr.Gone)
	var b struct{ Error, Detail string }
	r.json(t, &b)
	if b.Error != apierr.Gone || b.Detail == "" || len(b.Detail) > 200 {
		t.Errorf("body %+v", b)
	}
}

// ---- leases ----

func TestServeAndTakeover(t *testing.T) {
	e := newEnv(t, "")
	c := e.nodeSock("echo")
	l1, inflight := c.serve(1, false)
	if l1 == "" || inflight != 16 {
		t.Fatalf("lease %q inflight %d", l1, inflight)
	}
	expect(t, c.post("/v1/service/serve?version=1", ""), apierr.AlreadyServed)
	expect(t, c.post("/v1/service/serve", ""), apierr.BadRequest)
	expect(t, c.post("/v1/service/serve?version=0", ""), apierr.BadRequest)

	// An open poll on the old lease is answered gone when it is taken over.
	got := make(chan resp, 1)
	go func() { got <- c.get("/v1/service/calls?lease=" + l1 + "&wait=25") }()
	eventually(t, "the poll to be open", func() bool { return e.node.sockState("n1", "echo", func(s *sock) int { return s.lease.polls }) == 1 })
	l2, _ := c.serve(1, true)
	if l2 == l1 {
		t.Fatal("takeover kept the token")
	}
	select {
	case r := <-got:
		expect(t, r, apierr.Gone)
	case <-time.After(3 * time.Second):
		t.Fatal("old poll not ended")
	}
	expect(t, c.get("/v1/service/calls?lease="+l1+"&wait=0"), apierr.Gone)
	if r := c.get("/v1/service/calls?lease=" + l2 + "&wait=0"); r.status != 204 {
		t.Errorf("new lease poll: %d", r.status)
	}
}

func TestRelease(t *testing.T) {
	e := newEnv(t, "")
	c := e.nodeSock("echo")
	l, _ := c.serve(1, false)
	if r := c.post("/v1/service/release?lease="+l, ""); r.status != 204 {
		t.Fatalf("release: %d", r.status)
	}
	expect(t, c.post("/v1/service/release?lease="+l, ""), apierr.Gone)
	// A new lease needs no take.
	c.serve(1, false)
}

func TestLeaseGraceWithoutPolls(t *testing.T) {
	e := newEnv(t, "")
	c := e.nodeSock("echo")
	l, _ := c.serve(1, false)
	// With no poll open the lease is live now and ends by itself.
	expect(t, c.post("/v1/service/serve?version=1", ""), apierr.AlreadyServed)
	eventually(t, "the lease to end", func() bool { return !e.node.leaseLive("n1", "echo", l) })
	expect(t, c.get("/v1/service/calls?lease="+l+"&wait=0"), apierr.Gone)
	c.serve(1, false)
}

// leaseLive is whether the socket's live lease has this token.
func (n *Node) leaseLive(relay, svc, token string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := n.socks[sockKey{relay, svc}]
	return s != nil && s.leaseFor(token) != nil
}

func TestLeaseStaysLiveWhilePolling(t *testing.T) {
	e := newEnv(t, "")
	c := e.nodeSock("echo")
	l, _ := c.serve(1, false)
	// Polls back to back keep the lease past several graces.
	end := time.Now().Add(3 * LeaseGrace)
	for time.Now().Before(end) {
		if r := c.get("/v1/service/calls?lease=" + l + "&wait=1"); r.status != 204 {
			t.Fatalf("poll: %d %s", r.status, r.body)
		}
	}
	expect(t, c.post("/v1/service/serve?version=1", ""), apierr.AlreadyServed)
}

func TestLeaseGraceCountsFromLastPoll(t *testing.T) {
	e := newEnv(t, "", func() { LeaseGrace = time.Second })
	c := e.nodeSock("echo")
	l, _ := c.serve(1, false)
	c.get("/v1/service/calls?lease=" + l + "&wait=1") // a poll of 100 ms
	ended := time.Now()
	// Half a grace after the poll ended the lease still lives (the grace did
	// not start at serve, which was a poll earlier) ...
	time.Sleep(LeaseGrace / 2)
	if r := c.get("/v1/service/calls?lease=" + l + "&wait=0"); r.status != 204 {
		t.Fatalf("lease ended early: %d", r.status)
	}
	// ... and the grace then runs out, counted from that last poll.
	eventually(t, "the lease to end", func() bool { return !e.node.leaseLive("n1", "echo", l) })
	if d := time.Since(ended); d < LeaseGrace/2 {
		t.Errorf("the lease ended %v after the last poll", d)
	}
}

func TestPollsLimitedByInflight(t *testing.T) {
	y := strings.Replace(baseYAML, "- name: echo\n      allow: [\"*\"]\n      namespaces: [chat]", "- name: echo\n      allow: [\"*\"]\n      namespaces: [chat]\n      inflight: 2", 1)
	e := newEnv(t, y)
	c := e.nodeSock("echo")
	l, inflight := c.serve(1, false)
	if inflight != 2 {
		t.Fatalf("inflight %d", inflight)
	}
	done := make(chan resp, 3)
	for i := 0; i < 2; i++ {
		go func() { done <- c.get("/v1/service/calls?lease=" + l + "&wait=25") }()
	}
	time.Sleep(100 * time.Millisecond)
	expect(t, c.get("/v1/service/calls?lease="+l+"&wait=1"), apierr.Busy)
	c.post("/v1/service/release?lease="+l, "")
	for i := 0; i < 2; i++ {
		expect(t, <-done, apierr.Gone)
	}
}

func TestWaitParam(t *testing.T) {
	e := newEnv(t, "")
	c := e.nodeSock("echo")
	l, _ := c.serve(1, false)
	for _, w := range []string{"26", "-1", "x", "1.5"} {
		expect(t, c.get("/v1/service/calls?lease="+l+"&wait="+w), apierr.BadRequest)
	}
	if c.get("/v1/service/calls?lease="+l+"&wait=0").status != 204 {
		t.Error("wait=0")
	}
}

func TestManagedExitEndsLeaseAtOnce(t *testing.T) {
	e := newEnv(t, "")
	c := e.nodeSock("echo")
	l, _ := c.serve(1, false)
	e.node.ProcessExited("n1", "echo")
	expect(t, c.get("/v1/service/calls?lease="+l+"&wait=0"), apierr.Gone)
	c.serve(1, false) // no grace to wait out
}

func TestLeaseInfoCountsEpochs(t *testing.T) {
	e := newEnv(t, "")
	c := e.nodeSock("echo")
	if held, _, epoch := e.node.LeaseInfo("echo"); held || epoch != 0 {
		t.Fatalf("before any lease: held %v epoch %d", held, epoch)
	}
	before := time.Now()
	c.serve(1, false)
	held, since, epoch := e.node.LeaseInfo("echo")
	if !held || epoch != 1 || since.Before(before) {
		t.Fatalf("first lease: held %v since %v epoch %d", held, since, epoch)
	}
	e.node.ProcessExited("n1", "echo")
	if held, _, epoch := e.node.LeaseInfo("echo"); held || epoch != 1 {
		t.Fatalf("after the exit: held %v epoch %d", held, epoch)
	}
	c.serve(1, false)
	if held, _, epoch := e.node.LeaseInfo("echo"); !held || epoch != 2 {
		t.Fatalf("second lease: held %v epoch %d", held, epoch)
	}
	if held, _, epoch := e.node.LeaseInfo("nobody"); held || epoch != 0 {
		t.Fatalf("unknown service: held %v epoch %d", held, epoch)
	}
}

// Only the node's own services keep an epoch: a child's sockets come and go
// under names others chose, so counting them would only grow the map.
func TestOnlyTheNodesOwnLeasesAreCounted(t *testing.T) {
	e := newEnv(t, "")
	e.mkChild("dyn")
	deadline := time.Now().Add(5 * time.Second)
	var c *cl
	for {
		c = e.childSock("dyn", "echo")
		if _, err := os.Stat(c.path); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.serve(1, false)
	e.nodeSock("echo").serve(1, false)
	e.node.mu.Lock()
	defer e.node.mu.Unlock()
	if len(e.node.epochs) != 1 || e.node.epochs[sockKey{"n1", "echo"}] != 1 {
		t.Errorf("epochs %v", e.node.epochs)
	}
}

// flaky wraps a link so a test can break it.
type flaky struct {
	link.Link
	mu   sync.Mutex
	fail bool
	node string
}

func (f *flaky) Peers(ctx context.Context, since string, wait time.Duration) (link.PeerList, error) {
	f.mu.Lock()
	fail := f.fail
	f.mu.Unlock()
	if fail {
		return link.PeerList{}, errors.New("link closed")
	}
	return f.Link.Peers(ctx, since, 100*time.Millisecond) // short polls, so a break is seen soon
}

func (f *flaky) Hello(ctx context.Context) (link.Hello, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return link.Hello{}, errors.New("link closed")
	}
	h, _ := f.Link.Hello(ctx)
	if f.node != "" {
		h.Node = f.node
	}
	return h, nil
}

func (f *flaky) set(fail bool, node string) { f.mu.Lock(); f.fail, f.node = fail, node; f.mu.Unlock() }

func TestLinkFailureMarksPeersDownAndRecovers(t *testing.T) {
	net := link.NewNet()
	net.Join("far")
	f := &flaky{Link: net.Join("n1")}
	e := newEnv(t, "", func() { testLink = f })
	c := e.nodeSock("echo")
	status := func() (up bool, peers int) {
		var st struct {
			LinkUp bool `json:"link_up"`
			Peers  []any
		}
		c.get("/v1/service/status").json(t, &st)
		return st.LinkUp, len(st.Peers)
	}
	eventually(t, "link up with a peer", func() bool { up, p := status(); return up && p == 1 })
	f.set(true, "")
	eventually(t, "every peer down", func() bool { up, p := status(); return !up && p == 0 })
	expect(t, c.call("x.far", "m", "", ""), apierr.NoTarget)
	// A transport that comes back as another node is refused.
	f.set(false, "wrong")
	time.Sleep(1500 * time.Millisecond)
	if up, _ := status(); up {
		t.Error("link accepted a transport for another node")
	}
	f.set(false, "")
	eventually(t, "link back", func() bool { up, p := status(); return up && p == 1 })
}

func TestStreamNeed(t *testing.T) {
	cfg := parseYAML(t, "/d", baseYAML)
	// 2 node services and 3 configured children at 32 each, 13 template
	// children at 8, plus the 16 for the transport's own requests, plus room
	// for AssumedPeers peers: each with a list of services and one of entries
	// per service (2 here), polled by both sides, at most MaxListPolls open
	// from the peer to this node.
	want := 16 + 2*32 + 3*32 + 13*8 + AssumedPeers*(1+2)*(1+MaxListPolls)
	if n := StreamNeed(cfg); n != want {
		t.Errorf("need %d, want about %d", n, want)
	}
	if n := StreamNeed(cfg); n > link.MinStreams {
		t.Errorf("default config does not fit the link: %d", n)
	}
}

func TestTakeoversDoNotAccumulateTimers(t *testing.T) {
	e := newEnv(t, "")
	c := e.childSock("kit", "echo")
	e.node.mu.Lock()
	before := len(e.node.timers)
	e.node.mu.Unlock()
	for i := 0; i < 200; i++ {
		c.serve(1, true)
	}
	e.node.mu.Lock()
	after := len(e.node.timers)
	e.node.mu.Unlock()
	if after != before {
		t.Errorf("node timers grew from %d to %d over 200 takeovers", before, after)
	}
}

func TestServeIsRateLimited(t *testing.T) {
	e := newEnv(t, "", func() { ServeInterval = time.Hour })
	c := e.childSock("kit", "echo")
	l, _ := c.serve(1, false)
	expect(t, c.post("/v1/service/serve?version=1&take=1", ""), apierr.Busy)
	// The first lease is still the one that lives.
	expectOK(t, c.get("/v1/service/calls?lease="+l+"&wait=0"))
}

func TestAnyNonLiveTokenIsGoneAndNoTokenIsBadRequest(t *testing.T) {
	e := newEnv(t, "")
	c := e.childSock("kit", "echo")
	first, _ := c.serve(1, true)
	for i := 0; i < 20; i++ {
		c.serve(1, true)
	}
	// Ended long ago, never issued, and from another boot all answer gone.
	expect(t, c.get("/v1/service/calls?lease="+first+"&wait=0"), apierr.Gone)
	expect(t, c.get("/v1/service/calls?lease=never-issued&wait=0"), apierr.Gone)
	expect(t, c.post("/v1/service/release?lease=never-issued", ""), apierr.Gone)
	expect(t, c.post("/v1/service/answer?lease=never-issued&id=x", ""), apierr.Gone)
	// No token is the caller's mistake.
	expect(t, c.get("/v1/service/calls?wait=0"), apierr.BadRequest)
	expect(t, c.get("/v1/service/calls?lease=&wait=0"), apierr.BadRequest)
}

func TestStatusCarriesTransportNameAndVersion(t *testing.T) {
	net := link.NewNet()
	f := net.Join("n1")
	f.HelloValue.Name, f.HelloValue.SoftwareVersion = "tr", "9.9"
	e := newEnv(t, "", func() { testLink = f })
	eventually(t, "the hello reaches status", func() bool {
		var st struct {
			Transport struct{ Name, Version string }
		}
		e.nodeSock("echo").get("/v1/service/status").json(t, &st)
		return st.Transport.Name == "tr" && st.Transport.Version == "9.9"
	})
}

type lockedBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

func TestHelloRefusalIsLoggedOnce(t *testing.T) {
	quick(t)
	net := link.NewNet()
	f := net.Join("n1")
	f.HelloValue.Node = "someone-else"
	dir := tmpDir(t)
	cfg := parseYAML(t, dir, baseYAML)
	var buf lockedBuf
	n, err := New(Options{Config: cfg, Link: f, Log: slog.New(slog.NewTextHandler(&buf, nil))})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	eventually(t, "the refusal is logged", func() bool { return strings.Contains(buf.String(), "hello refused") })
	time.Sleep(1700 * time.Millisecond) // several more attempts at 0.5 s, 1 s backoff
	if c := strings.Count(buf.String(), "hello refused"); c != 1 {
		t.Errorf("the same refusal was logged %d times", c)
	}
}

func TestPreExistingOpenServicesDirIsClosed(t *testing.T) {
	quick(t)
	dir := tmpDir(t)
	cfg := parseYAML(t, dir, baseYAML)
	if err := os.MkdirAll(cfg.Services.Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cfg.Services.Dir, 0o777); err != nil {
		t.Fatal(err)
	}
	var buf lockedBuf
	n, err := New(Options{Config: cfg, Log: slog.New(slog.NewTextHandler(&buf, nil))})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	// Warren owns it and no service names a uid, so it closes it, and says so.
	if fi, err := os.Stat(cfg.Services.Dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Errorf("services.dir after start: %v %v, want 0700", fi, err)
	}
	if !strings.Contains(buf.String(), "services.dir mode changed") {
		t.Errorf("the change is not logged:\n%s", buf.String())
	}
}

func TestMemoryBoundCountsStoredEntries(t *testing.T) {
	cfg := parseYAML(t, "/tmp/x", "node: n1\nstate_dir: DIR/state\nservices:\n  dir: DIR/s\n  list:\n    - name: echo\n      allow: [\"*\"]\n")
	s := cfg.Services.List[0]
	floor := int64(s.Inflight+s.Outflight)*callMemory + 2*MaxPutBody + 2*MaxPutEntries*MaxEntryData
	if got := MemoryBound(cfg); got < floor {
		t.Errorf("bound %d is under what one socket may hold, %d", got, floor)
	}
}

func TestNamesFileEndsWithTheLatestSnapshot(t *testing.T) {
	e := newEnv(t, "")
	var last string
	for i := 0; i < 60; i++ {
		last = "name" + itoa(i)
		e.node.mu.Lock()
		e.node.socks[sockKey{"n1", "echo"}].heldNames = []string{last}
		e.node.saveNames()
		e.node.mu.Unlock()
	}
	e.node.flushNames()
	data, err := os.ReadFile(filepath.Join(e.cfg.StateDir, "names.json"))
	if err != nil || !strings.Contains(string(data), `"`+last+`"`) {
		t.Errorf("names.json is not the latest snapshot: %s %v", data, err)
	}
}
