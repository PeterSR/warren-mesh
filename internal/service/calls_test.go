package service

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
)

func TestCallNodeToChild(t *testing.T) {
	e := newEnv(t, "")
	child := startEcho(t, e.childSock("kit", "echo"))
	_ = child
	node := e.nodeSock("echo")
	r := node.call("kit.n1", "hello", "world", "")
	expectOK(t, r)
	if string(r.body) != "hello:world" {
		t.Errorf("answer %q", r.body)
	}
	h := child.last()
	if h.Get("X-Warren-Origin") != "n1" || h.Get("X-Warren-To") != "kit.n1" || h.Get("X-Warren-Method") != "hello" ||
		h.Get("X-Warren-From") != "" || h.Get("X-Warren-Call") == "" {
		t.Errorf("headers %v", h)
	}
	if ms := h.Get("X-Warren-Deadline-Ms"); ms == "" || ms == "0" {
		t.Errorf("deadline %q", ms)
	}
}

func TestCallChildToNodeAndChildToChild(t *testing.T) {
	e := newEnv(t, "")
	nodeEcho := startEcho(t, e.nodeSock("echo"))
	kit := startEcho(t, e.childSock("kit", "echo"))
	e.mkChild("dyn")
	dyn := e.childSock("dyn", "echo")

	// Child to node.
	r := dyn.call("n1", "ping", "a", "")
	expectOK(t, r)
	if string(r.body) != "ping:a" || nodeEcho.last().Get("X-Warren-Origin") != "dyn.n1" {
		t.Errorf("child to node: %q %v", r.body, nodeEcho.last())
	}
	// Child to child, with from.
	r = dyn.call("kit.n1", "ping", "b", "&from=bob")
	expectOK(t, r)
	h := kit.last()
	if string(r.body) != "ping:b" || h.Get("X-Warren-Origin") != "dyn.n1" || h.Get("X-Warren-From") != "bob@dyn.n1" {
		t.Errorf("child to child: %q %v", r.body, h)
	}
	// The explicit namespace form.
	expectOK(t, dyn.call("kit.n1", "ping", "b", "&from=bob@dyn.n1"))
	// From a namespace the socket may not publish in.
	expect(t, dyn.call("kit.n1", "ping", "b", "&from=bob@kit.n1"), apierr.Refused)
	expect(t, dyn.call("kit.n1", "ping", "b", "&from=Bad%20Name"), apierr.BadRequest)
	// The node may speak in its service namespace.
	expectOK(t, e.nodeSock("echo").call("kit.n1", "ping", "b", "&from=alice@chat.n1"))
	if kit.last().Get("X-Warren-From") != "alice@chat.n1" {
		t.Errorf("from %q", kit.last().Get("X-Warren-From"))
	}
}

func TestCallToEntry(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	ke := startEcho(t, kit)
	expectOK(t, kit.put(ke.lease, names("alice"), ""))
	node := startEcho(t, e.nodeSock("echo"))
	expectOK(t, e.nodeSock("echo").put(node.lease, names("root"), ""))

	c := e.childSock("host", "echo")
	r := c.call("alice@kit.n1", "msg", "hi", "")
	expectOK(t, r)
	if string(r.body) != "msg:hi" || ke.last().Get("X-Warren-To") != "alice@kit.n1" {
		t.Errorf("%q %v", r.body, ke.last())
	}
	expectOK(t, c.call("root@n1", "msg", "hi", ""))
	if node.last().Get("X-Warren-To") != "root@n1" {
		t.Errorf("to %q", node.last().Get("X-Warren-To"))
	}
	// An entry nobody publishes.
	expect(t, c.call("nobody@kit.n1", "msg", "hi", ""), apierr.NoTarget)
	expect(t, c.call("nobody@n1", "msg", "hi", ""), apierr.NoTarget)
	expect(t, c.call("nobody@chat.n1", "msg", "hi", ""), apierr.NoTarget)
	expect(t, c.call("nobody@nowhere.n1", "msg", "hi", ""), apierr.NoTarget)
}

func TestCallErrors(t *testing.T) {
	e := newEnv(t, "")
	e.mkChild("dyn")
	dyn := e.childSock("dyn", "echo")
	kit := startEcho(t, e.childSock("kit", "echo"))
	_ = kit

	// bad-request
	expect(t, dyn.post("/v1/service/call?method=m&version=1", ""), apierr.BadRequest)            // no to
	expect(t, dyn.post("/v1/service/call?to=kit.n1&version=1", ""), apierr.BadRequest)           // no method
	expect(t, dyn.post("/v1/service/call?to=kit.n1&method=M!&version=1", ""), apierr.BadRequest) // bad method
	expect(t, dyn.post("/v1/service/call?to=kit.n1&method=m", ""), apierr.BadRequest)            // no version
	expect(t, dyn.post("/v1/service/call?to=kit.n1&method=m&version=0", ""), apierr.BadRequest)  // bad version
	expect(t, dyn.post("/v1/service/call?to=Kel.n1&method=m&version=1", ""), apierr.BadRequest)  // bad relay id
	expect(t, dyn.post("/v1/service/call?to=a@b@c&method=m&version=1", ""), apierr.BadRequest)   // bad address
	expect(t, dyn.call("kit.n1", "m", "", "&timeout_ms=0"), apierr.BadRequest)
	expect(t, dyn.call("kit.n1", "m", "", "&timeout_ms=300001"), apierr.BadRequest)

	// no-target: unknown child, unknown node
	expect(t, dyn.call("ghost.n1", "m", "", ""), apierr.NoTarget)
	expect(t, dyn.call("kit.elsewhere", "m", "", ""), apierr.NoTarget)
	// refused: the relay exists but has no such service
	expect(t, e.nodeSock("other").call("kit.n1", "m", "", ""), apierr.Refused)
	// no-service: child exists, nobody serves
	expect(t, dyn.call("host.n1", "m", "", ""), apierr.NoService)
	// version
	expect(t, dyn.post("/v1/service/call?to=kit.n1&method=m&version=2", ""), apierr.Version)
	// too-large
	expect(t, dyn.headerOnly("/v1/service/call?to=kit.n1&method=m&version=1", MaxCallBody+1), apierr.TooLarge)
	expectOK(t, dyn.call("kit.n1", "m", strings.Repeat("x", MaxCallBody-10), ""))
}

func TestAllow(t *testing.T) {
	y := `
node: n1
state_dir: DIR/state
services:
  dir: DIR/s
  list:
    - name: echo
      allow: [n1]
attach:
  dir: DIR/a
  default:
    services:
      - name: echo
        allow: ["*.n1"]
  children:
    - name: strict
      services:
        - name: echo
          allow: [other.n1]
`
	e := newEnv(t, y)
	e.mkChild("a")
	e.mkChild("b")
	e.mkChild("other")
	startEcho(t, e.nodeSock("echo"))
	startEcho(t, e.childSock("a", "echo"))
	startEcho(t, e.childSock("strict", "echo"))
	a, b, other := e.childSock("a", "echo"), e.childSock("b", "echo"), e.childSock("other", "echo")
	node := e.nodeSock("echo")

	expectOK(t, other.call("strict.n1", "m", "", "")) // exact id
	expect(t, a.call("strict.n1", "m", "", ""), apierr.Refused)
	expect(t, node.call("strict.n1", "m", "", ""), apierr.Refused)
	expectOK(t, b.call("a.n1", "m", "", "")) // *.n1: any child
	expectOK(t, other.call("a.n1", "m", "", ""))
	expect(t, node.call("a.n1", "m", "", ""), apierr.Refused) // *.n1 is not the node itself
	expectOK(t, node.call("n1", "m", "", ""))                 // exact node id (self call)
	expect(t, a.call("n1", "m", "", ""), apierr.Refused)
	// Refusal comes before the lease check: b has no lease and refuses a.
	expect(t, a.call("b.n1", "m", "", ""), apierr.NoService)
}

func TestOffline(t *testing.T) {
	e := newEnv(t, "")
	c := e.nodeSock("echo")
	expect(t, c.call("x.elsewhere", "m", "", ""), apierr.NoTarget)
	expect(t, c.call("alice@elsewhere", "m", "", ""), apierr.NoTarget)
}

func TestCallTimeoutAndNoPoll(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	_ = l
	node := e.nodeSock("echo")
	start := time.Now()
	// A live lease with no poll: the call is held until its deadline.
	expect(t, node.call("kit.n1", "m", "", "&timeout_ms=150"), apierr.Timeout)
	if d := time.Since(start); d < 120*time.Millisecond || d > 2*time.Second {
		t.Errorf("timeout took %v", d)
	}
}

func TestHeldCallIsDeliveredToTheNextPoll(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	node := e.nodeSock("echo")
	got := make(chan resp, 1)
	go func() { got <- node.call("kit.n1", "m", "payload", "") }()
	time.Sleep(80 * time.Millisecond)
	r := kit.get("/v1/service/calls?lease=" + l + "&wait=1")
	expectOK(t, r)
	if string(r.body) != "payload" {
		t.Fatalf("poll body %q", r.body)
	}
	expectOK(t, kit.post("/v1/service/answer?lease="+l+"&id="+r.hdr.Get("X-Warren-Call"), "done"))
	if a := <-got; string(a.body) != "done" {
		t.Errorf("answer %q", a.body)
	}
}

func TestAnswerRules(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	node := e.nodeSock("echo")
	got := make(chan resp, 1)
	go func() { got <- node.call("kit.n1", "m", "x", "&timeout_ms=400") }()
	r := kit.get("/v1/service/calls?lease=" + l + "&wait=25")
	expectOK(t, r)
	id := r.hdr.Get("X-Warren-Call")

	expect(t, kit.post("/v1/service/answer?lease=bad&id="+id, "a"), apierr.Gone)
	expect(t, kit.post("/v1/service/answer?lease="+l+"&id=bad", "a"), apierr.Gone)
	// The lease and id are checked before the body is read: an oversized
	// claim with a bad id is gone, not too-large.
	// Only the headers are sent: the server must answer without the body.
	expect(t, kit.headerOnly("/v1/service/answer?lease="+l+"&id=bad", 2000000), apierr.Gone)
	expect(t, kit.headerOnly("/v1/service/answer?lease="+l+"&id="+id, 2000000), apierr.TooLarge)
	expectOK(t, kit.post("/v1/service/answer?lease="+l+"&id="+id, "first"))
	expect(t, kit.post("/v1/service/answer?lease="+l+"&id="+id, "second"), apierr.Gone)
	if a := <-got; string(a.body) != "first" {
		t.Errorf("answer %q", a.body)
	}

	// An answer after the deadline is gone.
	go func() { got <- node.call("kit.n1", "m", "x", "&timeout_ms=100") }()
	r = kit.get("/v1/service/calls?lease=" + l + "&wait=25")
	expectOK(t, r)
	expect(t, <-got, apierr.Timeout)
	expect(t, kit.post("/v1/service/answer?lease="+l+"&id="+r.hdr.Get("X-Warren-Call"), "late"), apierr.Gone)
}

// ---- every way a lease ends, and what each pending call gets ----

// pending makes a held call and a handed-out one on kit's lease and returns
// their results channels. The lease must be given a poll first.
func pending(t *testing.T, e *env, kit *cl, l string) (handed, held chan resp) {
	t.Helper()
	node := e.nodeSock("echo")
	handed, held = make(chan resp, 1), make(chan resp, 1)
	go func() { handed <- node.call("kit.n1", "a", "", "&timeout_ms=20000") }()
	r := kit.get("/v1/service/calls?lease=" + l + "&wait=25")
	expectOK(t, r)
	go func() { held <- node.call("kit.n1", "b", "", "&timeout_ms=20000") }()
	time.Sleep(100 * time.Millisecond) // the second call is held, no poll is open
	return
}

func checkEnd(t *testing.T, handed, held chan resp) {
	t.Helper()
	select {
	case r := <-handed:
		expect(t, r, apierr.Unknown) // handed out: outcome lost
	case <-time.After(5 * time.Second):
		t.Fatal("handed call not ended")
	}
	select {
	case r := <-held:
		expect(t, r, apierr.NoService) // never handed out
	case <-time.After(5 * time.Second):
		t.Fatal("held call not ended")
	}
}

func TestLeaseEndCases(t *testing.T) {
	ends := map[string]func(e *env, kit *cl, l string){
		"release":  func(e *env, kit *cl, l string) { kit.post("/v1/service/release?lease="+l, "") },
		"takeover": func(e *env, kit *cl, l string) { kit.serve(1, true) },
		"exit":     func(e *env, kit *cl, l string) { e.node.ProcessExited("kit.n1", "echo") },
		"removed": func(e *env, kit *cl, l string) {
			next := parseYAML(t, e.dir, strings.Replace(e.yaml, "    - name: kit\n      services:\n        - name: echo\n          allow: [\"*\"]\n", "    - name: kit\n      services: []\n", 1))
			if err := e.node.Reload(next); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, end := range ends {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, "")
			kit := e.childSock("kit", "echo")
			l, _ := kit.serve(1, false)
			handed, held := pending(t, e, kit, l)
			end(e, kit, l)
			checkEnd(t, handed, held)
		})
	}
	t.Run("grace", func(t *testing.T) {
		e := newEnv(t, "")
		kit := e.childSock("kit", "echo")
		l, _ := kit.serve(1, false)
		// The handed call keeps the lease alive until its deadline, so use
		// a short one and let the grace run out after it.
		node := e.nodeSock("echo")
		held := make(chan resp, 1)
		go func() { held <- node.call("kit.n1", "b", "", "&timeout_ms=20000") }()
		time.Sleep(50 * time.Millisecond)
		// No poll was ever open and nothing was handed out: grace ends it.
		r := <-held
		expect(t, r, apierr.NoService)
		_ = l
	})
	t.Run("shutdown", func(t *testing.T) {
		e := newEnv(t, "")
		kit := e.childSock("kit", "echo")
		l, _ := kit.serve(1, false)
		handed, held := pending(t, e, kit, l)
		e.node.Close()
		checkEnd(t, handed, held)
	})
}

func TestHandedCallKeepsLeaseUntilItsDeadline(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	node := e.nodeSock("echo")
	got := make(chan resp, 1)
	go func() { got <- node.call("kit.n1", "a", "", "&timeout_ms=900") }()
	r := kit.get("/v1/service/calls?lease=" + l + "&wait=25")
	expectOK(t, r)
	// Longer than the grace, no poll open, but the call is still handed
	// out and within its deadline: the lease stays live.
	time.Sleep(LeaseGrace + 100*time.Millisecond)
	expect(t, kit.post("/v1/service/serve?version=1", ""), apierr.AlreadyServed)
	expectOK(t, kit.post("/v1/service/answer?lease="+l+"&id="+r.hdr.Get("X-Warren-Call"), "ok"))
	<-got
	// After the call is done, the grace starts.
	time.Sleep(LeaseGrace + 150*time.Millisecond)
	kit.serve(1, false)
}

func TestInflightAndPerRootPools(t *testing.T) {
	y := strings.Replace(baseYAML, "    - name: kit\n      services:\n        - name: echo\n          allow: [\"*\"]\n",
		"    - name: kit\n      services:\n        - name: echo\n          allow: [\"*\"]\n          inflight: 3\n          per_root: 2\n          outflight: 2\n", 1)
	e := newEnv(t, y, func() { LeaseGrace = 5 * time.Second })
	e.mkChild("a")
	e.mkChild("b")
	kit := e.childSock("kit", "echo")
	l, inflight := kit.serve(1, false)
	if inflight != 3 {
		t.Fatalf("inflight %d", inflight)
	}
	a, b := e.childSock("a", "echo"), e.childSock("b", "echo")
	var wg sync.WaitGroup
	results := make(chan resp, 8)
	placed := 0
	send := func(c *cl) {
		wg.Add(1)
		go func() { defer wg.Done(); results <- c.call("kit.n1", "m", "", "&timeout_ms=1500") }()
		// Go on once the call holds its place, not after a guessed delay.
		placed++
		eventually(t, "the call to hold a place", func() bool { return e.node.placesTaken("kit.n1", "echo") == placed })
	}
	// Per origin: a gets 2 places, the third is busy though the service has room.
	send(a)
	send(a)
	expect(t, a.call("kit.n1", "m", "", "&timeout_ms=1500"), apierr.Busy)
	// b still has a place, and uses the last one of 3.
	send(b)
	// Now the service's inflight is full for everyone.
	expect(t, b.call("kit.n1", "m", "", "&timeout_ms=1500"), apierr.Busy)
	_ = l
	wg.Wait()
	for i := 0; i < 3; i++ {
		expect(t, <-results, apierr.Timeout)
	}
}

// placesTaken is how many calls the lease on a socket holds or has handed out.
func (n *Node) placesTaken(relay, svc string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if s := n.socks[sockKey{relay, svc}]; s != nil && s.lease != nil {
		return len(s.lease.held) + len(s.lease.handed)
	}
	return 0
}

// outflightTaken is how many calls a socket has in flight as a caller.
func (n *Node) outflightTaken(relay, svc string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if s := n.socks[sockKey{relay, svc}]; s != nil {
		return s.outflight
	}
	return 0
}

func TestOutflight(t *testing.T) {
	y := strings.Replace(baseYAML, "    - name: kit\n      services:\n        - name: echo\n          allow: [\"*\"]\n",
		"    - name: kit\n      services:\n        - name: echo\n          allow: [\"*\"]\n          outflight: 1\n", 1)
	e := newEnv(t, y, func() { LeaseGrace = 5 * time.Second })
	host := e.childSock("host", "echo")
	host.serve(1, false)
	kit := e.childSock("kit", "echo")
	got := make(chan resp, 1)
	go func() { got <- kit.call("host.n1", "m", "", "&timeout_ms=400") }()
	eventually(t, "the call to take the outflight place", func() bool { return e.node.outflightTaken("kit.n1", "echo") == 1 })
	expect(t, kit.call("host.n1", "m", "", ""), apierr.Busy)
	expect(t, <-got, apierr.Timeout)
	// Place is free again.
	expect(t, kit.call("host.n1", "m", "", "&timeout_ms=50"), apierr.Timeout)
}

func TestCallerLeavingFreesHeldCall(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	node := e.nodeSock("echo")
	// Use a short client timeout: the HTTP client gives up, the held call goes.
	short := *node
	hc := *node.hc
	hc.Timeout = 100 * time.Millisecond
	short.hc = &hc
	short.t = t
	req := func() {
		defer func() { recover() }()
	}
	_ = req
	_, err := short.hc.Post("http://warren/v1/service/call?to=kit.n1&method=m&version=1", "", nil)
	_ = err
	time.Sleep(100 * time.Millisecond)
	// The abandoned call is not delivered.
	if r := kit.get("/v1/service/calls?lease=" + l + "&wait=0"); r.status != 204 {
		t.Errorf("abandoned call delivered: %d", r.status)
	}
}

func TestConcurrentAnswersForOneCallAreBusy(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	node := e.nodeSock("echo")
	got := make(chan resp, 1)
	go func() { got <- node.call("kit.n1", "m", "x", "&timeout_ms=10000") }()
	r := kit.get("/v1/service/calls?lease=" + l + "&wait=25")
	expectOK(t, r)
	id := r.hdr.Get("X-Warren-Call")
	answer := "/v1/service/answer?lease=" + l + "&id=" + id

	// The first answer promises a body and sends none: its read is open.
	conn, err := net.Dial("unix", kit.path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: x\r\nX-Warren: 1\r\nContent-Length: 10\r\n\r\n", answer)
	eventually(t, "the first answer to be marked", func() bool {
		e.node.mu.Lock()
		defer e.node.mu.Unlock()
		for _, c := range e.node.socks[sockKey{"kit.n1", "echo"}].lease.handed {
			return c.answering
		}
		return false
	})
	expect(t, kit.post(answer, "second"), apierr.Busy)
	// The first one fails: the mark is cleared and the call can be answered.
	conn.Close()
	eventually(t, "the mark to clear", func() bool {
		e.node.mu.Lock()
		defer e.node.mu.Unlock()
		for _, c := range e.node.socks[sockKey{"kit.n1", "echo"}].lease.handed {
			return !c.answering
		}
		return false
	})
	expectOK(t, kit.post(answer, "third"))
	if r := <-got; string(r.body) != "third" {
		t.Errorf("the caller got %q", r.body)
	}
}
