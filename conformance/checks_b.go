package conformance

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// exchange makes a call from caller and has the leased target answer it.
// It returns the call as the target saw it and the answer the caller got.
func (e *env) exchange(caller *sc, tl *lease, to, method, extra, body, answer string, opts ...opt) (*call, *resp) {
	e.t.Helper()
	ch := caller.async("POST", callQ(to, method, 1, extra), []byte(body), opts...)
	c := tl.mustPoll(10 * time.Second)
	if a := tl.answer(c.id, answer); a.status != 204 {
		e.t.Fatalf("answer: %v", a)
	}
	return c, e.wait(ch, 10*time.Second, "the caller's answer")
}

// callsAndAnswers: docs/service-api.md, Leases, polls and answers and Calling. A call comes back as 200 with the raw body
// and the X-Warren-* headers; one without a poll is held until a poll
// comes; an unanswered call ends at its deadline.
func (e *env) callsAndAnswers() {
	target, trelay := e.fresh()
	caller, crelay := e.fresh()
	l := target.serve(1, false)
	defer l.release()

	// A poll is open when the call arrives.
	pollCh := target.async("GET", "/v1/service/calls?"+l.q("&wait=10"), nil)
	time.Sleep(200 * time.Millisecond)
	ch := caller.async("POST", callQ(trelay, "hello", 1, "&from=bob"), []byte("ping\x00\xff"))
	c := e.asCall(e.wait(pollCh, 10*time.Second, "the call to reach the open poll"))
	if c == nil || c.origin != crelay || c.from != "bob@"+crelay || c.to != trelay || c.method != "hello" || string(c.body) != "ping\x00\xff" {
		e.t.Fatalf("the call as handed out: %+v", c)
	}
	if c.deadlineMs < 25000 || c.deadlineMs > 30000 {
		e.t.Errorf("X-Warren-Deadline-Ms = %d, the default timeout is 30000", c.deadlineMs)
	}
	if ct := c.h.Get("Content-Type"); ct != "application/octet-stream" {
		e.t.Errorf("a handed-out call has Content-Type %q, want application/octet-stream", ct)
	}
	e.status(l.answer(c.id, "pong\x00\xfe"), 204)
	r := e.wait(ch, 10*time.Second, "the caller's answer")
	if ct := r.h.Get("Content-Type"); ct != "application/octet-stream" {
		e.t.Errorf("the caller's answer has Content-Type %q, want application/octet-stream", ct)
	}
	if r.status != 200 || string(r.body) != "pong\x00\xfe" {
		e.t.Errorf("the caller's answer: %v", r)
	}

	// No poll open: the call is held until one comes.
	ch = caller.async("POST", callQ(trelay, "later", 1, ""), []byte("held"))
	select {
	case r := <-ch:
		e.t.Fatalf("a held call was answered without a poll: %v", r)
	case <-time.After(400 * time.Millisecond):
	}
	c2 := l.mustPoll(5 * time.Second)
	if c2.method != "later" || string(c2.body) != "held" || c2.from != "" {
		e.t.Errorf("held call: %+v", c2)
	}
	if c2.id == c.id {
		e.t.Error("call ids must be unique per lease")
	}
	e.status(l.answer(c2.id, ""), 204)
	if r := e.wait(ch, 5*time.Second, "answer"); r.status != 200 || len(r.body) != 0 {
		e.t.Errorf("an empty answer: %v", r)
	}

	// A call to an entry is handed out with the full entry address.
	l.putOK(`{"entries":[{"name":"alice","data":{"k":1}}]}`)
	addr := "alice@" + trelay
	c3, r := e.exchange(caller, l, addr, "ent", "", "x", "y")
	if c3.to != addr || r.status != 200 || string(r.body) != "y" {
		e.t.Errorf("call to an entry: %+v %v", c3, r)
	}

	// Handed out and never answered: timeout at the deadline.
	start := time.Now()
	ch = caller.async("POST", callQ(trelay, "slow", 1, "&timeout_ms=400"), nil)
	c4 := l.mustPoll(5 * time.Second)
	if c4.deadlineMs > 400 {
		e.t.Errorf("deadline-ms %d for a 400 ms call", c4.deadlineMs)
	}
	r = e.wait(ch, 10*time.Second, "the timeout")
	e.isErr(r, "timeout")
	if d := time.Since(start); d < 300*time.Millisecond || d > 3*time.Second {
		e.t.Errorf("the timeout came after %v", d)
	}
	// Held and never polled: timeout too.
	r = caller.do("POST", callQ(trelay, "slow", 1, "&timeout_ms=300"), nil)
	e.isErr(r, "timeout")
	// The one that timed out is gone from the queue: the next poll gets nothing.
	if c := l.poll(0); c != nil {
		e.t.Errorf("a timed-out call was handed out: %+v", c)
	}
}

// answerRules: docs/service-api.md, Leases, polls and answers. /answer checks lease and id before reading the body,
// reads at most 1 MiB, a second answer or one after the deadline is gone,
// and a concurrent duplicate answer is busy.
func (e *env) answerRules() {
	target, trelay := e.fresh()
	caller, _ := e.fresh()
	l := target.serve(1, false)
	defer l.release()

	ch := caller.async("POST", callQ(trelay, "m", 1, ""), []byte("q"))
	c := l.mustPoll(10 * time.Second)

	// Lease and id are checked before the body is read: a wrong id is gone
	// at once, even though the body (announced, never sent) is huge.
	raw := fmt.Sprintf("POST /v1/service/answer?lease=%s&id=nonsense HTTP/1.1\r\nHost: w\r\nX-Warren: 1\r\nContent-Length: 5000000\r\nConnection: close\r\n\r\n", url.QueryEscape(l.Token))
	e.isErr(target.rawRequest(raw), "gone")
	// A body over 1 MiB is too-large, and the call is still there to answer.
	raw = fmt.Sprintf("POST /v1/service/answer?lease=%s&id=%s HTTP/1.1\r\nHost: w\r\nX-Warren: 1\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", url.QueryEscape(l.Token), c.id, 1<<20+1)
	e.isErr(target.rawRequest(raw), "too-large")

	// Another answer for the same call while one is being read is busy.
	conn := target.rawConn()
	body := strings.Repeat("a", 100)
	fmt.Fprintf(conn, "POST /v1/service/answer?lease=%s&id=%s HTTP/1.1\r\nHost: w\r\nX-Warren: 1\r\nContent-Length: %d\r\n\r\n%s", url.QueryEscape(l.Token), c.id, len(body), body[:10])
	time.Sleep(300 * time.Millisecond)
	e.isErr(l.answer(c.id, "second"), "busy")
	fmt.Fprint(conn, body[10:])
	if r := e.wait(ch, 10*time.Second, "the answer being read to finish"); r.status != 200 || string(r.body) != body {
		e.t.Errorf("the first answer should win: %v", r)
	}
	// A second answer for the same id is gone.
	e.isErr(l.answer(c.id, "again"), "gone")

	// After the deadline: gone, and the caller got timeout.
	ch = caller.async("POST", callQ(trelay, "m", 1, "&timeout_ms=300"), nil)
	c = l.mustPoll(10 * time.Second)
	time.Sleep(500 * time.Millisecond)
	e.isErr(l.answer(c.id, "late"), "gone")
	e.isErr(e.wait(ch, 5*time.Second, "the timeout"), "timeout")
}

// giveUp: docs/service-api.md, Leases, polls and answers. Answer with fail=1
// and no body gives a handed-out call up: the caller gets unknown at once and
// the place is free again. Anything else about fail is bad-request, and the
// lease and id are checked as for an answer.
func (e *env) giveUp() {
	target, trelay := e.fresh()
	caller, _ := e.fresh()
	l := target.serve(1, false)
	defer l.release()

	// More calls than the target has places (4 for a template child): each
	// is given up, ends as unknown quickly, and frees its place, so none is
	// ever busy.
	for i := 0; i < 8; i++ {
		ch := caller.async("POST", callQ(trelay, "m", 1, "&timeout_ms=60000"), []byte("q"))
		c := l.mustPoll(10 * time.Second)
		if r := l.fail(c.id); r.status != 204 {
			e.t.Fatalf("fail answered %v", r)
		}
		started := time.Now()
		e.isErr(e.wait(ch, 5*time.Second, "the caller to be told the call was given up"), "unknown")
		if d := time.Since(started); d > 3*time.Second {
			e.t.Errorf("the caller waited %v for a call that was given up", d)
		}
		// The call has ended: a second give-up or an answer is gone.
		e.isErr(l.fail(c.id), "gone")
		e.isErr(l.answer(c.id, "late"), "gone")
	}
	// Only fail=1, and no body.
	ch := caller.async("POST", callQ(trelay, "m", 1, "&timeout_ms=60000"), []byte("q"))
	c := l.mustPoll(10 * time.Second)
	e.isErr(l.s.do("POST", "/v1/service/answer?"+l.q("&id="+url.QueryEscape(c.id)+"&fail=2"), nil), "bad-request")
	e.isErr(l.s.do("POST", "/v1/service/answer?"+l.q("&id="+url.QueryEscape(c.id)+"&fail=1"), []byte("body")), "bad-request")
	e.isErr(l.fail("nonsense"), "gone")
	// The call is still open after those: it can still be answered.
	if r := l.answer(c.id, "ok"); r.status != 204 {
		e.t.Errorf("answer after bad give-ups: %v", r)
	}
	if r := e.wait(ch, 5*time.Second, "the answer"); r.status != 200 || string(r.body) != "ok" {
		e.t.Errorf("the caller got %v", r)
	}
}

// checkOrder: docs/service-api.md, Conventions. At the target the checks run in this order: service
// exists, allow, live lease, version, target entry.
func (e *env) checkOrder() {
	kit := e.sock("n1", "kit", "echo")
	fa := e.sock("n1", "fa", "echo")
	only := e.sock("n1", "only", "echo") // allow: kit.n1 only
	call := func(from *sc, to string, version int) *resp { return from.do("POST", callQ(to, "m", version, ""), nil) }

	// 1. The service must exist at the target: kit/locked calling fa.n1,
	// which has no locked service.
	e.isErr(call(e.sock("n1", "kit", "locked"), "fa.n1", 1), "refused")
	// 2. Allow comes before the lease, the version and the entry.
	e.isErr(call(fa, "only.n1", 1), "refused")
	e.isErr(call(fa, "only.n1", 9), "refused")
	e.isErr(call(fa, "ghost@only.n1", 1), "refused")
	// 3. A live lease comes before the version and the entry.
	e.isErr(call(kit, "only.n1", 9), "no-service")
	e.isErr(call(kit, "ghost@only.n1", 9), "no-service")
	e.isErr(call(kit, "ghost@only.n1", 1), "no-service")
	l := only.serve(1, false)
	defer l.hold()()
	// 4. The version comes before the entry.
	e.isErr(call(kit, "only.n1", 2), "version")
	e.isErr(call(kit, "ghost@only.n1", 2), "version")
	// 5. Last, the entry.
	e.isErr(call(kit, "ghost@only.n1", 1), "no-target")
	// Targets that are not known at all.
	e.isErr(call(kit, "zzz.n1", 1), "no-target")
	e.isErr(call(kit, "x@zzz.n1", 1), "no-target")
	e.isErr(call(kit, "x@nowhere", 1), "no-target")
	e.isErr(call(kit, "zzz.nowhere", 1), "no-target")
	// Allow patterns: "*.n1" admits every child of n1, not the node itself.
	wild := e.sock("n1", "wild", "echo")
	wl := wild.serve(1, false)
	defer wl.hold()()
	node := e.sock("n1", "", "echo")
	e.isErr(call(node, "wild.n1", 1), "refused")
	e.isErr(call(node, "wild.n1", 2), "refused")
	e.isErr(call(fa, "wild.n1", 2), "version") // allowed: a child of n1
	// Not a place at all.
	e.isErr(call(kit, "NOT A RELAY", 1), "bad-request")
	e.isErr(call(kit, "a@b@c", 1), "bad-request")
	e.isErr(call(kit, "@n1", 1), "bad-request")
	e.isErr(call(kit, "Bad.n1", 1), "bad-request")
}

// callParameters: docs/service-api.md, Calling.
func (e *env) callParameters() {
	s, relay := e.fresh()
	t, trelay := e.fresh()
	tl := t.serve(1, false)
	defer tl.release()
	_ = relay
	for _, m := range []string{"", "UPPER", "has space", "-lead", strings.Repeat("a", 64), "a.b"} {
		e.isErr(s.do("POST", "/v1/service/call?to="+trelay+"&version=1&method="+url.QueryEscape(m), nil), "bad-request")
	}
	// The longest method is 63 bytes.
	c, r := e.exchange(s, tl, trelay, strings.Repeat("m", 63), "", "", "ok")
	if r.status != 200 || len(c.method) != 63 {
		e.t.Errorf("a 63 byte method: %v", r)
	}
	e.isErr(s.do("POST", "/v1/service/call?method=m&version=1", nil), "bad-request")
	e.isErr(s.do("POST", callQ(trelay, "m", 1, "&from=Bad%20Name"), nil), "bad-request")
	e.isErr(s.do("POST", callQ(trelay, "m", 1, "&from=a@b@c"), nil), "bad-request")
	e.isErr(s.do("POST", callQ(trelay, "m", 1, "&from=x@"), nil), "bad-request")
}

// callLimits: docs/service-api.md, Calling, and docs/architecture.md, Limits and fairness. outflight is taken before the body is read, inflight
// bounds held and handed calls, per_root bounds them per origin; bodies are
// at most 1 MiB.
func (e *env) callLimits() {
	// The target has inflight 4 and per_root 2 (the template); its lease has
	// no poll open, so every accepted call is held.
	target, trelay := e.fresh()
	tl := target.serve(1, false)
	var held []<-chan *resp
	start := func(from *sc) {
		held = append(held, from.async("POST", callQ(trelay, "m", 1, "&timeout_ms=60000"), []byte("x")))
		time.Sleep(50 * time.Millisecond)
	}
	fa, fb := e.sock("n1", "fa", "echo"), e.sock("n1", "fb", "echo")
	kit, pat := e.sock("n1", "kit", "echo"), e.sock("n1", "pat", "echo")
	start(fa)
	start(fa)
	// A third call of the same origin: per_root is 2.
	e.isErr(fa.do("POST", callQ(trelay, "m", 1, ""), []byte("x")), "busy")
	// Other origins have their own share, until inflight (4) is full.
	start(fb)
	start(kit)
	e.isErr(pat.do("POST", callQ(trelay, "m", 1, ""), []byte("x")), "busy")
	// A busy caller was told so before its call was held: nothing was queued
	// for it, and the held ones end with the lease.
	tl.release()
	for i, ch := range held {
		e.isErr(e.wait(ch, 5*time.Second, fmt.Sprintf("held call %d to end with the lease", i)), "no-service")
	}

	// Handed-out calls count too. Two origins each have a call handed out and
	// left unanswered and a second one held (inflight 4 is full, per_root 2 is
	// reached for each); a third call of either origin, and a call of a fifth
	// origin, are busy.
	t4, t4relay := e.fresh()
	l4 := t4.serve(1, false)
	var calls []<-chan *resp
	callFrom := func(from *sc) {
		calls = append(calls, from.async("POST", callQ(t4relay, "m", 1, "&timeout_ms=60000"), []byte("x")))
		time.Sleep(50 * time.Millisecond)
	}
	callFrom(fa)
	l4.mustPoll(10 * time.Second) // handed out, never answered
	callFrom(fb)
	l4.mustPoll(10 * time.Second)
	callFrom(fa) // held: no poll is open
	callFrom(fb)
	e.isErr(fa.do("POST", callQ(t4relay, "m", 1, ""), []byte("x")), "busy")  // per_root: one handed out, one held
	e.isErr(kit.do("POST", callQ(t4relay, "m", 1, ""), []byte("x")), "busy") // inflight: two handed out, two held
	l4.release()
	var codes []string
	for _, ch := range calls {
		r := e.wait(ch, 5*time.Second, "a call to end with the lease")
		codes = append(codes, r.h.Get("X-Warren-Error"))
	}
	if n := strings.Count(strings.Join(codes, " "), "unknown"); n != 2 || strings.Count(strings.Join(codes, " "), "no-service") != 2 {
		e.t.Errorf("the lease ended: handed out calls fail with unknown, held ones with no-service; got %v", codes)
	}

	// outflight: the node's echo socket has 2. Its calls wait for a poll.
	t2, t2relay := e.fresh()
	l2 := t2.serve(1, false)
	node := e.sock("n1", "", "echo")
	held = nil
	for i := 0; i < 2; i++ {
		held = append(held, node.async("POST", callQ(t2relay, "m", 1, "&timeout_ms=60000"), []byte("x")))
		time.Sleep(50 * time.Millisecond)
	}
	e.isErr(node.do("POST", callQ(t2relay, "m", 1, ""), []byte("x")), "busy")
	// It is taken before the body is read: busy even for an announced body that never comes.
	raw := fmt.Sprintf("POST %s HTTP/1.1\r\nHost: w\r\nX-Warren: 1\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n", callQ(t2relay, "m", 1, ""))
	e.isErr(node.rawRequest(raw), "busy")
	l2.release()
	for _, ch := range held {
		e.isErr(e.wait(ch, 5*time.Second, "held call to end"), "no-service")
	}

	// Bodies: 1 MiB goes both ways, one byte more does not. The target is
	// looked up before the body is read, so an unknown target is no-target
	// whatever the body says.
	t3, t3relay := e.fresh()
	l3 := t3.serve(1, false)
	defer l3.release()
	caller, _ := e.fresh()
	big := strings.Repeat("b", 1<<20)
	c, r := e.exchange(caller, l3, t3relay, "big", "", big, big)
	if r.status != 200 || len(r.body) != 1<<20 || len(c.body) != 1<<20 {
		e.t.Errorf("a 1 MiB call and answer: %v", r.status)
	}
	head := fmt.Sprintf("POST %s HTTP/1.1\r\nHost: w\r\nX-Warren: 1\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", callQ(t3relay, "big", 1, ""), 1<<20+1)
	e.isErr(caller.rawWithBody(head, []byte(big+"x")), "too-large")
	raw = fmt.Sprintf("POST %s HTTP/1.1\r\nHost: w\r\nX-Warren: 1\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", callQ("zzz.n1", "m", 1, ""), 1<<20+1)
	e.isErr(caller.rawRequest(raw), "no-target")
}

// fromChecks: docs/service-api.md, Calling. from is any name the socket may publish: in its entry
// namespace or one of its namespaces, not held by an earlier publisher, not
// reserved by the node, matching the child's name patterns. It need not be
// listed yet.
func (e *env) fromChecks() {
	target, trelay := e.fresh()
	tl := target.serve(1, false)
	defer tl.release()
	ok := func(from *sc, f, want string) {
		e.t.Helper()
		c, r := e.exchange(from, tl, trelay, "m", "&from="+url.QueryEscape(f), "", "ok")
		if r.status != 200 || c.from != want {
			e.t.Errorf("from=%s on %s: handed out as %q (%v), want %q", f, from.name, c.from, r.status, want)
		}
	}
	refused := func(from *sc, f string) {
		e.t.Helper()
		e.isErr(from.do("POST", callQ(trelay, "m", 1, "&from="+url.QueryEscape(f)), nil), "refused")
	}
	node := e.sock("n1", "", "echo")
	fa, fb := e.sock("n1", "fa", "echo"), e.sock("n1", "fb", "echo")
	kit, pat := e.sock("n1", "kit", "echo"), e.sock("n1", "pat", "echo")

	// The node: its namespace and its service namespaces.
	ok(node, "x", "x@n1")
	ok(node, "x@n1", "x@n1")
	ok(node, "y@chat.n1", "y@chat.n1")
	refused(node, "x@other.n1")
	refused(node, "x@kit.n1")
	// A child: its own namespace only. A flat child: the node's.
	ok(kit, "me", "me@kit.n1")
	ok(kit, "me@kit.n1", "me@kit.n1")
	refused(kit, "me@n1")
	refused(kit, "me@chat.n1")
	ok(fa, "me", "me@n1")
	refused(fa, "me@chat.n1") // service namespaces belong to the node's socket
	refused(fa, "me@fa.n1")
	// Name patterns of the child.
	ok(pat, "ok-1", "ok-1@pat.n1")
	refused(pat, "bad")

	// Held by the node, reserved by the node, held by an earlier flat child.
	nl := node.serve(1, true)
	defer nl.hold()()
	nl.putOK(`{"entries":[{"name":"boss"}],"reserved":["root"]}`)
	refused(fa, "boss")
	refused(fb, "root")
	ok(node, "boss", "boss@n1")
	ok(node, "root", "root@n1")
	ok(fa, "fine", "fine@n1")
	fl := fa.serve(1, false)
	defer fl.hold()()
	fl.putOK(`{"entries":[{"name":"shared"}]}`)
	ok(fa, "shared", "shared@n1")
	refused(fb, "shared")
	// A name that is not yet listed may still send.
	ok(fb, "not-listed-yet", "not-listed-yet@n1")
}

// onlyTheBody: docs/service-api.md, Conventions. A caller's request headers never reach the server, and
// the server's answer reaches the caller as a body only.
func (e *env) onlyTheBody() {
	target, trelay := e.fresh()
	caller, crelay := e.fresh()
	tl := target.serve(1, false)
	defer tl.release()
	ch := caller.async("POST", callQ(trelay, "m", 1, ""), []byte("b"),
		header("X-Custom", "leak"), header("Authorization", "Bearer x"), header("X-Warren-Origin", "forged"),
		header("X-Warren-From", "forged@n1"), header("X-Warren-Method", "forged"), header("X-Warren-Call", "forged"), header("Cookie", "a=b"))
	c := tl.mustPoll(10 * time.Second)
	if c.origin != crelay || c.method != "m" || c.from != "" || c.id == "forged" {
		e.t.Errorf("a caller forged a header: %+v", c)
	}
	for _, k := range []string{"X-Custom", "Authorization", "Cookie"} {
		if c.h.Get(k) != "" {
			e.t.Errorf("the caller's header %s reached the server", k)
		}
	}
	// The server's request headers do not shape the caller's answer either.
	r := tl.s.do("POST", "/v1/service/answer?"+tl.q("&id="+c.id), []byte("answer"),
		header("X-Warren-Error", "busy"), header("X-Custom", "leak"), header("Content-Type", "text/x-weird"))
	e.status(r, 204)
	got := e.wait(ch, 5*time.Second, "the answer")
	if got.status != 200 || string(got.body) != "answer" || got.h.Get("X-Warren-Error") != "" || got.h.Get("X-Custom") != "" {
		e.t.Errorf("the caller's answer: %v %v", got, got.h)
	}
}

// crossNode: docs/service-api.md, Calling, and docs/architecture.md, Peer protocol. Calls and entries between two nodes, with the
// target's checks made on its node.
func (e *env) crossNode() {
	n2 := e.sock("n2", "", "echo")
	e.startEcho(n2, entryOf("zed", `{"n":2}`))
	fa := e.sock("n1", "fa", "echo")
	var r *resp
	e.eventually(15*time.Second+3*e.tm.Debounce, "n2's service to answer a call from n1", func() bool {
		r = fa.do("POST", callQ("zed@n2", "hi", 1, "&from=bob"), []byte("data"))
		return r.status == 200
	})
	if want := "origin=fa.n1 from=bob@n1 to=zed@n2 method=hi body=data"; string(r.body) != want {
		e.t.Errorf("answer %q, want %q", r.body, want)
	}
	r = fa.do("POST", callQ("n2", "hi", 1, ""), []byte("d"))
	if r.status != 200 || !strings.HasPrefix(string(r.body), "origin=fa.n1 from= to=n2 ") {
		e.t.Errorf("call to the node: %v", r)
	}
	// The remote node's codes come back as they are.
	e.isErr(fa.do("POST", callQ("zed@n2", "hi", 9, ""), nil), "version")
	e.isErr(fa.do("POST", callQ("ghost@n2", "hi", 1, ""), nil), "no-target")
	e.isErr(fa.do("POST", callQ("zzz.n2", "hi", 1, ""), nil), "no-target")
	e.isErr(fa.do("POST", callQ("k2.n2", "hi", 1, ""), nil), "no-service")
	// allow is decided on the origin, on the target's node: n2's locked has
	// no allow, n1's locked allows exactly n2.
	e.isErr(e.sock("n1", "", "locked").do("POST", callQ("n2", "m", 1, ""), nil), "refused")
	e.isErr(e.sock("n2", "", "locked").do("POST", callQ("n1", "m", 1, ""), nil), "no-service")
	e.isErr(e.sock("n2", "", "echo").do("POST", callQ("wild.n1", "m", 1, ""), nil), "refused") // *.n1 is not n2
	// The entry is in n1's view with its origin, and n1's own entries are in n2's.
	e.eventually(10*time.Second, "zed@n2 in n1's view", func() bool {
		en := fa.view("", 0).find("zed@n2")
		return en != nil && en.Origin == "n2" && string(en.Data) == `{"n":2}` && !en.Stale
	})
	// Entries are visible whatever allow says: n1's locked publishes, n2's
	// locked (which admits nobody) sees it.
	nl := e.sock("n1", "", "locked")
	l := nl.serve(1, true)
	defer l.hold()()
	l.putOK(`{"entries":[{"name":"hidden"}]}`)
	e.eventually(10*time.Second, "hidden@n1 in n2's locked view", func() bool {
		en := e.sock("n2", "", "locked").view("", 0).find("hidden@n1")
		return en != nil && en.Origin == "n1"
	})
}

// crossNodeLeases: docs/service-api.md, Leases, polls and answers and Calling, across nodes. A lease on n2 polled by hand
// shows the deadline a call arrives with, the timeout, and what a lease that
// ends does to calls from n1: unknown for one handed out, no-service for one
// held.
func (e *env) crossNodeLeases() {
	target := e.sock("n2", "k2", "echo")
	l := target.serve(1, true)
	defer l.hold()() // released at the end, before the timers group runs
	caller := e.sock("n1", "fa", "echo")
	e.eventually(15*time.Second+3*e.tm.Debounce, "n1 to see n2", func() bool {
		r := caller.do("POST", callQ("k2.n2", "m", 1, "&timeout_ms=100"), nil)
		return r.status == 504 || r.status == 200 || r.h.Get("X-Warren-Error") != "offline" && r.h.Get("X-Warren-Error") != "no-target"
	})
	l.release()
	l = target.serve(1, true)
	// The deadline is carried: what arrives is at most what was asked.
	ch := caller.async("POST", callQ("k2.n2", "m", 1, "&timeout_ms=400"), []byte("x"))
	c := l.mustPoll(10 * time.Second)
	if c.deadlineMs < 1 || c.deadlineMs > 400 || c.origin != "fa.n1" {
		e.t.Errorf("a call from another node: deadline-ms %d (asked 400), origin %q", c.deadlineMs, c.origin)
	}
	e.isErr(e.wait(ch, 10*time.Second, "the timeout"), "timeout")
	// Handed out, then the lease ends: unknown.
	ch = caller.async("POST", callQ("k2.n2", "m", 1, "&timeout_ms=60000"), nil)
	l.mustPoll(10 * time.Second)
	l.release()
	e.isErr(e.wait(ch, 10*time.Second, "unknown after release"), "unknown")
	// Held (no poll), then the lease ends: no-service.
	l = target.serve(1, false)
	ch = caller.async("POST", callQ("k2.n2", "m", 1, "&timeout_ms=60000"), nil)
	time.Sleep(300 * time.Millisecond)
	l.release()
	e.isErr(e.wait(ch, 10*time.Second, "no-service after release"), "no-service")
	e.isErr(caller.do("POST", callQ("k2.n2", "m", 1, ""), nil), "no-service")
}
