package conformance

import (
	"path/filepath"
	"time"
)

// leaseOf is whether the status of the node says the relay's service has a
// live lease.
func (e *env) leaseOf(node, relay string) bool {
	m := e.anyJSON(e.sock(node, "", "echo").do("GET", "/v1/service/status", nil))
	items, _ := m["services"].([]any)
	for _, it := range items {
		im, _ := it.(map[string]any)
		if im["relay"] == relay && im["name"] == "echo" {
			return im["lease"] == true
		}
	}
	e.t.Fatalf("status lists no echo at %s", relay)
	return false
}

// leaseGrace: docs/service-api.md, Leases, polls and answers. A lease stays live while it has a poll open and ends one
// lease grace after the last poll.
func (e *env) leaseGrace() {
	s, relay := e.fresh()
	g := e.tm.LeaseGrace
	l := s.serve(1, false)
	// A poll that has ended, and nothing else: the grace starts.
	l.poll(0)
	last := time.Now()
	time.Sleep(g / 4)
	if !e.leaseOf("n1", relay) {
		e.t.Errorf("the lease ended %v after its last poll, before the grace of %v", time.Since(last), g)
	}
	e.eventually(g+10*time.Second, "the lease to end after the grace", func() bool { return !e.leaseOf("n1", relay) })
	if d := time.Since(last); d < g*9/10 {
		e.t.Errorf("the lease ended %v after its last poll; the grace is %v", d, g)
	}
	// Its token is gone; the socket can be served again, and a call finds no lease.
	e.isErr(s.do("GET", "/v1/service/calls?"+l.q("&wait=0"), nil), "gone")
	caller, _ := e.fresh()
	e.isErr(caller.do("POST", callQ(relay, "m", 1, ""), nil), "no-service")

	// A lease with a poll open stays live for longer than the grace.
	l = s.serve(1, false)
	deadline := time.Now().Add(g + g/2 + 2*time.Second)
	for time.Now().Before(deadline) {
		l.poll(1)
	}
	if !e.leaseOf("n1", relay) {
		e.t.Error("a lease that keeps polling ended")
	}
	l.release()
}

// leaseEndCases: docs/service-api.md, Leases, polls and answers. What happens to calls, polls and entries when a lease
// ends: handed out calls fail with unknown, held ones with no-service, open
// polls answer gone. A handed out call keeps the lease live until its
// deadline.
func (e *env) leaseEndCases() {
	for _, how := range []string{"release", "takeover"} {
		end := func(target *sc, l *lease) {
			switch how {
			case "release":
				l.release()
			case "takeover":
				time.Sleep(e.tm.ServeInterval)
				target.serve(1, true)
			}
		}
		// A call handed out fails with unknown.
		target, trelay := e.fresh()
		caller, _ := e.fresh()
		l := target.serve(1, false)
		ch := caller.async("POST", callQ(trelay, "m", 1, "&timeout_ms=60000"), nil)
		l.mustPoll(10 * time.Second)
		end(target, l)
		e.isErr(e.wait(ch, 10*time.Second, how+": the handed out call"), "unknown")

		// A call held for a poll that never comes fails with no-service.
		target, trelay = e.fresh()
		l = target.serve(1, false)
		ch = caller.async("POST", callQ(trelay, "m", 1, "&timeout_ms=60000"), nil)
		time.Sleep(300 * time.Millisecond)
		end(target, l)
		e.isErr(e.wait(ch, 10*time.Second, how+": the held call"), "no-service")

		// An open poll answers gone.
		target, _ = e.fresh()
		l = target.serve(1, false)
		open := target.async("GET", "/v1/service/calls?"+l.q("&wait=20"), nil)
		time.Sleep(300 * time.Millisecond)
		end(target, l)
		e.isErr(e.wait(open, 10*time.Second, how+": the open poll"), "gone")
	}
	target, trelay := e.fresh()
	caller, _ := e.fresh()

	// A call handed out keeps the lease live, with no poll open, until its
	// deadline passes; then the grace runs.
	g := e.tm.LeaseGrace
	l := target.serve(1, false)
	ch := caller.async("POST", callQ(trelay, "m", 1, "&timeout_ms=60000"), nil)
	c := l.mustPoll(10 * time.Second)
	time.Sleep(g + g/2)
	if !e.leaseOf("n1", trelay) {
		e.t.Error("a lease with a call handed out ended")
	}
	e.status(l.answer(c.id, "late but in time"), 204)
	if r := e.wait(ch, 5*time.Second, "the answer"); r.status != 200 {
		e.t.Errorf("answer: %v", r)
	}
	e.eventually(g+10*time.Second, "the lease to end once the call was answered", func() bool { return !e.leaseOf("n1", trelay) })
	// A call handed out whose deadline passes no longer holds the lease.
	l = target.serve(1, false)
	ch = caller.async("POST", callQ(trelay, "m", 1, "&timeout_ms=300"), nil)
	l.mustPoll(10 * time.Second)
	e.isErr(e.wait(ch, 5*time.Second, "the timeout"), "timeout")
	e.eventually(g+10*time.Second, "the lease to end after the deadline and the grace", func() bool { return !e.leaseOf("n1", trelay) })
}

// staleEntries: docs/service-api.md, Entries and the view. Entries are stale from the end of the lease for the
// stale window, then dropped; a PUT within it revives them; a new lease has
// no list of its own until it PUTs.
func (e *env) staleEntries() {
	s, relay := e.fresh()
	viewer := e.sock("n2", "", "echo")
	addr := "keep@" + relay
	l := s.serve(1, false)
	l.putOK(`{"entries":[{"name":"keep"}]}`)
	state := func(sock *sc) string { // live, stale or absent
		en := sock.view("", 0).find(addr)
		switch {
		case en == nil:
			return "absent"
		case en.Stale:
			return "stale"
		}
		return "live"
	}
	both := func(want string) {
		e.t.Helper()
		e.eventually(15*time.Second+3*e.tm.Debounce, "the entry to be "+want, func() bool { return state(s) == want && state(viewer) == want })
	}
	both("live")
	l.release()
	both("stale")
	// A new lease starts with no list of its own: still stale until it PUTs.
	l = s.serve(1, false)
	time.Sleep(e.tm.PutInterval)
	if st := state(s); st != "stale" {
		e.t.Errorf("a new lease without a PUT: the entry is %s, want stale", st)
	}
	// A PUT within the stale window revives the same names.
	l.putOK(`{"entries":[{"name":"keep"}]}`)
	both("live")
	l.release()
	ended := time.Now()
	both("stale")
	e.eventually(e.tm.StaleFor+10*time.Second, "the stale entry to be dropped", func() bool { return state(s) == "absent" && state(viewer) == "absent" })
	if d := time.Since(ended); d < e.tm.StaleFor*9/10 {
		e.t.Errorf("the entry was dropped %v after the lease ended; it stays stale for %v", d, e.tm.StaleFor)
	}
}

// nodeDown: docs/service-api.md, Conventions and Entries and the view. A node that cannot be reached: calls to it are
// offline, and its entries are stale. Needs a launcher that can take a node
// down.
func (e *env) nodeDown() {
	if e.in.Down == nil {
		e.t.Skip("this launcher cannot make a node unreachable")
	}
	n2, _ := e.fresh() // a relay on n1 to call from
	pub := e.sock("n2", "k2", "echo")
	e.startEcho(pub, entryOf("pal", ""))
	e.eventually(15*time.Second+3*e.tm.Debounce, "pal@k2.n2 to be live on n1", func() bool {
		en := n2.view("", 0).find("pal@k2.n2")
		return en != nil && !en.Stale
	})
	e.in.Down("n2", true)
	defer e.in.Down("n2", false)
	e.eventually(15*time.Second, "pal@k2.n2 to go stale", func() bool {
		en := n2.view("", 0).find("pal@k2.n2")
		return en != nil && en.Stale
	})
	e.isErr(n2.do("POST", callQ("pal@k2.n2", "m", 1, ""), nil), "offline")
	e.isErr(n2.do("POST", callQ("k2.n2", "m", 1, ""), nil), "offline")
	e.in.Down("n2", false)
	e.eventually(15*time.Second+3*e.tm.Debounce, "pal@k2.n2 to be live again", func() bool {
		en := n2.view("", 0).find("pal@k2.n2")
		return en != nil && !en.Stale
	})
}

// restart: docs/service-api.md, Identity and status and Entries and the view. boot changes on every start; a lease from before is
// gone; the names a node service published are remembered, so after a
// restart a flat child cannot take them before the service has published.
func (e *env) restart(root string) {
	dir := filepath.Join(filepath.Dir(root), "restart")
	in := e.l.Launch(e.t, Topology(!e.real), dir, e.tm)
	sub := &env{t: e.t, shared: &shared{l: e.l, real: e.real, in: in, tm: e.tm, mu: e.mu, seen: e.seen}}
	node := sub.sock("n1", "", "echo")
	old := node.serve(1, false)
	old.putOK(`{"entries":[{"name":"boss"},{"name":"c1","namespace":"chat.n1"}],"reserved":["root"]}`)
	boot := sub.anyJSON(node.do("GET", "/v1/service/self", nil))["boot"]
	oldView := node.view("", 0).Version
	in.Stop()

	in = e.l.Launch(e.t, Topology(!e.real), dir, e.tm)
	sub = &env{t: e.t, shared: &shared{l: e.l, real: e.real, in: in, tm: e.tm, mu: e.mu, seen: e.seen}}
	node = sub.sock("n1", "", "echo")
	if b := sub.anyJSON(node.do("GET", "/v1/service/self", nil))["boot"]; b == boot || b == "" {
		e.t.Errorf("boot after a restart: %v, before: %v", b, boot)
	}
	// The old lease is gone, and a view from the old boot answers at once with everything.
	e.isErr(node.do("GET", "/v1/service/calls?"+old.q("&wait=0"), nil), "gone")
	start := time.Now()
	r := node.do("GET", "/v1/service/view?wait=5&since="+oldView, nil)
	if r.status != 200 || time.Since(start) > 2*time.Second {
		e.t.Errorf("a view since a version of the old boot: %v after %v", r.status, time.Since(start))
	}
	// The node service has not published yet; the flat child still cannot
	// take its names or its reservations.
	fa := sub.sock("n1", "fa", "echo")
	fl := fa.serve(1, false)
	got := fl.putOK(`{"entries":[{"name":"boss"},{"name":"root"},{"name":"mine"}]}`)
	if got["boss"] != "held-by-node" || got["root"] != "reserved" || len(got) != 2 {
		e.t.Errorf("after a restart the flat child's refusals are %v; the node's names are remembered", got)
	}
	// Reservations are part of the view as well.
	if v := fa.view("", 0); len(v.Reserved["n1"]) != 1 || v.Reserved["n1"][0] != "root" {
		e.t.Errorf("reserved after a restart: %v", v.Reserved)
	}
	// from is checked against them too.
	e.isErr(fa.do("POST", callQ("zzz.n1", "m", 1, "&from=boss"), nil), "refused")
	in.Stop()
}

// shutdown: docs/service-api.md, Leases, polls and answers. During shutdown /serve, /calls and /view answer not-ready;
// calls waiting for a poll fail with not-ready; a call handed out may still be
// answered.
func (e *env) shutdown(root string) {
	dir := filepath.Join(filepath.Dir(root), "shutdown")
	in := e.l.Launch(e.t, Topology(!e.real), dir, e.tm)
	sub := &env{t: e.t, shared: &shared{l: e.l, real: e.real, in: in, tm: e.tm, mu: e.mu, seen: e.seen}}
	target, trelay := sub.fresh()
	a, _ := sub.fresh()
	b, _ := sub.fresh()
	l := target.serve(1, false)
	handed := a.async("POST", callQ(trelay, "m", 1, "&timeout_ms=60000"), []byte("x"))
	c := l.mustPoll(10 * time.Second)
	waiting := b.async("POST", callQ(trelay, "m", 1, "&timeout_ms=60000"), []byte("y"))
	time.Sleep(300 * time.Millisecond)
	// The shutdown waits for the handed out call: that is the window.
	in.Shutdown()
	sub.eventually(e.tm.Drain, "calls to answer not-ready during shutdown", func() bool {
		r, err := target.try("GET", "/v1/service/calls?"+l.q("&wait=0"), nil)
		return err == nil && r.status == 503
	})
	e.isErr(target.do("GET", "/v1/service/calls?"+l.q("&wait=0"), nil), "not-ready")
	e.isErr(target.do("GET", "/v1/service/view?wait=0", nil), "not-ready")
	e.isErr(target.do("POST", "/v1/service/serve?version=1&take=1", nil), "not-ready")
	e.isErr(sub.wait(waiting, 10*time.Second, "the waiting call"), "not-ready")
	// The call handed out can still be answered during the drain.
	if r := l.answer(c.id, "done"); r.status != 204 {
		e.t.Errorf("an answer during shutdown: %v", r)
	}
	if r := sub.wait(handed, 10*time.Second, "the handed out call"); r.status != 200 || string(r.body) != "done" {
		e.t.Errorf("the call handed out before shutdown: %v", r)
	}
	in.Wait()
}

// heldCallsAndGrace: docs/service-api.md, Leases, polls and answers. A call held for a poll that never comes does not
// keep the lease alive: with no poll ever opened, the lease ends one grace
// after it started and the call fails with no-service.
func (e *env) heldCallsAndGrace() {
	target, trelay := e.fresh()
	caller, _ := e.fresh()
	g := e.tm.LeaseGrace
	l := target.serve(1, false)
	start := time.Now()
	ch := caller.async("POST", callQ(trelay, "m", 1, "&timeout_ms=60000"), nil)
	r := e.wait(ch, g+15*time.Second, "the held call to fail when the lease ran out")
	e.isErr(r, "no-service")
	if d := time.Since(start); d < g*8/10 {
		e.t.Errorf("the held call failed after %v, before the lease grace of %v: a held call must not end a lease early", d, g)
	}
	e.isErr(target.do("GET", "/v1/service/calls?"+l.q("&wait=0"), nil), "gone")
}
