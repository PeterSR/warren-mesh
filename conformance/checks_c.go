package conformance

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// entriesPut: docs/service-api.md, Entries and the view. Only the lease holder may PUT; a PUT replaces the
// previous list; the rate is one per interval.
func (e *env) entriesPut() {
	s, relay := e.fresh()
	e.isErr(s.do("PUT", "/v1/service/entries", []byte(`{"entries":[]}`)), "bad-request")
	e.isErr(s.do("PUT", "/v1/service/entries?lease=nonsense", []byte(`{"entries":[]}`)), "gone")
	l := s.serve(1, false)
	stop := l.hold()
	r := l.put(`{"entries":[{"name":"a","data":{"x":[1,2]}},{"name":"b"}]}`)
	e.status(r, 200)
	if strings.ReplaceAll(string(r.body), " ", "") != `{"refused":[]}` {
		e.t.Errorf("a clean PUT answers %q, want {\"refused\":[]}", r.body)
	}
	v := s.view("", 0)
	if a := v.find("a@" + relay); a == nil || a.Origin != relay || string(a.Data) != `{"x":[1,2]}` || a.Stale {
		e.t.Errorf("entry a: %+v", a)
	}
	if b := v.find("b@" + relay); b == nil || strings.TrimSpace(string(b.Data)) != "null" {
		e.t.Errorf("entry b (no data) should have data null: %+v", b)
	}
	// While one PUT body is being read, another is busy (checked after the
	// rate interval, so it is the body being read that refuses it).
	time.Sleep(e.tm.PutInterval + 100*time.Millisecond)
	conn := s.rawConn()
	fmt.Fprintf(conn, "PUT /v1/service/entries?%s HTTP/1.1\r\nHost: w\r\nX-Warren: 1\r\nContent-Length: 14\r\nConnection: close\r\n\r\n{\"ent", l.q(""))
	time.Sleep(e.tm.PutInterval + 200*time.Millisecond)
	e.isErr(s.do("PUT", "/v1/service/entries?"+l.q(""), []byte(`{"entries":[]}`)), "busy")
	fmt.Fprint(conn, `ries":[]}`)
	if hr, err := http.ReadResponse(bufioReader(conn), nil); err != nil || hr.StatusCode != 200 {
		e.t.Errorf("the PUT being read should complete: %v", err)
	}
	time.Sleep(e.tm.PutInterval + 50*time.Millisecond)
	l.putOK(`{"entries":[{"name":"c"}]}`)
	// Too soon after the last PUT.
	e.isErr(s.do("PUT", "/v1/service/entries?"+l.q(""), []byte(`{"entries":[]}`)), "busy")
	// A PUT replaces the list.
	l.putOK(`{"entries":[{"name":"c"}]}`)
	e.eventually(5*time.Second, "the old entries to go", func() bool {
		v := s.view("", 0)
		return v.find("a@"+relay) == nil && v.find("b@"+relay) == nil && v.find("c@"+relay) != nil
	})
	// An empty list publishes nothing.
	l.putOK(`{"entries":[]}`)
	e.eventually(5*time.Second, "an empty list to remove entries", func() bool { return s.view("", 0).find("c@"+relay) == nil })
	// After the lease ends, its token is gone for PUT.
	stop()
	e.isErr(s.do("PUT", "/v1/service/entries?"+l.q(""), []byte(`{"entries":[]}`)), "gone")
}

// entriesLimits: docs/service-api.md, Entries and the view. At most 1000 entries and 64 reserved names, data at most
// 3 KiB, body at most 4 MiB, no duplicates.
func (e *env) entriesLimits() {
	s, _ := e.fresh()
	l := s.serve(1, false)
	defer l.hold()()
	bad := func(what, body string) {
		e.t.Helper()
		if r := l.put(body); !e.isErr(r, "bad-request") {
			e.t.Logf("%s", what)
		}
	}
	bad("not JSON", `{"entries":`)
	bad("not an object", `[]`)
	bad("entries not a list", `{"entries":{}}`)
	bad("duplicate names", `{"entries":[{"name":"a"},{"name":"a"}]}`)
	bad("a bad reserved name", `{"entries":[],"reserved":["Bad Name"]}`)
	var many []string
	for i := 0; i < 1001; i++ {
		many = append(many, fmt.Sprintf(`{"name":"e%d"}`, i))
	}
	bad("1001 entries", `{"entries":[`+strings.Join(many, ",")+`]}`)
	var res []string
	for i := 0; i < 65; i++ {
		res = append(res, fmt.Sprintf(`"r%d"`, i))
	}
	bad("65 reserved names", `{"entries":[],"reserved":[`+strings.Join(res, ",")+`]}`)
	bad("data over 3 KiB", `{"entries":[{"name":"a","data":"`+strings.Repeat("x", 3071)+`"}]}`)
	bad("data that is not JSON", `{"entries":[{"name":"a","data":nope}]}`)
	// Exactly 3 KiB of data is fine, as are 1000 entries (the rest is cut by
	// the child's max_entries, which is its business) and 64 reserved names
	// (a child's are refused as not-allowed).
	e.putFine(l, `{"entries":[{"name":"a","data":"`+strings.Repeat("x", 3070)+`"}]}`)
	e.putFine(l, `{"entries":[`+strings.Join(many[:1000], ",")+`]}`)
	e.putFine(l, `{"entries":[],"reserved":[`+strings.Join(res[:64], ",")+`]}`)
	// Over 4 MiB is too-large, decided from the Content-Length before the
	// body is read.
	time.Sleep(e.tm.PutInterval + 50*time.Millisecond)
	raw := fmt.Sprintf("PUT /v1/service/entries?lease=%s HTTP/1.1\r\nHost: w\r\nX-Warren: 1\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", url.QueryEscape(l.Token), 4<<20+1)
	e.isErr(s.rawRequest(raw), "too-large")
}

func (e *env) putFine(l *lease, body string) {
	e.t.Helper()
	if r := l.put(body); r.status != 200 {
		e.t.Errorf("PUT should be accepted: %v", r)
	}
}

// entriesPrecedence: docs/architecture.md, Entries, and docs/service-api.md, Entries and the view. In the node's namespace the node's names
// and reservations come first, then flat children in config order; the
// displaced entries are in lost, and come back when the earlier one goes.
func (e *env) entriesPrecedence() {
	node := e.sock("n1", "", "echo")
	fa, fb := e.sock("n1", "fa", "echo"), e.sock("n1", "fb", "echo")
	nl := node.serve(1, true)
	defer nl.hold()()
	nl.putOK(`{"entries":[{"name":"boss","data":1},{"name":"c1","namespace":"chat.n1"}],"reserved":["root"]}`)
	fl := fa.serve(1, true)
	defer fl.hold()()
	got := fl.putOK(`{"entries":[{"name":"boss"},{"name":"root"},{"name":"dup","data":"fa"},{"name":"fa1"},{"name":"c2","namespace":"chat.n1"}]}`)
	if got["boss"] != "held-by-node" || got["root"] != "reserved" || got["c2"] != "namespace" || len(got) != 3 {
		e.t.Errorf("fa's refusals: %v", got)
	}
	bl := fb.serve(1, true)
	defer bl.hold()()
	got = bl.putOK(`{"entries":[{"name":"dup","data":"fb"},{"name":"fb1"}]}`)
	if got["dup"] != "held-by-earlier-child" || len(got) != 1 {
		e.t.Errorf("fb's refusals: %v", got)
	}
	var v *view
	e.eventually(5*time.Second, "the merged view", func() bool {
		v = fb.view("", 0)
		return v.find("fb1@n1") != nil && v.find("fa1@n1") != nil && v.find("boss@n1") != nil
	})
	if en := v.find("boss@n1"); en.Origin != "n1" {
		e.t.Errorf("boss is the node's: %+v", en)
	}
	if en := v.find("dup@n1"); en == nil || en.Origin != "fa.n1" || string(en.Data) != `"fa"` {
		e.t.Errorf("dup belongs to the earlier flat child: %+v", en)
	}
	if en := v.find("c1@chat.n1"); en == nil || en.Origin != "n1" {
		e.t.Errorf("a service namespace entry: %+v", en)
	}
	if v.find("root@n1") != nil || v.find("c2@chat.n1") != nil {
		e.t.Error("a reserved name or a foreign namespace was published")
	}
	if got := v.Reserved["n1"]; len(got) != 1 || got[0] != "root" {
		e.t.Errorf("reserved = %v, want {n1: [root]}", v.Reserved)
	}
	if !v.Ready {
		e.t.Error("view.ready is false")
	}
	if v.lost("dup") != "held-by-earlier-child" || len(v.Lost) != 1 {
		e.t.Errorf("fb's lost: %+v", v.Lost)
	}
	if fv := fa.view("", 0); fv.lost("boss") != "held-by-node" || fv.lost("root") != "reserved" || fv.lost("c2") != "" {
		e.t.Errorf("fa's lost: %+v", fv.Lost)
	}
	// Precedence is config and names, never presence: when fa drops dup, fb's
	// comes back without a new PUT.
	fl.putOK(`{"entries":[{"name":"fa1"}]}`)
	e.eventually(5*time.Second, "fb's dup to come back", func() bool {
		v := fb.view("", 0)
		en := v.find("dup@n1")
		return en != nil && en.Origin == "fb.n1" && len(v.Lost) == 0
	})
	// And when the node publishes the name, it goes the other way.
	nl.putOK(`{"entries":[{"name":"boss"},{"name":"dup"}],"reserved":["root"]}`)
	e.eventually(5*time.Second, "the node to take dup", func() bool {
		v := fb.view("", 0)
		en := v.find("dup@n1")
		return en != nil && en.Origin == "n1" && v.lost("dup") == "held-by-node"
	})
}

// entriesReasons: docs/service-api.md, Entries and the view. The reasons of a refusal: namespace, grammar, cut,
// not-allowed (the others are in entriesPrecedence).
func (e *env) entriesReasons() {
	pat := e.sock("n1", "pat", "echo")
	pl := pat.serve(1, true)
	defer pl.hold()()
	got := pl.putOK(`{"entries":[{"name":"ok-1"},{"name":"nope"},{"name":"ok-2","namespace":"n1"}],"reserved":["x"]}`)
	if got["nope"] != "not-allowed" || got["x"] != "not-allowed" || got["ok-2"] != "namespace" || len(got) != 3 {
		e.t.Errorf("pat: %v", got)
	}
	if v := pat.view("", 0); v.find("ok-1@pat.n1") == nil || v.find("nope@pat.n1") != nil {
		e.t.Errorf("pat's view: %+v", v.Entries)
	}
	kit := e.sock("n1", "kit", "echo")
	kl := kit.serve(1, true)
	defer kl.hold()()
	got = kl.putOK(`{"entries":[{"name":"Bad Name"},{"name":".dot"},{"name":"fine"},{"name":"other","namespace":"fa.n1"},{"name":"own","namespace":"kit.n1"}]}`)
	if got["Bad Name"] != "grammar" || got[".dot"] != "grammar" || got["other"] != "namespace" || len(got) != 3 {
		e.t.Errorf("kit: %v", got)
	}
	// The child small has max_entries 3: the list is cut from the tail.
	small := e.sock("n1", "small", "echo")
	sl := small.serve(1, true)
	defer sl.hold()()
	got = sl.putOK(`{"entries":[{"name":"s1"},{"name":"s2"},{"name":"s3"},{"name":"s4"},{"name":"s5"}]}`)
	if got["s4"] != "cut" || got["s5"] != "cut" || len(got) != 2 {
		e.t.Errorf("small: %v", got)
	}
	v := small.view("", 0)
	if v.find("s3@small.n1") == nil || v.find("s4@small.n1") != nil || v.lost("s4") != "cut" || v.lost("s5") != "cut" {
		e.t.Errorf("small's view: %+v lost %+v", v.Entries, v.Lost)
	}
}

// viewChecks: docs/service-api.md, Entries and the view. since and boot, and the shape of the view. It uses the
// node's nobody service, which only this check publishes in, so that the
// entries of other checks (going stale and dropped) do not change the
// version under it.
func (e *env) viewChecks() {
	s := e.sock("n1", "", "nobody")
	relay := "n1"
	l := s.serve(1, true)
	defer l.hold()()
	l.putOK(`{"entries":[{"name":"v1","data":{"b":1,"a":[1,2]}}]}`)
	var v *view
	e.eventually(5*time.Second, "the entry in the view", func() bool { v = s.view("", 0); return v.find("v1@"+relay) != nil })
	self := e.anyJSON(s.do("GET", "/v1/service/self", nil))
	if v.Boot != self["boot"] || v.Version == "" || v.Entries == nil {
		e.t.Errorf("view header: %+v", v)
	}
	m := e.anyJSON(s.do("GET", "/v1/service/view?wait=0", nil))
	for _, k := range []string{"version", "boot", "ready", "reserved", "lost", "entries"} {
		if _, ok := m[k]; !ok {
			e.t.Errorf("view lacks %s", k)
		}
	}
	if _, ok := m["lost"].([]any); !ok {
		e.t.Errorf("view.lost must be an array, got %v", m["lost"])
	}
	if _, ok := m["reserved"].(map[string]any); !ok {
		e.t.Errorf("view.reserved must be an object, got %v", m["reserved"])
	}
	if en := v.find("v1@" + relay); string(en.Data) != `{"b":1,"a":[1,2]}` {
		e.t.Errorf("data is served as JSON, compact and in order: %s", en.Data)
	}
	// since empty, from another boot, or behind: answers at once.
	for _, since := range []string{"", "deadbeef.1", "garbage", "0"} {
		start := time.Now()
		r := s.do("GET", "/v1/service/view?wait=5&since="+url.QueryEscape(since), nil)
		if r.status != 200 || time.Since(start) > 2*time.Second {
			e.t.Errorf("since=%q should answer at once: %v after %v", since, r.status, time.Since(start))
		}
	}
	// since=current waits for a change up to wait, and then answers with the
	// current view, unchanged.
	start := time.Now()
	r := s.do("GET", "/v1/service/view?wait=1&since="+url.QueryEscape(v.Version), nil)
	var same view
	e.json(r, &same)
	if d := time.Since(start); d < 800*time.Millisecond || d > 4*time.Second || same.Version != v.Version {
		e.t.Errorf("a poll at the current version took %v and answered version %q (was %q)", d, same.Version, v.Version)
	}
	// A change wakes it early.
	ch := s.async("GET", "/v1/service/view?wait=20&since="+url.QueryEscape(v.Version), nil)
	time.Sleep(200 * time.Millisecond)
	start = time.Now()
	l.putOK(`{"entries":[{"name":"v1"},{"name":"v2"}]}`)
	r = e.wait(ch, 10*time.Second, "a change to wake the poll")
	var next view
	e.json(r, &next)
	if time.Since(start) > 5*time.Second || next.Version == v.Version || next.find("v2@"+relay) == nil {
		e.t.Errorf("a woken poll: version %q after %v, entries %+v", next.Version, time.Since(start), next.Entries)
	}
	if next.Boot != v.Boot {
		e.t.Error("boot changed without a restart")
	}
	// Nothing else changed, so the same state gives the same version.
	again := s.view("", 0)
	if again.Version != next.Version {
		e.t.Errorf("the version changed without a change: %q then %q", next.Version, again.Version)
	}
}

// viewPolls: docs/service-api.md, Entries and the view. At most 4 view polls per socket.
func (e *env) viewPolls() {
	s, relay := e.fresh()
	l := s.serve(1, false)
	defer l.hold()()
	cur := s.view("", 0).Version
	var open []<-chan *resp
	for i := 0; i < 4; i++ {
		open = append(open, s.async("GET", "/v1/service/view?wait=20&since="+url.QueryEscape(cur), nil))
	}
	time.Sleep(400 * time.Millisecond)
	e.isErr(s.do("GET", "/v1/service/view?wait=0", nil), "busy")
	e.isErr(s.do("GET", "/v1/service/view?wait=1&since="+url.QueryEscape(cur), nil), "busy")
	l.putOK(`{"entries":[{"name":"w"}]}`)
	_ = relay
	for _, ch := range open {
		if r := e.wait(ch, 10*time.Second, "a view poll to wake"); r.status != 200 {
			e.t.Errorf("a woken view poll: %v", r)
		}
	}
	// The places are free again.
	e.status(s.do("GET", "/v1/service/view?wait=0", nil), 200)
}

// pickup: docs/architecture.md, Configuration. A directory made under attach.dir while Warren runs gets the
// template's services within the scan time; a directory that is no usable
// label, or is named like a service namespace, gets nothing.
func (e *env) pickup() {
	s, relay := e.fresh() // fresh waits for the socket
	m := e.anyJSON(s.do("GET", "/v1/service/self", nil))
	if m["relay"] != relay || m["flat"] != false || m["entry_namespace"] != relay {
		e.t.Errorf("a template child's self: %v", m)
	}
	// Template children have the smaller limits.
	l := s.serve(1, false)
	if l.Inflight != 4 {
		e.t.Errorf("template inflight = %d, want 4", l.Inflight)
	}
	l.release()
	for _, name := range []string{"UPPER", "has space", "chat"} {
		_ = mkdir(e, name)
	}
	time.Sleep(2*e.tm.Scan + 500*time.Millisecond)
	for _, name := range []string{"UPPER", "has space", "chat"} {
		if r, err := (&sc{e: e, path: attachPath(e, name)}).try("GET", "/v1/service/self", nil); err == nil && r.status == 200 {
			e.t.Errorf("a directory named %q got a socket", name)
		}
	}
}
