package conformance

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// anyJSON decodes a JSON document into maps, for checks of its shape.
func (e *env) anyJSON(r *resp) map[string]any {
	e.t.Helper()
	var m map[string]any
	e.json(r, &m)
	return m
}

// conventions: docs/service-api.md, Conventions. Every request carries X-Warren: 1, bodies need a
// Content-Length, every error has the same shape, and an unknown path is
// unsupported.
func (e *env) conventions() {
	s, _ := e.fresh()
	e.isErr(s.do("GET", "/v1/service/self", nil, noWarrenHeader()), "bad-request")
	e.isErr(s.do("POST", "/v1/service/serve?version=1", nil, noWarrenHeader()), "bad-request")
	if r := s.do("GET", "/v1/service/self", nil, header("X-Warren", "2")); r.status != 400 {
		e.t.Errorf("X-Warren: 2 should be refused as bad-request: %v", r)
	}
	// An endpoint this Warren does not have, or one with another method, is
	// unsupported: frozen in v1, so a newer caller can tell it from no-target.
	for _, p := range [][2]string{
		{"GET", "/"}, {"GET", "/v1/service"}, {"GET", "/v1/service/nope"}, {"GET", "/v2/service/self"},
		{"POST", "/v1/service/self"}, {"GET", "/v1/service/serve"}, {"DELETE", "/v1/service/entries"},
		{"GET", "/v1/transport/hello"}, {"GET", "/v1/peer/hello"},
	} {
		if r := s.do(p[0], p[1], nil); !e.isErr(r, "unsupported") {
			e.t.Logf("%s %s", p[0], p[1])
		}
	}
	// A chunked body is refused (a Content-Length is required).
	r := s.rawRequest("POST /v1/service/call?to=x&method=m&version=1 HTTP/1.1\r\nHost: w\r\nX-Warren: 1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n0\r\n\r\n")
	e.isErr(r, "bad-request")
	r = s.rawRequest("PUT /v1/service/entries?lease=x HTTP/1.1\r\nHost: w\r\nX-Warren: 1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n0\r\n\r\n")
	e.isErr(r, "bad-request")
	// An unknown path is unsupported whether or not X-Warren is there.
	e.isErr(s.do("GET", "/v1/service/nope", nil, noWarrenHeader()), "unsupported")
	// The host name is not looked at.
	r = s.rawRequest("GET /v1/service/self HTTP/1.1\r\nHost: anything.example\r\nX-Warren: 1\r\nConnection: close\r\n\r\n")
	e.status(r, 200)
	// An error's detail is cleaned and capped: a long, ugly parameter cannot
	// make a long or multi-line detail.
	r = s.do("POST", "/v1/service/call?to="+strings.Repeat("%0a%e2%80%ae", 400)+"&method=m&version=1", nil)
	e.isErr(r, "bad-request")
	// Hostile text in a name never reaches the detail raw: a newline and a
	// bidi override in a reserved name come back cleaned (isErr checks).
	l := s.serve(1, false)
	defer l.hold()()
	e.isErr(l.put(`{"entries":[],"reserved":["a\u202eb\nc"]}`), "bad-request")
	e.isErr(s.do("POST", "/v1/service/call?to=a%0ab%E2%80%AE&method=m&version=1", nil), "bad-request")
}

// parameters: docs/service-api.md, Conventions and Leases, polls and answers. Integers are decimal, wait is whole seconds 0 to
// 25, and anything else is bad-request.
func (e *env) parameters() {
	s, _ := e.fresh()
	l := s.serve(1, false)
	defer l.release()
	for _, w := range []string{"26", "-1", "abc", "1.5", "100"} {
		e.isErr(s.do("GET", "/v1/service/view?wait="+w, nil), "bad-request")
		e.isErr(s.do("GET", "/v1/service/calls?"+l.q("&wait="+w), nil), "bad-request")
	}
	// wait=0 is accepted (25 is too, but it would wait that long; 26 is refused above).
	r := s.do("GET", "/v1/service/view?wait=0", nil)
	e.status(r, 200)
	if ct := r.h.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		e.t.Errorf("view Content-Type %q", ct)
	}
	for _, v := range []string{"", "&version=0", "&version=abc", "&version=-1", "&version=1.0"} {
		e.isErr(s.do("POST", "/v1/service/call?to=x&method=m"+v, nil), "bad-request")
	}
	for _, v := range []string{"", "0", "abc", "-1", "1.0"} {
		e.isErr(s.do("POST", "/v1/service/serve?version="+v, nil), "bad-request")
	}
	for _, v := range []string{"0", "-5", "300001", "abc"} {
		e.isErr(s.do("POST", "/v1/service/call?to=x&method=m&version=1&timeout_ms="+v, nil), "bad-request")
	}
	// The largest timeout is accepted (the call then fails for another reason).
	if r := s.do("POST", "/v1/service/call?to=zzz.n1&method=m&version=1&timeout_ms=300000", nil); !e.isErr(r, "no-target") {
		e.t.Error("timeout_ms=300000 should be accepted")
	}
	// A lease is required where one is used.
	e.isErr(s.do("GET", "/v1/service/calls?wait=0", nil), "bad-request")
	e.isErr(s.do("POST", "/v1/service/answer?id=x", nil), "bad-request")
	e.isErr(s.do("POST", "/v1/service/release", nil), "bad-request")
	e.isErr(s.do("PUT", "/v1/service/entries", []byte(`{"entries":[]}`)), "bad-request")
}

// connectionLimit: docs/service-api.md, Conventions. A socket accepts at most 64 connections; the 65th is
// closed at once.
func (e *env) connectionLimit() {
	s, _ := e.fresh()
	req := "GET /v1/service/self HTTP/1.1\r\nHost: w\r\nX-Warren: 1\r\n\r\n"
	var conns []net.Conn
	var readers []*bufio.Reader
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for i := 0; i < 64; i++ {
		c, err := net.DialTimeout("unix", s.path, 5*time.Second)
		if err != nil {
			e.t.Fatalf("connection %d: %v", i+1, err)
		}
		conns = append(conns, c)
		readers = append(readers, bufio.NewReader(c))
		fmt.Fprint(c, req)
		hr, err := http.ReadResponse(readers[i], nil)
		if err != nil {
			e.t.Fatalf("connection %d was not served: %v", i+1, err)
		}
		hr.Body.Close()
	}
	c65, err := net.DialTimeout("unix", s.path, 5*time.Second)
	if err != nil {
		e.t.Fatal(err)
	}
	defer c65.Close()
	fmt.Fprint(c65, req)
	_ = c65.SetReadDeadline(time.Now().Add(3 * time.Second))
	if hr, err := http.ReadResponse(bufio.NewReader(c65), nil); err == nil {
		hr.Body.Close()
		e.t.Errorf("the 65th connection was served (%d); it should be closed at once", hr.StatusCode)
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		e.t.Error("the 65th connection was left open; it should be closed at once")
	}
	// A place frees when a connection closes.
	conns[0].Close()
	e.eventually(5*time.Second, "a closed connection to free its place", func() bool {
		r, err := s.try("GET", "/v1/service/self", nil)
		return err == nil && r.status == 200
	})
}

// selfAndStatus: docs/service-api.md, Identity and status.
func (e *env) selfAndStatus() {
	n := e.sock("n1", "", "echo")
	m := e.anyJSON(n.do("GET", "/v1/service/self", nil))
	for k, want := range map[string]any{"relay": "n1", "node": "n1", "service": "echo", "flat": false, "entry_namespace": "n1", "max_call_body": float64(1 << 20)} {
		if m[k] != want {
			e.t.Errorf("self[%s] = %v, want %v", k, m[k], want)
		}
	}
	if ns, ok := m["namespaces"].([]any); !ok || len(ns) != 1 || ns[0] != "chat.n1" {
		e.t.Errorf("self namespaces = %v, want [chat.n1]", m["namespaces"])
	}
	boot, _ := m["boot"].(string)
	if boot == "" {
		e.t.Error("self has no boot")
	}
	fa := e.anyJSON(e.sock("n1", "fa", "echo").do("GET", "/v1/service/self", nil))
	if fa["relay"] != "fa.n1" || fa["flat"] != true || fa["entry_namespace"] != "n1" || fa["boot"] != boot {
		e.t.Errorf("flat child self: %v", fa)
	}
	if ns, ok := fa["namespaces"].([]any); !ok || len(ns) != 0 {
		e.t.Errorf("a child's namespaces must be [], got %v", fa["namespaces"])
	}
	kit := e.anyJSON(e.sock("n1", "kit", "echo").do("GET", "/v1/service/self", nil))
	if kit["relay"] != "kit.n1" || kit["flat"] != false || kit["entry_namespace"] != "kit.n1" {
		e.t.Errorf("child self: %v", kit)
	}
	if r := e.anyJSON(e.sock("n2", "", "echo").do("GET", "/v1/service/self", nil)); r["node"] != "n2" || r["boot"] == boot {
		e.t.Errorf("another node's self: %v", r)
	}

	// status
	var st map[string]any
	e.eventually(10*time.Second, "n2 to show as an up peer", func() bool {
		st = e.anyJSON(n.do("GET", "/v1/service/status", nil))
		peers, _ := st["peers"].([]any)
		for _, p := range peers {
			if pm, _ := p.(map[string]any); pm["node"] == "n2" && pm["up"] == true {
				for _, k := range []string{"version", "peer_protocol", "features"} {
					if _, ok := pm[k]; !ok {
						e.t.Errorf("peer lacks %s: %v", k, pm)
					}
				}
				return true
			}
		}
		return false
	})
	if st["node"] != "n1" || st["boot"] != boot {
		e.t.Errorf("status node/boot: %v", st)
	}
	w, _ := st["warren"].(map[string]any)
	if v, _ := w["version"].(string); v == "" {
		e.t.Errorf("status warren.version: %v", w)
	}
	if pp, _ := w["peer_protocol"].([]any); len(pp) == 0 || pp[0] != float64(1) {
		e.t.Errorf("status warren.peer_protocol: %v", w["peer_protocol"])
	}
	if _, ok := w["features"].([]any); !ok {
		e.t.Errorf("status warren.features must be an array: %v", w["features"])
	}
	if tr, ok := st["transport"].(map[string]any); !ok || tr["name"] == nil {
		e.t.Errorf("status transport: %v", st["transport"])
	}
	has := func(list, key, val string, extra map[string]any) bool {
		items, _ := st[list].([]any)
		for _, it := range items {
			im, _ := it.(map[string]any)
			if im[key] == val {
				ok := true
				for k, v := range extra {
					ok = ok && im[k] == v
				}
				if ok {
					return true
				}
			}
		}
		return false
	}
	if !has("children", "label", "fa", map[string]any{"flat": true}) || !has("children", "label", "kit", map[string]any{"flat": false}) {
		e.t.Errorf("status children: %v", st["children"])
	}
	if !has("services", "relay", "n1", map[string]any{"name": "echo", "lease": false}) || !has("services", "relay", "kit.n1", map[string]any{"name": "locked"}) {
		e.t.Errorf("status services: %v", st["services"])
	}
	// A lease shows in the status of every socket of the node.
	s, relay := e.fresh()
	l := s.serve(1, false)
	e.eventually(5*time.Second, "the lease to show in status", func() bool {
		st = e.anyJSON(n.do("GET", "/v1/service/status", nil))
		return has("services", "relay", relay, map[string]any{"name": "echo", "lease": true})
	})
	l.release()
	e.eventually(5*time.Second, "the release to show in status", func() bool {
		st = e.anyJSON(n.do("GET", "/v1/service/status", nil))
		return has("services", "relay", relay, map[string]any{"name": "echo", "lease": false})
	})
}

// serveAndRelease: docs/service-api.md, Leases, polls and answers. One lease per socket, take over, rate, release.
func (e *env) serveAndRelease() {
	s, relay := e.fresh()
	l1 := s.serve(1, false)
	if l1.Inflight != 4 {
		e.t.Errorf("inflight = %d, the template gives 4", l1.Inflight)
	}
	// Another lease while one is live: already-served; take=1 takes over.
	e.isErr(s.do("POST", "/v1/service/serve?version=1", nil), "already-served")
	e.isErr(s.do("POST", "/v1/service/serve?version=2", nil), "already-served")
	// One serve per interval per socket: the takeover right after is too soon.
	r := s.do("POST", "/v1/service/serve?version=1&take=1", nil)
	e.isErr(r, "busy")
	time.Sleep(e.tm.ServeInterval + 50*time.Millisecond)
	l2 := s.serve(1, true)
	if l2.Token == l1.Token {
		e.t.Error("a takeover gave the same lease")
	}
	// The old lease is dead, for everything.
	e.isErr(s.do("GET", "/v1/service/calls?"+l1.q("&wait=0"), nil), "gone")
	e.isErr(l1.answer("x", ""), "gone")
	e.isErr(l1.release(), "gone")
	e.isErr(s.do("PUT", "/v1/service/entries?"+l1.q(""), []byte(`{"entries":[]}`)), "gone")
	e.isErr(s.do("GET", "/v1/service/calls?lease=nonsense&wait=0", nil), "gone")
	// The new one works, and a call finds it.
	if c := l2.poll(0); c != nil {
		e.t.Errorf("a call out of nowhere: %+v", c)
	}
	// Release ends it at once: the target then has no live lease.
	caller, _ := e.fresh()
	if r := l2.release(); r.status != 204 {
		e.t.Errorf("release: %v", r)
	}
	e.isErr(l2.release(), "gone")
	e.isErr(caller.do("POST", callQ(relay, "m", 1, ""), nil), "no-service")
	// And a new lease can be had at once (after the serve interval).
	l3 := s.serve(2, false)
	e.isErr(caller.do("POST", callQ(relay, "m", 1, ""), nil), "version")
	l3.release()
}

// polls: docs/service-api.md, Leases, polls and answers. A lease may have at most inflight polls open; calls with wait=0
// and no call is 204; the lease ending answers open polls gone.
func (e *env) polls() {
	s, _ := e.fresh()
	l := s.serve(1, false)
	e.status(s.do("GET", "/v1/service/calls?"+l.q("&wait=0"), nil), 204)
	var open []<-chan *resp
	for i := 0; i < l.Inflight; i++ {
		open = append(open, s.async("GET", "/v1/service/calls?"+l.q("&wait=10"), nil))
	}
	time.Sleep(300 * time.Millisecond)
	e.isErr(s.do("GET", "/v1/service/calls?"+l.q("&wait=0"), nil), "busy")
	e.isErr(s.do("GET", "/v1/service/calls?"+l.q("&wait=1"), nil), "busy")
	// The polls are still open and wake with gone when the lease ends.
	select {
	case r := <-open[0]:
		e.t.Fatalf("a poll returned early: %v", r)
	default:
	}
	start := time.Now()
	l.release()
	for _, ch := range open {
		r := e.wait(ch, 5*time.Second, "an open poll to be told the lease ended")
		e.isErr(r, "gone")
	}
	if time.Since(start) > 3*time.Second {
		e.t.Errorf("open polls took %v to learn that the lease ended", time.Since(start))
	}
}
