package conformance

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"
)

// errorTable is the error table of the contract: the status of each code.
var errorTable = map[string]int{
	"bad-request":    400,
	"too-large":      413,
	"refused":        403,
	"no-target":      404,
	"no-service":     404,
	"version":        409,
	"already-served": 409,
	"gone":           410,
	"busy":           429,
	"offline":        503,
	"not-ready":      503,
	"unknown":        502,
	"timeout":        504,
	"unsupported":    404,
}

// resp is one answer.
type resp struct {
	status int
	h      http.Header
	body   []byte
}

func (r *resp) String() string {
	return fmt.Sprintf("%d %s %q", r.status, r.h.Get("X-Warren-Error"), r.body)
}

// opt changes one request.
type opt func(*http.Request)

func noWarrenHeader() opt    { return func(r *http.Request) { r.Header.Del("X-Warren") } }
func header(k, v string) opt { return func(r *http.Request) { r.Header.Set(k, v) } }

// sc is one service socket.
type sc struct {
	e    *env
	path string
	name string // for messages: "n1/kit/echo"
}

// client makes an HTTP client that dials the socket afresh every time.
func (s *sc) client() *http.Client {
	return &http.Client{Timeout: 90 * time.Second, Transport: &http.Transport{DisableKeepAlives: true, Proxy: nil,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", s.path)
		}}}
}

// do makes a request on a fresh connection, with X-Warren: 1 unless told not
// to. A transport error fails the test.
func (s *sc) do(method, pq string, body []byte, opts ...opt) *resp {
	s.e.t.Helper()
	r, err := s.try(method, pq, body, opts...)
	if err != nil {
		s.e.t.Fatalf("%s %s on %s: %v", method, pq, s.name, err)
	}
	return r
}

func (s *sc) try(method, pq string, body []byte, opts ...opt) (*resp, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, "http://warren"+pq, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Warren", "1")
	for _, o := range opts {
		o(req)
	}
	hr, err := s.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer hr.Body.Close()
	b, err := io.ReadAll(hr.Body)
	if err != nil {
		return nil, err
	}
	return &resp{hr.StatusCode, hr.Header, b}, nil
}

// async runs a request in the background.
func (s *sc) async(method, pq string, body []byte, opts ...opt) <-chan *resp {
	ch := make(chan *resp, 1)
	go func() {
		r, err := s.try(method, pq, body, opts...)
		if err != nil {
			r = &resp{status: -1, h: http.Header{}, body: []byte(err.Error())}
		}
		ch <- r
	}()
	return ch
}

// wait gets the answer of an async request within d.
func (e *env) wait(ch <-chan *resp, d time.Duration, what string) *resp {
	e.t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(d):
		e.t.Fatalf("no answer within %v: %s", d, what)
		return nil
	}
}

// isErr checks an answer against the error table: status, header, JSON body
// with the same code and a detail of at most 200 bytes. It returns whether
// it matched, and records the code as covered.
func (e *env) isErr(r *resp, code string) bool {
	e.t.Helper()
	ok := true
	fail := func(format string, a ...any) {
		e.t.Helper()
		e.t.Errorf("want error %s: "+format+" (got %v)", append(append([]any{code}, a...), r)...)
		ok = false
	}
	if r.status != errorTable[code] {
		fail("status %d, want %d", r.status, errorTable[code])
	}
	if got := r.h.Get("X-Warren-Error"); got != code {
		fail("X-Warren-Error %q", got)
	}
	var b struct{ Error, Detail string }
	if err := json.Unmarshal(r.body, &b); err != nil || b.Error != code {
		fail("body is not {\"error\": %q, \"detail\": ...}", code)
	}
	if len(b.Detail) > 200 {
		fail("detail is %d bytes, at most 200", len(b.Detail))
	}
	for _, c := range b.Detail {
		if unicode.IsControl(c) || unicode.Is(unicode.Cf, c) {
			fail("detail has the control or invisible character %U", c)
			break
		}
	}
	if ct := r.h.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		fail("Content-Type %q, want application/json", ct)
	}
	if ok {
		e.mu.Lock()
		e.seen[code] = true
		e.mu.Unlock()
	}
	return ok
}

// status checks the status.
func (e *env) status(r *resp, want int) bool {
	e.t.Helper()
	if r.status != want {
		e.t.Errorf("want status %d, got %v", want, r)
		return false
	}
	return true
}

func (e *env) json(r *resp, v any) {
	e.t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		e.t.Fatalf("not JSON: %v (%v)", err, r)
	}
}

// ---- the environment of one run ----

// env is what one check works with: its own *testing.T and the Warren
// under test, which all checks share.
type env struct {
	t *testing.T
	*shared
}

type shared struct {
	l    Launcher
	real bool
	in   *Instance
	tm   Timing

	mu   *sync.Mutex     // guards seen and next, shared by instances
	seen map[string]bool // error codes observed, checked at the end
	next int             // template children made
}

// sock is the socket of a service at the node (label "") or a child of it.
func (e *env) sock(node, label, service string) *sc {
	p := filepath.Join(e.in.Dir, node, "services", service+".sock")
	name := node + "/" + service
	if label != "" {
		p = filepath.Join(e.in.Dir, node, "attach", label, service+".sock")
		name = node + "/" + label + "/" + service
	}
	return &sc{e: e, path: p, name: name}
}

// fresh makes a directory under n1's attach directory and waits for the
// template's echo socket in it: an isolated relay, label.n1.
func (e *env) fresh() (*sc, string) {
	e.t.Helper()
	e.mu.Lock()
	e.next++
	label := fmt.Sprintf("t%d", e.next)
	e.mu.Unlock()
	if err := os.Mkdir(filepath.Join(e.in.Dir, "n1", "attach", label), 0o755); err != nil {
		e.t.Fatal(err)
	}
	s := e.sock("n1", label, "echo")
	e.eventually(10*time.Second+3*e.tm.Scan, "the template to give "+label+" its socket", func() bool {
		r, err := s.try("GET", "/v1/service/self", nil)
		return err == nil && r.status == 200
	})
	return s, label + ".n1"
}

// eventually polls cond every 20 ms until it holds or d has passed.
func (e *env) eventually(d time.Duration, what string, cond func() bool) {
	e.t.Helper()
	dl := time.Now().Add(d)
	for !cond() {
		if time.Now().After(dl) {
			e.t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// lease is a lease taken by hand.
type lease struct {
	s        *sc
	Token    string `json:"lease"`
	Inflight int    `json:"inflight"`
}

// serve takes the lease, waiting out the serve interval when needed.
func (s *sc) serve(version int, take bool) *lease {
	s.e.t.Helper()
	q := fmt.Sprintf("/v1/service/serve?version=%d", version)
	if take {
		q += "&take=1"
	}
	var r *resp
	for i := 0; i < 50; i++ {
		r = s.do("POST", q, nil)
		if r.status == 429 && r.h.Get("X-Warren-Error") == "busy" {
			time.Sleep(s.e.tm.ServeInterval/4 + 20*time.Millisecond)
			continue
		}
		break
	}
	if r.status != 200 {
		s.e.t.Fatalf("serve on %s: %v", s.name, r)
	}
	l := &lease{s: s}
	s.e.json(r, l)
	if l.Token == "" || l.Inflight < 1 {
		s.e.t.Fatalf("serve on %s answered %v", s.name, r)
	}
	return l
}

func (l *lease) q(extra string) string { return "lease=" + url.QueryEscape(l.Token) + extra }

// poll polls once for up to wait seconds; it returns the call or nil (204).
func (l *lease) poll(wait int) *call {
	l.s.e.t.Helper()
	r := l.s.do("GET", fmt.Sprintf("/v1/service/calls?%s&wait=%d", l.q(""), wait), nil)
	return l.s.e.asCall(r)
}

// call is a call handed out.
type call struct {
	id, origin, from, to, method string
	deadlineMs                   int64
	body                         []byte
	h                            http.Header
}

func (e *env) asCall(r *resp) *call {
	e.t.Helper()
	switch r.status {
	case 204:
		return nil
	case 200:
		c := &call{id: r.h.Get("X-Warren-Call"), origin: r.h.Get("X-Warren-Origin"), from: r.h.Get("X-Warren-From"),
			to: r.h.Get("X-Warren-To"), method: r.h.Get("X-Warren-Method"), body: r.body, h: r.h}
		_, _ = fmt.Sscan(r.h.Get("X-Warren-Deadline-Ms"), &c.deadlineMs) // absent or bad: caught by the check below
		if c.id == "" || c.origin == "" || c.to == "" || c.method == "" || c.deadlineMs < 1 {
			e.t.Errorf("a handed-out call lacks headers: %v", r.h)
		}
		return c
	}
	e.t.Fatalf("poll: %v", r)
	return nil
}

// mustPoll waits up to d for a call.
func (l *lease) mustPoll(d time.Duration) *call {
	l.s.e.t.Helper()
	dl := time.Now().Add(d)
	for time.Now().Before(dl) {
		if c := l.poll(1); c != nil {
			return c
		}
	}
	l.s.e.t.Fatalf("no call arrived within %v on %s", d, l.s.name)
	return nil
}

func (l *lease) answer(id string, body string) *resp {
	l.s.e.t.Helper()
	return l.s.do("POST", "/v1/service/answer?"+l.q("&id="+url.QueryEscape(id)), []byte(body))
}

// fail gives a handed-out call up (answer with fail=1 and no body).
func (l *lease) fail(id string) *resp {
	l.s.e.t.Helper()
	return l.s.do("POST", "/v1/service/answer?"+l.q("&id="+url.QueryEscape(id)+"&fail=1"), nil)
}

func (l *lease) release() *resp { return l.s.do("POST", "/v1/service/release?"+l.q(""), nil) }

// put publishes entries.
func (l *lease) put(body string) *resp {
	l.s.e.t.Helper()
	var r *resp
	for i := 0; i < 50; i++ {
		var err error
		r, err = l.s.try("PUT", "/v1/service/entries?"+l.q(""), []byte(body))
		if err != nil {
			// A server that answered early and closed (a refused body) can make
			// the client's write fail: ask again after the interval.
			r = &resp{status: -1, h: http.Header{}}
			time.Sleep(l.s.e.tm.PutInterval + 20*time.Millisecond)
			continue
		}
		if r.status == 429 {
			time.Sleep(l.s.e.tm.PutInterval/4 + 20*time.Millisecond)
			continue
		}
		break
	}
	return r
}

// putOK publishes entries and returns the refusals as reason by name.
func (l *lease) putOK(body string) map[string]string {
	l.s.e.t.Helper()
	r := l.put(body)
	if r.status != 200 {
		l.s.e.t.Fatalf("PUT on %s: %v", l.s.name, r)
	}
	var out struct {
		Refused []struct{ Name, Namespace, Reason string }
	}
	l.s.e.json(r, &out)
	m := map[string]string{}
	for _, x := range out.Refused {
		m[x.Name] = x.Reason
	}
	return m
}

// callQ is the query of a call.
func callQ(to, method string, version int, extra string) string {
	return fmt.Sprintf("/v1/service/call?to=%s&method=%s&version=%d%s", url.QueryEscape(to), method, version, extra)
}

// view is the parsed view.
type view struct {
	Version  string
	Boot     string
	Ready    bool
	Reserved map[string][]string
	Lost     []struct{ Name, Namespace, Reason string }
	Entries  []struct {
		Address, Origin string
		Data            json.RawMessage
		Stale           bool
	}
}

func (s *sc) view(since string, wait int) *view {
	s.e.t.Helper()
	r := s.do("GET", fmt.Sprintf("/v1/service/view?since=%s&wait=%d", url.QueryEscape(since), wait), nil)
	if r.status != 200 {
		s.e.t.Fatalf("view on %s: %v", s.name, r)
	}
	var v view
	s.e.json(r, &v)
	return &v
}

// find is the entry at address in the view, or nil.
func (v *view) find(address string) *struct {
	Address, Origin string
	Data            json.RawMessage
	Stale           bool
} {
	for i := range v.Entries {
		if v.Entries[i].Address == address {
			return &v.Entries[i]
		}
	}
	return nil
}

func (v *view) lost(name string) string {
	for _, l := range v.Lost {
		if l.Name == name {
			return l.Reason
		}
	}
	return ""
}

// rawConn opens a connection to the socket for hand-written HTTP.
func (s *sc) rawConn() net.Conn {
	s.e.t.Helper()
	c, err := net.DialTimeout("unix", s.path, 5*time.Second)
	if err != nil {
		s.e.t.Fatal(err)
	}
	s.e.t.Cleanup(func() { c.Close() })
	return c
}

// rawRequest writes a request by hand and reads the answer.
func (s *sc) rawRequest(head string) *resp {
	s.e.t.Helper()
	c := s.rawConn()
	fmt.Fprint(c, head)
	hr, err := http.ReadResponse(bufioReader(c), nil)
	if err != nil {
		s.e.t.Fatalf("raw request on %s: %v", s.name, err)
	}
	defer hr.Body.Close()
	b, _ := io.ReadAll(hr.Body)
	return &resp{hr.StatusCode, hr.Header, b}
}

func bufioReader(c net.Conn) *bufio.Reader { return bufio.NewReader(c) }

// hold keeps the lease alive with polls in the background (a lease with no
// poll open ends after the lease grace) and returns what stops them and
// releases it. A call that arrives meanwhile is taken and left unanswered, so
// use it only where no call is expected.
func (l *lease) hold() (done func()) {
	stop := make(chan struct{})
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		for {
			select {
			case <-stop:
				return
			default:
			}
			r, err := l.s.try("GET", "/v1/service/calls?"+l.q("&wait=1"), nil)
			if err != nil || r.status >= 300 {
				return
			}
		}
	}()
	return func() {
		close(stop)
		<-ended
		_, _ = l.s.try("POST", "/v1/service/release?"+l.q(""), nil)
	}
}

// rawWithBody writes a request by hand, sends the body in the background and
// reads the answer as soon as it comes: a server that refuses a body it has
// not read may answer and close, which an HTTP client reports as a write
// error instead of the answer.
func (s *sc) rawWithBody(head string, body []byte) *resp {
	s.e.t.Helper()
	c := s.rawConn()
	fmt.Fprint(c, head)
	go func() { _, _ = c.Write(body) }()
	hr, err := http.ReadResponse(bufioReader(c), nil)
	if err != nil {
		s.e.t.Fatalf("raw request on %s: %v", s.name, err)
	}
	defer hr.Body.Close()
	b, _ := io.ReadAll(hr.Body)
	return &resp{hr.StatusCode, hr.Header, b}
}
