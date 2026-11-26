package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/config"
	"github.com/PeterSR/warren-mesh/internal/link"
)

var (
	quickMu sync.Mutex
	quickIn = map[*testing.T]bool{}
)

// quick shortens every interval so tests run in milliseconds. It is applied
// once per test, however many nodes the test starts, and before any of
// them runs, so a test that wants other values sets them after calling it
// and before starting a node.
func quick(t *testing.T) {
	t.Helper()
	quickMu.Lock()
	if quickIn[t] {
		quickMu.Unlock()
		return
	}
	quickIn[t] = true
	quickMu.Unlock()
	sc, lg, sf, bh, pi, dw, wu, si := ScanEvery, LeaseGrace, StaleFor, BootHold, PutInterval, DrainWait, WaitUnit, ServeInterval
	pd, pg, bmin, bmax, ct := PeerDebounce, PollGap, PeerBackoffMin, PeerBackoffMax, ClockTick
	dle := DropLogEvery
	PeerDebounce, PollGap, PeerBackoffMin, PeerBackoffMax = 30*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond, 200*time.Millisecond
	ServeInterval = 0
	ScanEvery, LeaseGrace, StaleFor, BootHold, PutInterval, DrainWait, WaitUnit =
		40*time.Millisecond, 300*time.Millisecond, 400*time.Millisecond, 300*time.Millisecond, 20*time.Millisecond, 300*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() {
		ScanEvery, LeaseGrace, StaleFor, BootHold, PutInterval, DrainWait, WaitUnit, ServeInterval = sc, lg, sf, bh, pi, dw, wu, si
		PeerDebounce, PollGap, PeerBackoffMin, PeerBackoffMax, ClockTick = pd, pg, bmin, bmax, ct
		DropLogEvery = dle
		quickMu.Lock()
		delete(quickIn, t)
		quickMu.Unlock()
	})
}

const baseYAML = `
node: n1
state_dir: DIR/state
services:
  dir: DIR/s
  list:
    - name: echo
      allow: ["*"]
      namespaces: [chat]
    - name: other
      allow: ["*"]
attach:
  dir: DIR/a
  default:
    services:
      - name: echo
        allow: ["*"]
  children:
    - name: host
      flat: true
      services:
        - name: echo
          allow: ["*"]
    - name: host2
      flat: true
      services:
        - name: echo
          allow: ["*"]
    - name: kit
      services:
        - name: echo
          allow: ["*"]
`

type env struct {
	t    *testing.T
	dir  string
	cfg  *config.Config
	node *Node
	yaml string
}

func tmpDir(t *testing.T) string {
	d, err := os.MkdirTemp("", "w")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func parseYAML(t *testing.T, dir, y string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(strings.ReplaceAll(y, "DIR", dir)))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// testLink, when set by a tweak, is the link the next env starts with, and
// testOpt changes the node's other options.
var (
	testLink link.Link
	testOpt  func(*Options)
)

// newEnv starts a node on yaml (baseYAML if empty) in a fresh directory.
func newEnv(t *testing.T, y string, tweaks ...func()) *env {
	t.Helper()
	quick(t)
	t.Cleanup(func() { testLink, testOpt = nil, nil })
	for _, f := range tweaks {
		f() // after quick, before the node starts, so nothing races
	}
	if y == "" {
		y = baseYAML
	}
	e := &env{t: t, dir: tmpDir(t), yaml: y}
	e.cfg = parseYAML(t, e.dir, y)
	for _, c := range e.cfg.Attach.Children {
		os.MkdirAll(filepath.Join(e.cfg.Attach.Dir, c.Name), 0o755)
	}
	e.start(testLink)
	return e
}

func (e *env) start(l link.Link) {
	e.t.Helper()
	o := Options{Config: e.cfg, Link: l, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if testOpt != nil {
		testOpt(&o)
	}
	n, err := New(o)
	if err != nil {
		e.t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		e.t.Fatal(err)
	}
	e.node = n
	n.flushNames()
	e.t.Cleanup(n.Close)
}

// restart stops the node and starts a new one on the same directories.
func (e *env) restart() {
	e.node.Close()
	e.cfg = parseYAML(e.t, e.dir, e.yaml)
	e.start(nil)
}

func (e *env) mkChild(label string) {
	e.t.Helper()
	if err := os.Mkdir(filepath.Join(e.cfg.Attach.Dir, label), 0o755); err != nil {
		e.t.Fatal(err)
	}
}

// ---- a client of one socket ----

type cl struct {
	t    *testing.T
	path string
	hc   *http.Client
}

func (e *env) nodeSock(svc string) *cl {
	return newCl(e.t, filepath.Join(e.cfg.Services.Dir, svc+".sock"))
}
func (e *env) childSock(label, svc string) *cl {
	return newCl(e.t, filepath.Join(e.cfg.Attach.Dir, label, svc+".sock"))
}

func newCl(t *testing.T, path string) *cl {
	c := &cl{t: t, path: path}
	c.hc = &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		},
		DisableKeepAlives: true,
	}, Timeout: 10 * time.Second}
	// Wait for the socket to be served.
	dl := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.Dial("unix", path)
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(dl) {
			t.Fatalf("socket %s never came up: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return c
}

type resp struct {
	status int
	hdr    http.Header
	body   []byte
}

func (r resp) code() string { return r.hdr.Get(apierr.Header) }

func (r resp) json(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("bad JSON %q: %v", r.body, err)
	}
}

func (c *cl) do(method, pq string, body []byte) resp {
	c.t.Helper()
	req, _ := http.NewRequest(method, "http://warren"+pq, bytes.NewReader(body))
	req.Header.Set("X-Warren", "1")
	r, err := c.hc.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, pq, err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return resp{r.StatusCode, r.Header, b}
}

func (c *cl) get(pq string) resp            { c.t.Helper(); return c.do("GET", pq, nil) }
func (c *cl) post(pq string, b string) resp { c.t.Helper(); return c.do("POST", pq, []byte(b)) }

// expect fails unless r is the given error code.
func expect(t *testing.T, r resp, code string) {
	t.Helper()
	if r.code() != code || r.status != apierr.Status(code) {
		t.Fatalf("want %s (%d), got status %d code %q body %s", code, apierr.Status(code), r.status, r.code(), r.body)
	}
}

func expectOK(t *testing.T, r resp) {
	t.Helper()
	if r.status/100 != 2 {
		t.Fatalf("want success, got status %d code %q body %s", r.status, r.code(), r.body)
	}
}

// serve takes a lease on the socket.
func (c *cl) serve(version int, take bool) (lease string, inflight int) {
	c.t.Helper()
	q := "/v1/service/serve?version=" + itoa(version)
	if take {
		q += "&take=1"
	}
	r := c.post(q, "")
	expectOK(c.t, r)
	var out struct {
		Lease    string `json:"lease"`
		Inflight int    `json:"inflight"`
	}
	r.json(c.t, &out)
	return out.Lease, out.Inflight
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func (c *cl) call(to, method string, body string, extra string) resp {
	c.t.Helper()
	return c.post("/v1/service/call?to="+to+"&method="+method+"&version=1"+extra, body)
}

// put publishes entries (JSON text for the entries array).
func (c *cl) put(lease, entries, reserved string) resp {
	c.t.Helper()
	if reserved == "" {
		reserved = "[]"
	}
	return c.do("PUT", "/v1/service/entries?lease="+lease, []byte(`{"entries":`+entries+`,"reserved":`+reserved+`}`))
}

func names(ns ...string) string {
	var parts []string
	for _, n := range ns {
		parts = append(parts, `{"name":"`+n+`","data":{"n":"`+n+`"}}`)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

type view struct {
	Version  string              `json:"version"`
	Boot     string              `json:"boot"`
	Ready    bool                `json:"ready"`
	Reserved map[string][]string `json:"reserved"`
	Lost     []lostRec           `json:"lost"`
	Entries  []struct {
		Address string          `json:"address"`
		Origin  string          `json:"origin"`
		Data    json.RawMessage `json:"data"`
		Stale   bool            `json:"stale"`
	} `json:"entries"`
}

func (c *cl) view() view {
	c.t.Helper()
	r := c.get("/v1/service/view?wait=0")
	expectOK(c.t, r)
	var v view
	r.json(c.t, &v)
	return v
}

func (v view) addrs() []string {
	var out []string
	for _, e := range v.Entries {
		out = append(out, e.Address)
	}
	return out
}

func (v view) has(addr string) bool {
	for _, a := range v.addrs() {
		if a == addr {
			return true
		}
	}
	return false
}

func (v view) lostReason(name string) string {
	for _, l := range v.Lost {
		if l.Name == name {
			return l.Reason
		}
	}
	return ""
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	dl := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(dl) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---- the test echo service ----

// echo is the test-only service that drives the API: it takes a lease,
// polls for calls and answers each with "<method>:<body>" while recording
// the headers it saw.
type echo struct {
	t      *testing.T
	c      *cl
	lease  string
	mu     sync.Mutex
	seen   []http.Header
	stop   chan struct{}
	done   chan struct{}
	answer func(h http.Header, body []byte) (reply string, ok bool)
}

func startEcho(t *testing.T, c *cl) *echo {
	t.Helper()
	lease, _ := c.serve(1, true)
	e := &echo{t: t, c: c, lease: lease, stop: make(chan struct{}), done: make(chan struct{})}
	go e.loop()
	t.Cleanup(e.close)
	return e
}

func (e *echo) loop() {
	defer close(e.done)
	for {
		select {
		case <-e.stop:
			return
		default:
		}
		req, _ := http.NewRequest("GET", "http://warren/v1/service/calls?lease="+e.lease+"&wait=1", nil)
		req.Header.Set("X-Warren", "1")
		r, err := e.c.hc.Do(req)
		if err != nil {
			return
		}
		body, _ := io.ReadAll(r.Body)
		r.Body.Close()
		switch r.StatusCode {
		case 200:
			e.mu.Lock()
			e.seen = append(e.seen, r.Header.Clone())
			e.mu.Unlock()
			reply := r.Header.Get("X-Warren-Method") + ":" + string(body)
			if e.answer != nil {
				var ok bool
				if reply, ok = e.answer(r.Header, body); !ok {
					continue
				}
			}
			ans, _ := http.NewRequest("POST", "http://warren/v1/service/answer?lease="+e.lease+"&id="+r.Header.Get("X-Warren-Call"), strings.NewReader(reply))
			ans.Header.Set("X-Warren", "1")
			if ar, err := e.c.hc.Do(ans); err == nil {
				ar.Body.Close()
			}
		case 204:
		default:
			return
		}
	}
}

func (e *echo) close() {
	select {
	case <-e.stop:
	default:
		close(e.stop)
	}
	<-e.done
}

func (e *echo) last() http.Header {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.seen) == 0 {
		return nil
	}
	return e.seen[len(e.seen)-1]
}

// headerOnly sends a POST claiming a body of n bytes and sends none, to see
// what the server answers before reading it.
func (c *cl) headerOnly(pq string, n int) resp { return c.headerOnlyM("POST", pq, n) }

func (c *cl) headerOnlyM(method, pq string, n int) resp {
	c.t.Helper()
	conn, err := net.Dial("unix", c.path)
	if err != nil {
		c.t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: x\r\nX-Warren: 1\r\nContent-Length: %d\r\n\r\n", method, pq, n)
	r, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		c.t.Fatalf("no answer to %s: %v", pq, err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return resp{r.StatusCode, r.Header, b}
}

// sockState reads a number off a socket under the lock; 0 if there is none.
func (n *Node) sockState(relay, svc string, f func(*sock) int) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if s := n.socks[sockKey{relay, svc}]; s != nil {
		return f(s)
	}
	return 0
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
