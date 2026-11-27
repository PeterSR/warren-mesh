package link

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
)

func TestCheckHello(t *testing.T) {
	good := Hello{Version: 1, Node: "n", MaxBody: MinMaxBody}
	if err := CheckHello(good, "n"); err != nil {
		t.Fatal(err)
	}
	for name, h := range map[string]Hello{
		"version": {Version: 2, Node: "n", MaxBody: MinMaxBody},
		"node":    {Version: 1, Node: "m", MaxBody: MinMaxBody},
		"body":    {Version: 1, Node: "n", MaxBody: MinMaxBody - 1},
	} {
		if CheckHello(h, "n") == nil {
			t.Errorf("%s mismatch accepted", name)
		}
	}
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Warren-From", FromOf(r))
		w.Header().Set("X-Warren-Echo", r.Header.Get("X-Warren-Thing"))
		if carriedHeaders(r.Header) {
			w.Header().Set("X-Warren-Leak", "request")
		}
		w.Header().Set(HeaderTo, "should-not-travel")
		b, _ := io.ReadAll(r.Body)
		_, _ = w.Write(b)
	})
}

func FromOf(r *http.Request) string { return From(r.Context()) }

func TestFakeCarry(t *testing.T) {
	n := NewNet()
	a, b := n.Join("a"), n.Join("b")
	b.Handle(okHandler())
	ctx := context.Background()
	resp, err := a.Carry(ctx, "b", &Request{Method: "POST", URI: "/v1/peer/call?x=1",
		Header: http.Header{"X-Warren-Thing": {"t"}, HeaderTo: {"evil"}}, Body: []byte("hello"),
		Deadline: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body) != "hello" || resp.Header.Get("X-Warren-From") != "a" || resp.Header.Get("X-Warren-Echo") != "t" {
		t.Errorf("got %q %v", resp.Body, resp.Header)
	}
	if resp.Header.Get("X-Warren-Leak") != "" || resp.Header.Get(HeaderTo) != "" {
		t.Error("hop headers travelled")
	}

	// Down node: not-sent.
	n.SetDown("b", true)
	_, err = a.Carry(ctx, "b", &Request{Method: "GET", URI: "/v1/peer/hello"})
	var ce *CarryError
	if !errors.As(err, &ce) || ce.Kind != NotSent {
		t.Errorf("want not-sent, got %v", err)
	}
	n.SetDown("b", false)
	a.FailNext(Lost)
	_, err = a.Carry(ctx, "b", &Request{Method: "GET", URI: "/v1/peer/hello"})
	if !errors.As(err, &ce) || ce.Kind != Lost {
		t.Errorf("want lost, got %v", err)
	}
	if len(a.Carried()) != 3 {
		t.Errorf("carried %d", len(a.Carried()))
	}
}

func TestFakePeersLongPoll(t *testing.T) {
	n := NewNet()
	a := n.Join("a")
	l, _ := a.Peers(context.Background(), "", 0)
	if len(l.Peers) != 0 {
		t.Fatal(l)
	}
	got := make(chan PeerList, 1)
	go func() {
		l2, _ := a.Peers(context.Background(), l.Version, 5*time.Second)
		got <- l2
	}()
	time.Sleep(50 * time.Millisecond)
	n.Join("b")
	select {
	case l2 := <-got:
		if len(l2.Peers) != 1 || l2.Peers[0] != "b" {
			t.Errorf("got %v", l2)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long poll not woken")
	}
}

func TestIncomingChecks(t *testing.T) {
	in := NewIncoming("me")
	in.Set(okHandler())
	do := func(path, from string) *http.Response {
		w := newRec()
		r, _ := http.NewRequest("GET", path, http.NoBody)
		if from != "" {
			r.Header.Set(HeaderFrom, from)
		}
		in.ServeHTTP(w, r)
		return w.result()
	}
	if r := do("/v1/peer/hello", "other"); r.StatusCode != 200 || r.Header.Get("X-Warren-From") != "other" {
		t.Errorf("good request: %d", r.StatusCode)
	}
	for _, from := range []string{"", "me", "Bad Name", "a.b"} {
		if r := do("/v1/peer/hello", from); r.StatusCode != 400 || r.Header.Get(apierr.Header) != apierr.BadRequest {
			t.Errorf("from %q: %d", from, r.StatusCode)
		}
	}
	for _, p := range []string{"/v1/service/self", "/v1/transport/hello", "/nope", "/vx/peer/hello", "/v/peer/hello", "/v1/peers/hello"} {
		if r := do(p, "other"); r.StatusCode != 404 || r.Header.Get(apierr.Header) != apierr.Unsupported {
			t.Errorf("%s: %d %s", p, r.StatusCode, r.Header.Get(apierr.Header))
		}
	}
	// Peer paths of every protocol version are carried; what to do with a
	// version is the node's business.
	if r := do("/v2/peer/hello", "other"); r.StatusCode != 200 {
		t.Errorf("/v2/peer/hello: %d", r.StatusCode)
	}
	if !IsPeerPath("/v12/peer/x") || IsPeerPath("/v1/peer") || IsPeerPath("/v1234567/peer/x") {
		t.Error("IsPeerPath")
	}
	// Without a handler Warren is starting.
	if r := func() *http.Response {
		w := newRec()
		r, _ := http.NewRequest("GET", "/v1/peer/hello", http.NoBody)
		r.Header.Set(HeaderFrom, "other")
		NewIncoming("me").ServeHTTP(w, r)
		return w.result()
	}(); r.Header.Get(apierr.Header) != apierr.NotReady {
		t.Error("not-ready expected")
	}
}

// testBackend is a transport for the wire tests.
type testBackend struct {
	hello  Hello
	peers  PeerList
	carry  func(r *http.Request, to string, req *Request) (*Response, error)
	deadMs chan int
}

func (b *testBackend) Hello(context.Context) Hello { return b.hello }
func (b *testBackend) Peers(done <-chan struct{}, since string, wait time.Duration) PeerList {
	if since == b.peers.Version {
		select {
		case <-done:
		case <-time.After(wait):
		}
	}
	return b.peers
}
func (b *testBackend) Carry(r *http.Request, to string, req *Request) (*Response, error) {
	if b.deadMs != nil {
		ms, _ := strconv.Atoi(r.Header.Get(HeaderDeadline))
		b.deadMs <- ms
	}
	return b.carry(r, to, req)
}

func newBackend() *testBackend {
	return &testBackend{
		hello: Hello{Version: 1, Node: "me", MaxBody: MinMaxBody, Name: "test"},
		peers: PeerList{Version: "1", Peers: []string{"other"}},
		carry: func(r *http.Request, to string, req *Request) (*Response, error) {
			h := http.Header{"X-Warren-To": {to}, "X-Warren-Echo": {req.Header.Get("X-Warren-Thing")}, HeaderTo: {"x"}}
			return &Response{Status: 201, Header: h, Body: append([]byte(req.Method+" "+req.URI+" "), req.Body...)}, nil
		},
		deadMs: make(chan int, 10),
	}
}

// startManaged runs the transport side of a Managed link in-process on the
// two descriptors Prepare returns, and returns a client that plays the
// transport's network side of link B.
func startManaged(t *testing.T, b *testBackend) (*Managed, *Client) {
	t.Helper()
	m := NewManaged("me")
	t.Cleanup(func() { m.Close() })
	files, release, err := m.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ca, err := net.FileConn(files[0])
	if err != nil {
		t.Fatal(err)
	}
	cb, err := net.FileConn(files[1])
	if err != nil {
		t.Fatal(err)
	}
	go ServeConn(ca, TransportHandler(b))
	t.Cleanup(func() { ca.Close() })
	return m, NewClient(func(context.Context) (net.Conn, error) { return cb, nil })
}

func TestManagedWire(t *testing.T) {
	b := newBackend()
	m, toWarren := startManaged(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	h, err := m.Hello(ctx)
	if err != nil || CheckHello(h, "me") != nil || h.Name != "test" {
		t.Fatalf("hello: %+v %v", h, err)
	}
	pl, err := m.Peers(ctx, "", 0)
	if err != nil || len(pl.Peers) != 1 {
		t.Fatalf("peers: %+v %v", pl, err)
	}
	// Long poll returns after the wait with the same list.
	start := time.Now()
	if _, err := m.Peers(ctx, "1", time.Second); err != nil || time.Since(start) < 800*time.Millisecond {
		t.Errorf("long poll: %v after %v", err, time.Since(start))
	}

	resp, err := m.Carry(ctx, "other", &Request{Method: "POST", URI: "/v1/peer/call?a=b",
		Header: http.Header{"X-Warren-Thing": {"v"}, HeaderTo: {"evil"}}, Body: []byte("body"),
		Deadline: time.Now().Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 201 || string(resp.Body) != "POST /v1/peer/call?a=b body" {
		t.Errorf("answer: %d %q", resp.Status, resp.Body)
	}
	if resp.Header.Get("X-Warren-To") != "other" || resp.Header.Get("X-Warren-Echo") != "v" {
		t.Errorf("headers: %v", resp.Header)
	}
	if resp.Header.Get(HeaderTo) != "" {
		t.Error("hop header in the answer")
	}
	if ms := <-b.deadMs; ms < 2000 || ms > 3000 {
		t.Errorf("deadline header %d", ms)
	}

	// Errors: not-sent and lost pass through with their kind.
	b.carry = func(*http.Request, string, *Request) (*Response, error) {
		return nil, &CarryError{Kind: NotSent, Detail: "no route"}
	}
	_, err = m.Carry(ctx, "other", &Request{Method: "GET", URI: "/v1/peer/hello"})
	var ce *CarryError
	if !errors.As(err, &ce) || ce.Kind != NotSent {
		t.Errorf("not-sent: %v", err)
	}
	b.carry = func(*http.Request, string, *Request) (*Response, error) { return nil, errors.New("boom") }
	_, err = m.Carry(ctx, "other", &Request{Method: "GET", URI: "/v1/peer/hello"})
	if !errors.As(err, &ce) || ce.Kind != Lost {
		t.Errorf("lost: %v", err)
	}
	// Over max_body: not-sent without asking the transport.
	_, err = m.Carry(ctx, "other", &Request{Method: "POST", URI: "/v1/peer/call", Body: make([]byte, MinMaxBody+1)})
	if !errors.As(err, &ce) || ce.Kind != NotSent {
		t.Errorf("too large: %v", err)
	}
	// Only peer paths are carried.
	if _, err := m.Carry(ctx, "other", &Request{Method: "GET", URI: "/v1/service/self"}); err == nil {
		t.Error("non-peer path carried")
	}

	// Link B: requests from the network reach Warren's handler with From.
	m.Handle(okHandler())
	req, _ := http.NewRequest("POST", base+"/v1/peer/call", strings.NewReader("in"))
	req.Header.Set(HeaderFrom, "other")
	req.Header.Set("X-Warren-Thing", "t")
	r, err := toWarren.do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if string(body) != "in" || r.Header.Get("X-Warren-From") != "other" || r.Header.Get("X-Warren-Leak") != "" {
		t.Errorf("link B: %q %v", body, r.Header)
	}
	// Unknown paths on link B and on link A answer unsupported.
	for _, c := range []struct {
		cl   *Client
		path string
	}{{toWarren, "/v1/service/self"}, {m.Client, "/v1/nothing"}} {
		req, _ := http.NewRequest("GET", base+c.path, nil)
		req.Header.Set(HeaderFrom, "other")
		r, err := c.cl.do(req)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != 404 || r.Header.Get(apierr.Header) != apierr.Unsupported {
			t.Errorf("%s: %d %q", c.path, r.StatusCode, r.Header.Get(apierr.Header))
		}
	}
}

func TestManagedNotRunning(t *testing.T) {
	m := NewManaged("me")
	_, err := m.Carry(context.Background(), "other", &Request{Method: "GET", URI: "/v1/peer/hello"})
	var ce *CarryError
	if !errors.As(err, &ce) || ce.Kind != NotSent {
		t.Errorf("want not-sent, got %v", err)
	}
	if _, err := m.Hello(context.Background()); err == nil {
		t.Error("hello without a transport")
	}
}

func shortDir(t *testing.T) string {
	d, err := os.MkdirTemp("", "ln")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func TestExternalRefusesSameUser(t *testing.T) {
	dir := shortDir(t)
	linkA, linkB := filepath.Join(dir, "a.sock"), filepath.Join(dir, "b.sock")
	// A transport running as the same user as the test (and so as Warren).
	ln, err := net.Listen("unix", linkA)
	if err != nil {
		t.Fatal(err)
	}
	go ServeListener(ln, TransportHandler(newBackend()), nil)
	defer ln.Close()

	e, err := NewExternal("me", linkA, linkB, os.Geteuid()+1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Link A: Warren will not talk to a transport of its own user.
	if _, err := e.Hello(ctx); err == nil || !strings.Contains(err.Error(), "another user") {
		t.Errorf("link A same user accepted: %v", err)
	}
	// Link B: the same user connecting is refused.
	e.Handle(okHandler())
	c := NewClient(func(ctx context.Context) (net.Conn, error) { return net.Dial("unix", linkB) })
	req, _ := http.NewRequest("GET", base+"/v1/peer/hello", nil)
	req.Header.Set(HeaderFrom, "other")
	// The connection is closed at accept, before any request is read.
	if r, err := c.do(req); err == nil {
		r.Body.Close()
		t.Errorf("link B same user was served: %d", r.StatusCode)
	}
}

func TestExternalUserRule(t *testing.T) {
	dir := shortDir(t)
	other := os.Geteuid() + 1
	e, err := NewExternal("me", filepath.Join(dir, "a"), filepath.Join(dir, "b"), other, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if e.userOK(os.Geteuid()) || !e.userOK(other) || e.userOK(other+1) {
		t.Error("user rule wrong")
	}
}

func TestManyConcurrentStreams(t *testing.T) {
	// Both sides allow at least MinStreams concurrent streams: far more than
	// the HTTP/2 defaults, over the single connection of a managed link.
	b := newBackend()
	b.peers.Version = "1"
	m, _ := startManaged(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := m.Hello(ctx); err != nil { // the single connection is made by the first request
		t.Fatal(err)
	}
	const n = 600
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := m.Peers(ctx, "1", 1*time.Second) // a held long poll each
			errs <- err
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
	}
}

func TestStreamSetting(t *testing.T) {
	if MinStreams < 1000 || h2().MaxConcurrentStreams < 1000 {
		t.Error("links must allow at least 1000 streams")
	}
}

func TestExternalRequiresAnotherUser(t *testing.T) {
	dir := shortDir(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := NewExternal("me", filepath.Join(dir, "a"), filepath.Join(dir, "b"), os.Geteuid(), log); err == nil {
		t.Error("a transport uid equal to Warren's own was accepted")
	}
	if _, err := NewExternal("me", filepath.Join(dir, "a"), filepath.Join(dir, "b"), -1, log); err == nil {
		t.Error("a missing transport uid was accepted")
	}
	e, err := NewExternal("me", filepath.Join(dir, "a"), filepath.Join(dir, "b"), os.Geteuid()+1, log)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	// The socket is world-connectable so a transport of another user can
	// reach it; the uid check at accept is what keeps everyone else out.
	fi, err := os.Stat(filepath.Join(dir, "b"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o666 {
		t.Errorf("link_b mode is %v, want 0666", fi.Mode().Perm())
	}
}

// carryClient is a client to a plain unix socket server running h.
func carryClient(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	dir := shortDir(t)
	p := filepath.Join(dir, "t.sock")
	ln, err := net.Listen("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	var protos http.Protocols
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: h, Protocols: &protos}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return NewClient(func(ctx context.Context) (net.Conn, error) { return net.Dial("unix", p) })
}

func TestCarryEnforcesItsDeadline(t *testing.T) {
	c := carryClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	start := time.Now()
	_, err := c.Carry(context.Background(), "x", &Request{Method: "GET", URI: "/v1/peer/hello",
		Deadline: time.Now().Add(300 * time.Millisecond)})
	var ce *CarryError
	if !errors.As(err, &ce) || ce.Kind != Lost {
		t.Errorf("want lost, got %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("Carry ignored its deadline: took %v", d)
	}
}

func TestCarryFailsOnAnAnswerOverTheLimit(t *testing.T) {
	big := make([]byte, maxAnswer+10)
	c := carryClient(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(big) })
	_, err := c.Carry(context.Background(), "x", &Request{Method: "GET", URI: "/v1/peer/hello",
		Deadline: time.Now().Add(10 * time.Second)})
	var ce *CarryError
	if !errors.As(err, &ce) || ce.Kind != Lost {
		t.Errorf("want lost for an oversized answer, got %v", err)
	}
	// Exactly the limit is fine.
	ok := make([]byte, maxAnswer)
	c2 := carryClient(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(ok) })
	resp, err := c2.Carry(context.Background(), "x", &Request{Method: "GET", URI: "/v1/peer/hello",
		Deadline: time.Now().Add(10 * time.Second)})
	if err != nil || len(resp.Body) != maxAnswer {
		t.Errorf("an answer of exactly the limit: %v", err)
	}
}

func TestOnlyAllowListedHeadersAreCarriedBothWays(t *testing.T) {
	in := http.Header{
		"Content-Type": {"application/json"}, "X-Warren-Thing": {"t"}, "x-warren-lower": {"l"},
		HeaderTo: {"evil"}, HeaderFrom: {"evil"}, "X-Warren-Transport-Other": {"evil"},
		"Content-Length": {"999"}, "Connection": {"close"}, "Transfer-Encoding": {"chunked"},
		"Keep-Alive": {"1"}, "Upgrade": {"x"}, "Set-Cookie": {"a=b"}, "Authorization": {"secret"}, "Accept": {"*/*"},
	}
	got := carryHeaders(in)
	for _, k := range []string{"Content-Type", "X-Warren-Thing", "X-Warren-Lower"} {
		if got.Get(k) == "" {
			t.Errorf("%s was dropped", k)
		}
	}
	for k := range got {
		switch k {
		case "Content-Type", "X-Warren-Thing", "X-Warren-Lower":
		default:
			t.Errorf("%s travelled", k)
		}
	}

	// Over the wire, both ways.
	b := newBackend()
	var seen http.Header
	b.carry = func(r *http.Request, to string, req *Request) (*Response, error) {
		seen = req.Header
		return &Response{Status: 200, Header: in.Clone(), Body: []byte("x")}, nil
	}
	m, _ := startManaged(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := m.Carry(ctx, "other", &Request{Method: "POST", URI: "/v1/peer/call", Header: in.Clone(),
		Body: []byte("b"), Deadline: time.Now().Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	for name, h := range map[string]http.Header{"request": seen, "response": resp.Header} {
		for k := range h {
			if k != "Content-Type" && !strings.HasPrefix(k, "X-Warren-") || strings.HasPrefix(k, PrefixTransport) {
				t.Errorf("%s carried %s", name, k)
			}
		}
		if h.Get("X-Warren-Thing") != "t" {
			t.Errorf("%s lost X-Warren-Thing: %v", name, h)
		}
	}
}

func TestExternalLinkBIsServedAgainAfterItsFileIsRemoved(t *testing.T) {
	dir := shortDir(t)
	linkB := filepath.Join(dir, "b")
	e, err := NewExternal("me", filepath.Join(dir, "a"), linkB, os.Geteuid()+1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := os.Remove(linkB); err != nil {
		t.Fatal(err)
	}
	e.Resync()
	if _, err := os.Lstat(linkB); err != nil {
		t.Errorf("link_b was not made again: %v", err)
	}
}

type countingBackend struct {
	*testBackend
	hellos atomic.Int64
}

func (c *countingBackend) Hello(ctx context.Context) Hello {
	c.hellos.Add(1)
	return c.testBackend.Hello(ctx)
}

func TestMaxBodyIsAskedOnce(t *testing.T) {
	b := &countingBackend{testBackend: &testBackend{hello: Hello{Version: 1, Node: "n", MaxBody: MinMaxBody},
		carry: func(*http.Request, string, *Request) (*Response, error) { return &Response{Status: 200}, nil }}}
	h := TransportHandler(b)
	for i := 0; i < 5; i++ {
		r := httptest.NewRequest("GET", "/v1/peer/hello", nil)
		r.Header.Set(HeaderTo, "other")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("carry: %d", w.Code)
		}
	}
	if n := b.hellos.Load(); n != 1 {
		t.Errorf("Hello was called %d times for 5 carried requests", n)
	}
}

func TestHelloThatAnswersBadlyIsAnAnswerError(t *testing.T) {
	for name, h := range map[string]http.HandlerFunc{
		"status": func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", 500) },
		"body":   func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("<html>")) },
	} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go ServeListener(ln, h, nil)
		c := NewClient(func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
		})
		_, err = c.Hello(context.Background())
		if err == nil || !IsAnswerError(err) {
			t.Errorf("%s: %v", name, err)
		}
		ln.Close()
	}
	// A transport that cannot be reached is not an answer.
	c := NewClient(func(ctx context.Context) (net.Conn, error) { return nil, errors.New("nobody") })
	if _, err := c.Hello(context.Background()); err == nil || IsAnswerError(err) {
		t.Errorf("unreachable: %v", err)
	}
}

func TestAnswerOverTheLimitIsMarkedTooBig(t *testing.T) {
	big := make([]byte, maxAnswer+10)
	b := &testBackend{hello: Hello{Version: 1, Node: "n", MaxBody: MinMaxBody},
		carry: func(*http.Request, string, *Request) (*Response, error) {
			return &Response{Status: 200, Body: big}, nil
		}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go ServeListener(ln, TransportHandler(b), nil)
	c := NewClient(func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
	})
	_, err = c.Carry(context.Background(), "other", &Request{Method: "GET", URI: "/v1/peer/x"})
	var ce *CarryError
	if !errors.As(err, &ce) || !ce.TooBig {
		t.Errorf("got %v", err)
	}
}
