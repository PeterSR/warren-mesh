package link

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Net is an in-memory network of fake links, for tests. It is not a
// supported way to run Warren.
type Net struct {
	mu    sync.Mutex
	nodes map[string]*Fake
	down  map[string]bool
	ver   int
	wake  chan struct{}
}

// NewNet makes an empty network.
func NewNet() *Net {
	return &Net{nodes: map[string]*Fake{}, down: map[string]bool{}, wake: make(chan struct{})}
}

// Fake is one node's link on a Net.
type Fake struct {
	net  *Net
	node string
	in   *Incoming
	// Hello may be changed by tests before the node starts.
	HelloValue Hello
	// Last records what the fake last carried, for tests.
	mu       sync.Mutex
	carried  []Request
	failNext string
}

// Join adds a node to the network.
func (n *Net) Join(node string) *Fake {
	f := &Fake{net: n, node: node, in: NewIncoming(node),
		HelloValue: Hello{Version: APIVersion, Node: node, MaxBody: MinMaxBody, Name: "fake", SoftwareVersion: "test"}}
	n.mu.Lock()
	n.nodes[node] = f
	n.changed()
	n.mu.Unlock()
	return f
}

// JoinDown adds a node that starts out unreachable, so that other nodes do not
// see it until SetDown(node, false), such as once it is ready to answer.
func (n *Net) JoinDown(node string) *Fake {
	n.mu.Lock()
	n.down[node] = true
	n.mu.Unlock()
	return n.Join(node)
}

// SetDown makes a node unreachable (or reachable again): its peers stop
// seeing it and carrying to it fails as not-sent.
func (n *Net) SetDown(node string, down bool) {
	n.mu.Lock()
	n.down[node] = down
	n.changed()
	n.mu.Unlock()
}

// changed bumps the peer list version and wakes long polls. mu held.
func (n *Net) changed() {
	n.ver++
	close(n.wake)
	n.wake = make(chan struct{})
}

func (n *Net) listFor(self string) PeerList {
	var peers []string
	for name := range n.nodes {
		if name != self && !n.down[name] && !n.down[self] {
			peers = append(peers, name)
		}
	}
	sort.Strings(peers)
	return PeerList{Version: strconv.Itoa(n.ver), Peers: peers}
}

// FailNext makes the next Carry fail with this kind (NotSent or Lost).
func (f *Fake) FailNext(kind string) {
	f.mu.Lock()
	f.failNext = kind
	f.mu.Unlock()
}

// Carried returns the requests this fake was asked to carry.
func (f *Fake) Carried() []Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Request(nil), f.carried...)
}

// Hello implements Link.
func (f *Fake) Hello(context.Context) (Hello, error) { return f.HelloValue, nil }

// Peers implements Link.
func (f *Fake) Peers(ctx context.Context, since string, wait time.Duration) (PeerList, error) {
	t := time.NewTimer(wait)
	defer t.Stop()
	for {
		f.net.mu.Lock()
		l := f.net.listFor(f.node)
		wake := f.net.wake
		f.net.mu.Unlock()
		if l.Version != since || wait == 0 {
			return l, nil
		}
		select {
		case <-wake:
		case <-t.C:
			return l, nil
		case <-ctx.Done():
			return l, ctx.Err()
		}
	}
}

// Carry implements Link: it calls the target's handler directly.
func (f *Fake) Carry(ctx context.Context, to string, r *Request) (*Response, error) {
	f.mu.Lock()
	f.carried = append(f.carried, *r)
	fail := f.failNext
	f.failNext = ""
	f.mu.Unlock()
	if fail != "" {
		return nil, &CarryError{Kind: fail, Detail: "injected"}
	}
	if len(r.Body) > f.HelloValue.MaxBody {
		return nil, &CarryError{Kind: NotSent, Detail: "over max_body"}
	}
	f.net.mu.Lock()
	target := f.net.nodes[to]
	down := f.net.down[to] || f.net.down[f.node]
	f.net.mu.Unlock()
	if target == nil || down {
		return nil, &CarryError{Kind: NotSent, Detail: "no such node or no connection"}
	}
	req := httptest.NewRequest(r.Method, r.URI, bytesReader(r.Body)).WithContext(ctx)
	for k, v := range carryHeaders(r.Header) {
		req.Header[k] = v
	}
	req.Header.Set(HeaderFrom, f.node)
	if !r.Deadline.IsZero() {
		req.Header.Set(HeaderDeadline, strconv.FormatInt(time.Until(r.Deadline).Milliseconds(), 10))
	}
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { target.in.ServeHTTP(rec, req); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return nil, &CarryError{Kind: Lost, Detail: ctx.Err().Error()}
	}
	res := rec.Result()
	return &Response{Status: res.StatusCode, Header: carryHeaders(res.Header), Body: rec.Body.Bytes()}, nil
}

// Handle implements Link.
func (f *Fake) Handle(h http.Handler) { f.in.Set(h) }

// Close implements Link.
func (f *Fake) Close() error {
	f.net.mu.Lock()
	delete(f.net.nodes, f.node)
	f.net.changed()
	f.net.mu.Unlock()
	return nil
}
