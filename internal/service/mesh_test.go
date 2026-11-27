package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/link"
)

// Helpers for tests with several nodes on one in-memory network.

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) count(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Count(l.b.String(), sub)
}

var meshStarted = map[*testing.T]bool{}

// mn is one node of a test mesh.
type mn struct {
	*env
	f    *link.Fake
	log  *logBuf
	name string
}

// meshNode starts node name on net with the base config (or y, whose node
// line is renamed).
func meshNode(t *testing.T, net *link.Net, name, y string, opt func(*Options)) *mn {
	t.Helper()
	quickMu.Lock()
	first := !meshStarted[t]
	meshStarted[t] = true
	quickMu.Unlock()
	if first {
		// Before the first node runs: services here publish and then hold
		// their lease without polling, so the grace must not end it.
		quick(t)
		LeaseGrace = time.Hour
		t.Cleanup(func() { quickMu.Lock(); delete(meshStarted, t); quickMu.Unlock() })
	}
	if y == "" {
		y = baseYAML
	}
	f := net.Join(name)
	lb := &logBuf{}
	e := newEnv(t, strings.Replace(y, "node: n1", "node: "+name, 1), func() {
		testLink = f
		testOpt = func(o *Options) {
			o.Log = slog.New(slog.NewTextHandler(lb, &slog.HandlerOptions{Level: slog.LevelDebug}))
			if opt != nil {
				opt(o)
			}
		}
	})
	m := &mn{env: e, f: f, log: lb, name: name}
	eventually(t, name+" to accept its transport", func() bool {
		m.node.mu.Lock()
		defer m.node.mu.Unlock()
		return m.node.helloPassed
	})
	return m
}

func (m *mn) peerInfo(name string) map[string]any {
	for _, p := range m.node.Status()["peers"].([]map[string]any) {
		if p["node"] == name {
			return p
		}
	}
	return nil
}

func (m *mn) peerUp(name string) bool {
	p := m.peerInfo(name)
	return p != nil && p["up"] == true
}

// waitUp waits until every named node is up in each of the nodes' views.
func waitUp(t *testing.T, ms []*mn, names ...string) {
	t.Helper()
	for _, m := range ms {
		for _, n := range names {
			if n == m.name {
				continue
			}
			m := m
			n := n
			eventually(t, m.name+" to see "+n+" up", func() bool { return m.peerUp(n) })
		}
	}
}

// publish takes a lease on a socket and publishes names (the default namespace).
func publish(t *testing.T, c *cl, ns ...string) string {
	t.Helper()
	l, _ := c.serve(1, true)
	refused(t, c.put(l, names(ns...), ""))
	return l
}

// waitAddr waits until the view of c lists addr (or not).
func waitAddr(t *testing.T, c *cl, addr string, want bool) view {
	t.Helper()
	var v view
	eventually(t, "view of "+addr+" = "+strconv.FormatBool(want), func() bool {
		v = c.view()
		return v.has(addr) == want
	})
	return v
}

func (v view) entry(addr string) (origin string, stale bool, ok bool) {
	for _, e := range v.Entries {
		if e.Address == addr {
			return e.Origin, e.Stale, true
		}
	}
	return "", false, false
}

// carried counts the requests a fake was asked to carry whose URI contains sub.
func carried(f *link.Fake, sub string) int {
	n := 0
	for _, r := range f.Carried() {
		if strings.Contains(r.URI, sub) {
			n++
		}
	}
	return n
}

// ---- raw peer requests ----

// raw sends a peer request as node f (which the fake vouches for) to node to.
func raw(t *testing.T, f *link.Fake, to, method, uri string, hdr map[string]string, body string) (*link.Response, error) {
	t.Helper()
	h := http.Header{}
	h.Set("X-Warren-Peer", "1")
	for k, v := range hdr {
		if v == "" {
			h.Del(k)
		} else {
			h.Set(k, v)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return f.Carry(ctx, to, &link.Request{Method: method, URI: uri, Header: h, Body: []byte(body), Deadline: time.Now().Add(5 * time.Second)})
}

func rawCode(t *testing.T, r *link.Response, err error) string {
	t.Helper()
	if err != nil {
		t.Fatalf("carry failed: %v", err)
	}
	return r.Header.Get(apierr.Header)
}

// ---- a scripted peer ----

// scripted is a node that is not a Warren: it joins the network and answers
// the peer protocol as the test says, for peers that lie or misbehave.
type scripted struct {
	t    *testing.T
	f    *link.Fake
	node string

	mu        sync.Mutex
	nodeHdr   string // X-Warren-Node in answers; node if empty
	protoHdr  string // X-Warren-Peer in answers; 1 if empty
	protocols []int
	listNode  string // "node" in the list body; node if empty
	svcs      []string
	entries   string // JSON array
	reserved  string // JSON object
	ver       int
	rawList   string // replaces the whole entries answer when set
	onCall    func(w http.ResponseWriter, r *http.Request)
	hellos    int
	entryErr  string // an error code for the entries answer
}

func newScripted(t *testing.T, net *link.Net, node string) *scripted {
	s := &scripted{t: t, f: net.Join(node), node: node, protocols: []int{1}, svcs: []string{"echo"}, entries: "[]", reserved: "{}"}
	s.f.Handle(http.HandlerFunc(s.serve))
	return s
}

func (s *scripted) set(entries string) {
	s.mu.Lock()
	s.entries = entries
	s.ver++
	s.mu.Unlock()
}

func (s *scripted) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	nodeHdr, protoHdr, listNode := s.nodeHdr, s.protoHdr, s.listNode
	ver, entries, reserved, rawList, onCall := s.ver, s.entries, s.reserved, s.rawList, s.onCall
	protocols, svcs := s.protocols, s.svcs
	s.mu.Unlock()
	if nodeHdr == "" {
		nodeHdr = s.node
	}
	if protoHdr == "" {
		protoHdr = "1"
	}
	if listNode == "" {
		listNode = s.node
	}
	w.Header().Set("X-Warren-Node", nodeHdr)
	w.Header().Set("X-Warren-Peer", protoHdr)
	rest := strings.TrimPrefix(r.URL.Path, "/v1/peer/")
	// A poll with the current version waits a moment, like a long poll.
	wait := func() {
		if r.URL.Query().Get("since") == "v"+strconv.Itoa(ver) {
			select {
			case <-time.After(150 * time.Millisecond):
			case <-r.Context().Done():
			}
		}
	}
	switch r.Method + " " + rest {
	case "GET hello":
		s.mu.Lock()
		s.hellos++
		s.mu.Unlock()
		b, _ := json.Marshal(map[string]any{"node": listNode, "version": "9.9.9", "peer_protocol": protocols, "features": []string{"x-test"}})
		w.Write(b)
	case "GET services":
		wait()
		b, _ := json.Marshal(map[string]any{"node": listNode, "version": "v" + strconv.Itoa(ver), "services": svcs})
		w.Write(b)
	case "GET entries":
		wait()
		s.mu.Lock()
		ee := s.entryErr
		s.mu.Unlock()
		if ee != "" {
			apierr.Write(w, ee, "gone")
			return
		}
		if rawList != "" {
			w.Write([]byte(rawList))
			return
		}
		fmt.Fprintf(w, `{"node":%q,"version":"v%d","service":%q,"reserved":%s,"entries":%s}`, listNode, ver, r.URL.Query().Get("service"), reserved, entries)
	case "POST call":
		if onCall != nil {
			onCall(w, r)
			return
		}
		w.Write([]byte("scripted"))
	default:
		apierr.NotFound(w, r)
	}
}

// entryJSON is one wire entry.
func entryJSON(addr, origin string) string {
	return fmt.Sprintf(`{"address":%q,"origin":%q,"data":{"k":1},"stale":false}`, addr, origin)
}

func list(es ...string) string { return "[" + strings.Join(es, ",") + "]" }

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

type netConn = net.Conn

func nettest() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }

func dialTCP(ctx context.Context, addr string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
}
