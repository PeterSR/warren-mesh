package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/link"
)

// Options tune the checks that wait.
type Options struct {
	// Idle is how long the managed links are left without a single request
	// before they are used again. It must be longer than 90 seconds to catch
	// the idle timeout of Go's default HTTP transport, which is the usual
	// way to get this wrong. Default 95 s; Short mode uses 5 s.
	Idle time.Duration
	// Slow is how long the receiving side holds an answer, to check that the
	// transport imposes no shorter timeout of its own than the deadline it
	// was given. Default 35 s (the usual choices are 10, 15, 30 s); Short
	// mode uses 3 s.
	Slow time.Duration
	// Burst is how many concurrent requests are sent to a node to exceed what
	// a transport takes from one caller, to check busy and that long polls
	// still get through. 0 skips the check. A transport with limits of its
	// own must set it above them.
	Burst int
	// Name, if set, is what hello must say in "name" (optional in the
	// contract).
	Name string
}

func (o *Options) defaults() {
	short := testing.Short()
	if o.Idle == 0 {
		o.Idle = 95 * time.Second
		if short {
			o.Idle = 5 * time.Second
		}
	}
	if o.Slow == 0 {
		o.Slow = 35 * time.Second
		if short {
			o.Slow = 3 * time.Second
		}
	}
}

// Run checks the transport that nw starts.
func Run(t *testing.T, nw Network, o Options) {
	o.defaults()
	// Every check uses nodes of its own where it must not be disturbed (the
	// idle links, the peer list) and shares w1 and w2 otherwise, each check
	// under its own path tag. They all run side by side.
	specs := nw.Join(t, "w1", "w2")
	w1, w2 := start(t, specs[0], "w1"), start(t, specs[1], "w2")
	h := &harness{t: t, nw: nw, o: o, w1: w1, w2: w2}
	h.waitUp()

	t.Run("checks", func(t *testing.T) {
		for _, c := range []struct {
			name string
			fn   func(*testing.T)
		}{
			{"hello", h.hello},
			{"unsupported-paths", h.unsupported},
			{"non-dumpable", h.nonDumpable},
			{"carry-methods-paths-headers", h.carry},
			{"carry-answers-pass-through", h.passThrough},
			{"carry-both-directions", h.bothDirections},
			{"carry-many-at-once", h.concurrent},
			{"max-body", h.maxBody},
			{"big-answers", h.bigAnswers},
			{"not-sent", h.notSent},
			{"lost", h.lost},
			{"too-big", h.tooBig},
			{"deadline-is-carried-and-abandoned", h.deadlines},
			{"no-shorter-timeout", h.noShorterTimeout},
			{"busy-and-room-for-polls", h.busy},
			{"network-input", h.networkInput},
			{"unvouched-caller", h.stranger},
			{"exits-when-a-link-closes", h.linkCloses},
			{"peers", h.peers},
			{"idle-links-stay-open", h.idle},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()
				c.fn(t)
			})
		}
	})
	for _, n := range []*node{w1, w2} {
		if n.broken.Load() || !n.alive() {
			t.Errorf("%s: a link closed or the process exited during the checks:\n%s", n.name, n.log.String())
		}
	}
}

type harness struct {
	t      *testing.T
	nw     Network
	o      Options
	w1, w2 *node
}

// waitUp waits until both transports answer hello and see each other.
func (h *harness) waitUp() {
	h.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for _, n := range []*node{h.w1, h.w2} {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			r, err := n.get(ctx, "/v1/transport/hello")
			cancel()
			if err == nil && r.status == 200 {
				break
			}
			if !n.alive() || time.Now().After(deadline) {
				h.t.Fatalf("%s did not answer hello (%v, %v)\ntransport output:\n%s", n.name, r, err, n.log.String())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	for _, n := range []*node{h.w1, h.w2} {
		other := "w2"
		if n == h.w2 {
			other = "w1"
		}
		for {
			p := peerList(h.t, n, "", 0)
			if contains(p.Peers, other) {
				break
			}
			if time.Now().After(deadline) {
				h.t.Fatalf("%s never saw %s as a peer: %v\n%s", n.name, other, p.Peers, n.log.String())
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

func peerList(t testing.TB, n *node, since string, wait int) link.PeerList {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(wait+30)*time.Second)
	defer cancel()
	r, err := n.get(ctx, fmt.Sprintf("/v1/transport/peers?since=%s&wait=%d", since, wait))
	if err != nil {
		t.Fatalf("%s: peers: %v", n.name, err)
	}
	if r.status != 200 {
		t.Fatalf("%s: peers: %v", n.name, r)
	}
	var raw struct {
		Version string
		Peers   []string
	}
	if err := json.Unmarshal(r.body, &raw); err != nil || raw.Peers == nil {
		t.Fatalf("%s: peers is not {\"version\": ..., \"peers\": [...]}: %v (%q)", n.name, err, r.body)
	}
	return link.PeerList{Version: raw.Version, Peers: raw.Peers}
}

func (h *harness) hello(t *testing.T) {
	for _, n := range []*node{h.w1, h.w2} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		r, err := n.get(ctx, "/v1/transport/hello")
		cancel()
		if err != nil || r.status != 200 {
			t.Fatalf("hello: %v %v", r, err)
		}
		var m map[string]any
		if err := json.Unmarshal(r.body, &m); err != nil {
			t.Fatalf("hello is not JSON: %q", r.body)
		}
		if m["version"] != float64(1) {
			t.Errorf("hello.version = %v, want 1", m["version"])
		}
		if m["node"] != n.name {
			t.Errorf("hello.node = %v, want %s (the name the network gave this node)", m["node"], n.name)
		}
		if mb, _ := m["max_body"].(float64); int(mb) < link.MinMaxBody {
			t.Errorf("hello.max_body = %v, at least %d (4 MiB + 64 KiB) is required", m["max_body"], link.MinMaxBody)
		}
		for _, k := range []string{"name", "software_version"} {
			if v, ok := m[k]; ok {
				if s, isStr := v.(string); !isStr || s == "" {
					t.Errorf("hello.%s, when present, is a non-empty string: %v", k, v)
				}
			}
		}
		if h.o.Name != "" && m["name"] != h.o.Name {
			t.Errorf("hello.name = %v, want %s", m["name"], h.o.Name)
		}
		// The node, then, passes Warren's own check.
		if err := link.CheckHello(link.Hello{Version: 1, Node: n.name, MaxBody: int(m["max_body"].(float64))}, n.name); err != nil {
			t.Error(err)
		}
	}
}

func (h *harness) unsupported(t *testing.T) {
	ctx := context.Background()
	for _, p := range []string{"/", "/v1/transport/nope", "/v2/transport/hello", "/v1/transport", "/v1/service/self"} {
		r, err := h.w1.get(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		if r.status != 404 || r.h.Get("X-Warren-Error") != "unsupported" {
			t.Errorf("GET %s: want 404 with X-Warren-Error: unsupported, got %v", p, r)
		}
	}
	// A path that is not a peer path is not carried, whatever the headers say.
	r := h.w1.mustCarry(t, "w2", "GET", "/v1/service/self", nil, nil)
	if r.status != 404 || r.h.Get("X-Warren-Error") != "unsupported" {
		t.Errorf("a carried request for /v1/service/self: want 404 unsupported, got %v", r)
	}
	if got := h.w2.requests("service"); len(got) != 0 {
		t.Errorf("a non-peer path reached the other Warren: %v", got)
	}
}

// nonDumpable: docs/transport-api.md, Requirements. The process made itself non-dumpable, so other processes
// of the same user cannot attach with ptrace or copy its descriptors.
func (h *harness) nonDumpable(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("PR_SET_DUMPABLE is checked through /proc on Linux")
	}
	if os.Geteuid() == 0 {
		t.Skip("root can read every process; run as another user to check")
	}
	for _, n := range []*node{h.w1, h.w2} {
		pid := n.cmd.Process.Pid
		var st syscall.Stat_t
		if err := syscall.Stat(fmt.Sprintf("/proc/%d/environ", pid), &st); err != nil {
			t.Fatalf("%s: %v", n.name, err)
		}
		if int(st.Uid) == os.Geteuid() {
			t.Errorf("%s: /proc/%d/environ belongs to the transport's own user: the process is dumpable; call prctl(PR_SET_DUMPABLE, 0) first thing", n.name, pid)
		}
		if _, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid)); err == nil {
			t.Errorf("%s: /proc/%d/environ is readable by another process of the same user", n.name, pid)
		}
		if copied := copiesDescriptor(pid, 3); copied {
			t.Errorf("%s: pidfd_getfd copied link A out of the transport", n.name)
		}
	}
}

func hdr(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Add(kv[i], kv[i+1])
	}
	return h
}

// carry: docs/transport-api.md, Link A, carrying a request. The method, path and query, the body, Content-Type and every
// X-Warren-* header except X-Warren-Transport-* are carried; nothing else is;
// the receiving transport sets X-Warren-Transport-From.
func (h *harness) carry(t *testing.T) {
	h.w2.on("carry", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	for _, method := range []string{"GET", "POST", "PUT", "DELETE", "PATCH"} {
		var body []byte
		if method != "GET" && method != "DELETE" {
			body = []byte("body of " + method + "\x00\xff")
		}
		uri := "/v1/peer/carry/" + strings.ToLower(method) + "?x=1&y=a%20b&z"
		r := h.w1.mustCarry(t, "w2", method, uri, hdr(
			"Content-Type", "application/x-test",
			"X-Warren-Peer", "1", "X-Warren-Custom", "kept",
			"X-Warren-Transport-Evil", "stripped", "X-Warren-Transport-From", "forged",
			"X-Custom", "dropped", "Authorization", "dropped", "Cookie", "dropped",
		), body)
		if r.status != 204 {
			t.Errorf("%s: %v", method, r)
			continue
		}
		var got *seenReq
		for _, s := range h.w2.requests("carry") {
			if s.URI == uri {
				got = s
			}
		}
		if got == nil {
			t.Errorf("%s %s never reached the other Warren", method, uri)
			continue
		}
		if got.Method != method || !bytes.Equal(got.Body, body) {
			t.Errorf("%s: arrived as %s with %d body bytes, sent %d", method, got.Method, len(got.Body), len(body))
		}
		if got.Header.Get("Content-Type") != "application/x-test" {
			t.Errorf("%s: Content-Type = %q", method, got.Header.Get("Content-Type"))
		}
		if got.Header.Get("X-Warren-Peer") != "1" || got.Header.Get("X-Warren-Custom") != "kept" {
			t.Errorf("%s: X-Warren-* headers were not carried: %v", method, got.Header)
		}
		if ms := got.Header.Get(link.HeaderDeadline); ms == "" {
			t.Errorf("%s: X-Warren-Deadline-Ms did not arrive", method)
		}
		if got.Header.Get(link.HeaderFrom) != "w1" {
			t.Errorf("%s: X-Warren-Transport-From = %q, want w1 (what the network proved), not what the sender wrote", method, got.Header.Get(link.HeaderFrom))
		}
		for k := range got.Header {
			ck := http.CanonicalHeaderKey(k)
			if strings.HasPrefix(ck, link.PrefixTransport) && ck != link.HeaderFrom {
				t.Errorf("%s: the header %s crossed the network", method, k)
			}
		}
		for _, k := range []string{"X-Custom", "Authorization", "Cookie"} {
			if got.Header.Get(k) != "" {
				t.Errorf("%s: the header %s is not on the allow-list but was carried", method, k)
			}
		}
	}
	// Any protocol version is carried: a transport needs no change for a /v2/peer.
	r := h.w1.mustCarry(t, "w2", "POST", "/v2/peer/carry/next", nil, []byte("v2"))
	if r.status != 204 {
		t.Errorf("/v2/peer: %v", r)
	}
}

// passThrough: docs/transport-api.md, Link A, carrying a request. Whatever status the other Warren answers is the answer,
// with Content-Type, X-Warren-* except X-Warren-Transport-*, and the body;
// the transport errors are only for the transport's own failures.
func (h *harness) passThrough(t *testing.T) {
	h.w2.on("pass", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("c") {
		case "created":
			w.Header().Set("Content-Type", "text/x-answer")
			w.Header().Set("X-Warren-Reply", "yes")
			w.Header().Set("X-Warren-Transport-Reply", "evil")
			w.Header().Set(link.HeaderError, "lost") // a hop header written by the sender must not become one here
			w.Header().Set("X-Custom", "dropped")
			w.WriteHeader(201)
			_, _ = w.Write([]byte("made"))
		case "nf":
			w.Header().Set("X-Warren-Error", "no-target")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"error":"no-target","detail":"x"}`))
		case "unknown":
			w.Header().Set("X-Warren-Error", "unknown")
			w.WriteHeader(502)
			_, _ = w.Write([]byte(`{"error":"unknown"}`))
		case "busy":
			w.Header().Set("X-Warren-Error", "busy")
			w.WriteHeader(429)
		case "empty":
			w.WriteHeader(200)
		}
	})
	r := h.w1.mustCarry(t, "w2", "GET", "/v1/peer/pass/a?c=created", nil, nil)
	if r.status != 201 || string(r.body) != "made" || r.h.Get("Content-Type") != "text/x-answer" || r.h.Get("X-Warren-Reply") != "yes" {
		t.Errorf("created: %v %v", r, r.h)
	}
	for _, k := range []string{"X-Warren-Transport-Reply", "X-Custom"} {
		if r.h.Get(k) != "" {
			t.Errorf("the header %s came back", k)
		}
	}
	if r.transportError() != "" {
		t.Errorf("an answer of the other Warren carries X-Warren-Transport-Error %q, which is for the transport's own failures", r.transportError())
	}
	for _, c := range []struct {
		q      string
		status int
		code   string
	}{{"nf", 404, "no-target"}, {"unknown", 502, "unknown"}, {"busy", 429, "busy"}} {
		r := h.w1.mustCarry(t, "w2", "GET", "/v1/peer/pass/b?c="+c.q, nil, nil)
		if r.status != c.status || r.h.Get("X-Warren-Error") != c.code || r.transportError() != "" {
			t.Errorf("%s: an error answer of the other Warren must pass through unchanged: %v", c.q, r)
		}
	}
	if r := h.w1.mustCarry(t, "w2", "GET", "/v1/peer/pass/e?c=empty", nil, nil); r.status != 200 || len(r.body) != 0 {
		t.Errorf("empty: %v", r)
	}
}

func (h *harness) bothDirections(t *testing.T) {
	h.w1.on("rev", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("from w1 to " + r.Header.Get(link.HeaderFrom)))
	})
	h.w2.on("rev", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("from w2 to " + r.Header.Get(link.HeaderFrom)))
	})
	if r := h.w1.mustCarry(t, "w2", "GET", "/v1/peer/rev/x", nil, nil); r.status != 200 || string(r.body) != "from w2 to w1" {
		t.Errorf("w1 to w2: %v", r)
	}
	if r := h.w2.mustCarry(t, "w1", "GET", "/v1/peer/rev/x", nil, nil); r.status != 200 || string(r.body) != "from w1 to w2" {
		t.Errorf("w2 to w1: %v", r)
	}
}

// concurrent: both links are shared by many requests (HTTP/2 streams), and a
// slow one does not hold the others up.
func (h *harness) concurrent(t *testing.T) {
	release := make(chan struct{})
	h.w2.on("many", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("slow") != "" {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		_, _ = w.Write([]byte(r.URL.Query().Get("i")))
	})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ { // slow ones, in flight throughout
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if r := h.w1.mustCarry(t, "w2", "GET", fmt.Sprintf("/v1/peer/many/s?slow=1&i=s%d", i), nil, nil); r.status != 200 || string(r.body) != fmt.Sprintf("s%d", i) {
				t.Errorf("slow %d: %v", i, r)
			}
		}(i)
	}
	time.Sleep(300 * time.Millisecond)
	var ok atomic.Int32
	var fast sync.WaitGroup
	start := time.Now()
	for i := 0; i < 60; i++ {
		fast.Add(1)
		go func(i int) {
			defer fast.Done()
			if r := h.w1.mustCarry(t, "w2", "GET", fmt.Sprintf("/v1/peer/many/f?i=%d", i), nil, nil); r.status == 200 && string(r.body) == fmt.Sprint(i) {
				ok.Add(1)
			} else {
				t.Errorf("fast %d: %v", i, r)
			}
		}(i)
	}
	fast.Wait()
	if ok.Load() != 60 || time.Since(start) > 30*time.Second {
		t.Errorf("%d of 60 concurrent requests answered correctly in %v while 8 others were held", ok.Load(), time.Since(start))
	}
	close(release)
	wg.Wait()
}

// maxBody: docs/transport-api.md, Errors of the transport itself. A request over max_body is answered 413 with
// X-Warren-Transport-Error: not-sent; one of max_body is carried.
func (h *harness) maxBody(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := h.w1.get(ctx, "/v1/transport/hello")
	if err != nil {
		t.Fatal(err)
	}
	var hello struct {
		Max int `json:"max_body"`
	}
	_ = json.Unmarshal(r.body, &hello)
	if hello.Max < link.MinMaxBody {
		t.Fatalf("max_body %d", hello.Max)
	}
	h.w2.on("max", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "%d", len(bodyOf(r))) })
	body := bytes.Repeat([]byte("m"), hello.Max)
	if r := h.w1.mustCarry(t, "w2", "POST", "/v1/peer/max/exact", nil, body); r.status != 200 || string(r.body) != fmt.Sprint(hello.Max) {
		t.Errorf("a body of max_body (%d) bytes: %v", hello.Max, r)
	}
	before := len(h.w2.requests("max"))
	r = h.w1.mustCarry(t, "w2", "POST", "/v1/peer/max/over", nil, append(body, 'x'))
	if r.status != 413 || r.transportError() != "not-sent" {
		t.Errorf("a body over max_body: want 413 with X-Warren-Transport-Error: not-sent, got %v", r)
	}
	if got := len(h.w2.requests("max")); got != before {
		t.Errorf("a request over max_body reached the other Warren")
	}
}

// bigAnswers: the largest answer Warren expects, a peer list of 4 MiB plus
// headers, comes back intact.
func (h *harness) bigAnswers(t *testing.T) {
	size := 4<<20 + 32<<10
	want := bytes.Repeat([]byte("0123456789abcdef"), size/16)
	h.w2.on("big", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(want) })
	if r := h.w1.mustCarry(t, "w2", "GET", "/v1/peer/big/list", nil, nil); r.status != 200 || !bytes.Equal(r.body, want) {
		t.Errorf("a %d byte answer: %v (%d bytes)", len(want), r.status, len(r.body))
	}
}

// notSent: docs/transport-api.md, Errors of the transport itself. A request that provably never left is 502 with
// X-Warren-Transport-Error: not-sent.
func (h *harness) notSent(t *testing.T) {
	for _, to := range []string{"nobody", "w1" /* this node itself is not a peer */} {
		r := h.w1.mustCarry(t, to, "GET", "/v1/peer/notsent/x", nil, nil)
		if r.status != 502 || r.transportError() != "not-sent" {
			t.Errorf("carrying to %s: want 502 with X-Warren-Transport-Error: not-sent, got %v", to, r)
		}
	}
	// A request without a usable destination is refused and never carried.
	for _, to := range []string{"", "Not A Node", "a.b"} {
		r := h.w1.mustCarry(t, to, "GET", "/v1/peer/notsent/y", nil, nil)
		if r.status < 400 || r.transportError() == "lost" {
			t.Errorf("carrying with To=%q: want a refusal, got %v", to, r)
		}
	}
	if got := h.w2.requests("notsent"); len(got) != 0 {
		t.Errorf("requests that were never to be sent reached w2: %d", len(got))
	}
}

// lost: docs/transport-api.md, Errors of the transport itself. Any other failure after the request was sent is 502 with
// X-Warren-Transport-Error: lost. The other Warren resets the stream.
func (h *harness) lost(t *testing.T) {
	h.w2.on("lost", func(w http.ResponseWriter, r *http.Request) { panic(http.ErrAbortHandler) })
	r := h.w1.mustCarry(t, "w2", "POST", "/v1/peer/lost/x", nil, []byte("sent"))
	if r.status != 502 || r.transportError() != "lost" {
		t.Errorf("a request whose answer broke off: want 502 with X-Warren-Transport-Error: lost, got %v", r)
	}
	if got := h.w2.requests("lost"); len(got) != 1 {
		t.Errorf("the request should have reached the other Warren once, %d times", len(got))
	}
}

// tooBig: docs/transport-api.md, Errors of the transport itself. A request that was sent whose answer is over what the transport
// carries back is 502 with X-Warren-Transport-Error: too-big (or carried, if
// the transport can).
func (h *harness) tooBig(t *testing.T) {
	size := 40 << 20
	h.w2.on("huge", func(w http.ResponseWriter, r *http.Request) {
		chunk := bytes.Repeat([]byte("h"), 1<<20)
		for i := 0; i < size>>20; i++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	})
	r := h.w1.mustCarry(t, "w2", "GET", "/v1/peer/huge/x", nil, nil)
	switch {
	case r.status == 200 && len(r.body) == size:
		// A transport that carries any size is within the contract.
	case r.status == 502 && r.transportError() == "too-big":
	default:
		t.Errorf("a %d byte answer: want it carried whole, or 502 with X-Warren-Transport-Error: too-big, got %d, %q, %d bytes", size, r.status, r.transportError(), len(r.body))
	}
}

// deadlines: docs/transport-api.md, Link A, carrying a request. Every carried request has X-Warren-Deadline-Ms, and the
// transport abandons the request when Warren resets the stream.
func (h *harness) deadlines(t *testing.T) {
	gotCtx := make(chan struct{})
	started := make(chan time.Duration, 1)
	h.w2.on("dl", func(w http.ResponseWriter, r *http.Request) {
		var ms int64
		_, _ = fmt.Sscan(r.Header.Get(link.HeaderDeadline), &ms)
		started <- time.Duration(ms) * time.Millisecond
		<-r.Context().Done() // the receiving Warren's side of the abandoned request
		close(gotCtx)
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = h.w1.carry(ctx, "w2", "GET", "/v1/peer/dl/x", nil, nil, 120*time.Second)
	}()
	select {
	case ms := <-started:
		// Sent with 120000: what arrives is what is left, a little less.
		if ms < 100*time.Second || ms > 120*time.Second {
			t.Errorf("X-Warren-Deadline-Ms arrived as %v for a request sent with 120000 (want 100000 to 120000: the time left)", ms)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the request did not reach the other Warren")
	}
	cancel() // Warren resets the stream
	<-done
	select {
	case <-gotCtx:
	case <-time.After(30 * time.Second):
		t.Error("the transport did not abandon the request at the other end when Warren reset the stream")
	}
}

// noShorterTimeout: docs/transport-api.md, Link A, carrying a request. The transport must not impose a shorter timeout of
// its own than the deadline: an answer that takes Slow still arrives.
func (h *harness) noShorterTimeout(t *testing.T) {
	h.w2.on("slow", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(h.o.Slow):
			_, _ = w.Write([]byte("late"))
		case <-r.Context().Done():
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), h.o.Slow+60*time.Second)
	defer cancel()
	r, err := h.w1.carry(ctx, "w2", "GET", "/v1/peer/slow/x", nil, nil, h.o.Slow+50*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if r.status != 200 || string(r.body) != "late" {
		t.Errorf("an answer that takes %v: the transport must not give up before the deadline of %v: %v", h.o.Slow, h.o.Slow+50*time.Second, r)
	}
}

// busy: docs/transport-api.md, Errors of the transport itself. A transport that refuses a request for its own limits before
// sending it answers 429 with X-Warren-Transport-Error: busy, and keeps room
// for long polls, which are carried while calls are refused.
func (h *harness) busy(t *testing.T) {
	if h.o.Burst == 0 {
		t.Skip("Options.Burst is 0: busy is not checked for this transport")
	}
	// Nodes of their own: the burst fills what w2 takes from one node, and
	// must not refuse the other checks.
	specs := h.nw.Join(t, "b1", "b2")
	b1, b2 := start(t, specs[0], "b1"), start(t, specs[1], "b2")
	(&harness{t: t, nw: h.nw, o: h.o, w1: b1, w2: b2}).waitUp()
	release := make(chan struct{})
	var inHandler atomic.Int32
	b2.on("burst", func(w http.ResponseWriter, r *http.Request) {
		inHandler.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		_, _ = w.Write([]byte("done"))
	})
	var wg sync.WaitGroup
	var ok, busy, other atomic.Int32
	var otherSample atomic.Value
	for i := 0; i < h.o.Burst; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			r, err := b1.carry(ctx, "b2", "POST", "/v1/peer/burst/x", nil, []byte("b"), 80*time.Second)
			switch {
			case err != nil:
				other.Add(1)
				otherSample.Store(err.Error())
			case r.status == 200:
				ok.Add(1)
			case r.status == 429 && r.transportError() == "busy":
				busy.Add(1)
			default:
				other.Add(1)
				otherSample.Store(r.String())
			}
		}()
	}
	// Wait until the refusals have come (they are immediate) and the rest is held.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && int(busy.Load())+int(inHandler.Load()) < h.o.Burst {
		time.Sleep(50 * time.Millisecond)
	}
	// Room for long polls: Warren's hello and list polls are carried while the
	// calls are being refused.
	for _, p := range []string{"/v1/peer/hello", "/v1/peer/services?since=&wait=0", "/v1/peer/entries?service=echo&since=&wait=0"} {
		r := b1.mustCarry(t, "b2", "GET", p, nil, nil)
		if r.status == 429 || r.transportError() != "" {
			t.Errorf("%s was refused while calls filled the transport: %v (it must keep room for Warren's long polls)", p, r)
		}
	}
	close(release)
	wg.Wait()
	if busy.Load() == 0 {
		t.Errorf("%d concurrent requests were all accepted (%d answered); a transport with limits answers 429 with X-Warren-Transport-Error: busy beyond them (set Options.Burst above the limits, or to 0)", h.o.Burst, ok.Load())
	}
	if other.Load() != 0 {
		t.Errorf("%d of the burst ended in something other than 200 or busy, such as %v", other.Load(), otherSample.Load())
	}
}

// networkInput: docs/transport-api.md, Link B. What arrives from the network: X-Warren-Transport-From is
// set from what the network proved, X-Warren-Transport-* headers that came over
// the network are stripped, and only /vN/peer paths are handed to Warren.
func (h *harness) networkInput(t *testing.T) {
	inj, ok := h.nw.(Injector)
	if !ok {
		t.Skip("this Network cannot inject raw network requests")
	}
	h.w2.on("net", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("seen")) })
	req, _ := http.NewRequest("POST", "http://w2/v1/peer/net/x", bytes.NewReader([]byte("b")))
	req.Header.Set("X-Warren-Transport-From", "victim")
	req.Header.Set("X-Warren-Transport-Evil", "evil")
	req.Header.Set("X-Warren-Transport-To", "w9")
	req.Header.Set("X-Warren-Peer", "1")
	req.Header.Set("X-Custom", "dropped")
	resp, err := inj.Inject(t, "w1", "w2", req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("a request from the network: %d", resp.StatusCode)
	}
	got := h.w2.requests("net")
	if len(got) != 1 {
		t.Fatalf("%d requests reached Warren", len(got))
	}
	if got[0].Header.Get(link.HeaderFrom) != "w1" {
		t.Errorf("X-Warren-Transport-From = %q: a transport sets it from what the network proved (w1), never from what the sender wrote", got[0].Header.Get(link.HeaderFrom))
	}
	for k := range got[0].Header {
		if ck := http.CanonicalHeaderKey(k); strings.HasPrefix(ck, link.PrefixTransport) && ck != link.HeaderFrom {
			t.Errorf("the header %s came over the network and was handed to Warren", k)
		}
	}
	if got[0].Header.Get("X-Warren-Peer") != "1" || got[0].Header.Get("X-Custom") != "" {
		t.Errorf("the allow-list on the way in: %v", got[0].Header)
	}
	// Anything that is not a /vN/peer/ request is not handed to Warren.
	before := len(h.w2.requests("service")) + len(h.w2.requests(""))
	for _, p := range []string{"/v1/service/self", "/v1/transport/hello", "/", "/v1/peerx/a"} {
		req, _ := http.NewRequest("GET", "http://w2"+p, nil)
		resp, err := inj.Inject(t, "w1", "w2", req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode < 400 {
			t.Errorf("%s from the network was answered %d", p, resp.StatusCode)
		}
	}
	if after := len(h.w2.requests("service")) + len(h.w2.requests("")); after != before {
		t.Errorf("a path that is not /vN/peer/ reached Warren")
	}
}

// peers: docs/transport-api.md, Link A, peers. The peers long poll: wait seconds at most, the same version
// while nothing changes, a change wakes it, this node is not in its own list.
func (h *harness) peers(t *testing.T) {
	specs := h.nw.Join(t, "p1", "p2")
	p1, p2 := start(t, specs[0], "p1"), start(t, specs[1], "p2")
	_ = p2
	var first link.PeerList
	deadline := time.Now().Add(60 * time.Second)
	for {
		first = peerList(t, p1, "", 0)
		if contains(first.Peers, "p2") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("p1 never saw p2: %v\n%s", first.Peers, p1.log.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
	if contains(first.Peers, "p1") {
		t.Errorf("a node is not its own peer: %v", first.Peers)
	}
	if first.Version == "" {
		t.Error("the peer list has no version")
	}
	// since the current version: nothing changed, so it waits and answers unchanged.
	began := time.Now()
	same := peerList(t, p1, first.Version, 2)
	if d := time.Since(began); d < 1500*time.Millisecond || d > 6*time.Second {
		t.Errorf("a poll with wait=2 at the current version took %v", d)
	}
	if same.Version != first.Version || len(same.Peers) != len(first.Peers) {
		t.Errorf("nothing changed, but the list did: %v then %v", first, same)
	}
	// wait=0 answers at once, whatever since says.
	began = time.Now()
	peerList(t, p1, first.Version, 0)
	if time.Since(began) > 3*time.Second {
		t.Error("wait=0 should answer at once")
	}
	// A new node wakes the poll.
	woke := make(chan link.PeerList, 1)
	go func() { woke <- peerList(t, p1, first.Version, 25) }()
	time.Sleep(500 * time.Millisecond)
	p3specs := h.nw.Join(t, "p3")
	start(t, p3specs[0], "p3")
	select {
	case l := <-woke:
		// A new node is a change: the poll answers with a new version, and
		// does not just run out its wait.
		if l.Version == first.Version {
			t.Errorf("the poll ended at its wait with the version unchanged (%q) after a node joined", l.Version)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the peers long poll did not return")
	}
	for !contains(peerList(t, p1, "", 0).Peers, "p3") {
		if time.Now().After(deadline.Add(60 * time.Second)) {
			t.Fatal("p1 never saw p3")
		}
		time.Sleep(200 * time.Millisecond)
	}
	// A node that goes away leaves the list, when the network can say so.
	if lv, ok := h.nw.(Leaver); ok {
		cur := peerList(t, p1, "", 0)
		lv.Leave(t, "p3")
		for end := time.Now().Add(60 * time.Second); ; {
			l := peerList(t, p1, cur.Version, 25)
			if !contains(l.Peers, "p3") {
				break
			}
			cur = l
			if time.Now().After(end) {
				t.Fatal("p3 left the network but stays in the peers list")
			}
		}
	}
}

// idle: docs/transport-api.md, Requirements. Both connections of a managed link stay open while idle: no
// idle timeout on either side, since a managed link cannot be dialled again
// and a closed one means a transport restart.
func (h *harness) idle(t *testing.T) {
	specs := h.nw.Join(t, "i1", "i2")
	i1, i2 := start(t, specs[0], "i1"), start(t, specs[1], "i2")
	h2 := &harness{t: t, nw: h.nw, o: h.o, w1: i1, w2: i2}
	h2.waitUp()
	// Both links of both nodes are used once first, so that link B's
	// connection exists (the transport's client makes it on its first request)
	// and is among those checked: a request each way crosses link A of the
	// sender and link B of the receiver.
	i1.on("idle", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("i1")) })
	i2.on("idle", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("i2")) })
	if r := i1.mustCarry(t, "i2", "GET", "/v1/peer/idle/before", nil, nil); r.status != 200 || string(r.body) != "i2" {
		t.Fatalf("before idling, i1 to i2: %v", r)
	}
	if r := i2.mustCarry(t, "i1", "GET", "/v1/peer/idle/before", nil, nil); r.status != 200 || string(r.body) != "i1" {
		t.Fatalf("before idling, i2 to i1: %v", r)
	}
	// Nothing is sent on either link of either node.
	t.Logf("leaving the links idle for %v", h.o.Idle)
	select {
	case <-time.After(h.o.Idle):
	case <-i1.exit:
		t.Fatalf("i1 exited while idle:\n%s", i1.log.String())
	case <-i2.exit:
		t.Fatalf("i2 exited while idle:\n%s", i2.log.String())
	}
	for _, n := range []*node{i1, i2} {
		if n.broken.Load() {
			t.Fatalf("%s: a link connection was closed while it was idle (an idle timeout on the transport's side or the other); a managed link cannot be dialled again\n%s", n.name, n.log.String())
		}
	}
	// Both links of both nodes still work.
	if r := i1.mustCarry(t, "i2", "GET", "/v1/peer/idle/x", nil, nil); r.status != 200 || string(r.body) != "i2" {
		t.Errorf("after idling, i1 to i2: %v\n%s\n%s", r, i1.log.String(), i2.log.String())
	}
	if r := i2.mustCarry(t, "i1", "GET", "/v1/peer/idle/x", nil, nil); r.status != 200 || string(r.body) != "i1" {
		t.Errorf("after idling, i2 to i1: %v", r)
	}
}

// stranger: docs/transport-api.md, Link B. A caller the network does not vouch for (not a member, not
// tagged) is refused, and nothing it sends reaches Warren.
func (h *harness) stranger(t *testing.T) {
	st, ok := h.nw.(Strangers)
	if !ok {
		t.Skip("this Network cannot send as an unvouched caller")
	}
	h.w2.on("stranger", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("reached")) })
	// The body announces 1 MiB and then stalls after a few bytes. A transport
	// that refuses a stranger does so before it reads the body, so the refusal
	// arrives while the body is still unsent; one that reads first never answers.
	body := &stalledBody{done: make(chan struct{})}
	defer close(body.done)
	req, _ := http.NewRequest("POST", "http://w2/v1/peer/stranger/x", body)
	req.ContentLength = 1 << 20
	req.Header.Set("X-Warren-Transport-From", "w1") // pretending to be a peer
	type answer struct {
		resp *http.Response
		name string
		err  error
	}
	got := make(chan answer, 1)
	go func() {
		resp, name, err := st.Stranger(t, "w2", req)
		got <- answer{resp, name, err}
	}()
	var a answer
	select {
	case a = <-got:
	case <-time.After(20 * time.Second):
		t.Fatal("a caller the network does not vouch for was not refused before its body was read")
	}
	resp, name, err := a.resp, a.name, a.err
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode < 400 {
			t.Errorf("a caller the network does not vouch for was answered %d", resp.StatusCode)
		}
	} // a connection closed on it is a refusal too
	if got := h.w2.requests("stranger"); len(got) != 0 {
		t.Errorf("a request of an unvouched caller reached Warren")
	}
	// It stays out of the peer lists, however long the transports have to
	// notice it (warren-tailscale looks at the network every 10 s).
	wait := 12 * time.Second
	if testing.Short() {
		wait = 3 * time.Second
	}
	time.Sleep(wait)
	for _, n := range []*node{h.w1, h.w2} {
		if p := peerList(t, n, "", 0); contains(p.Peers, name) {
			t.Errorf("%s lists %s, a device the network does not vouch for, as a peer: %v", n.name, name, p.Peers)
		}
	}
}

// stalledBody is a request body that gives a few bytes and then waits until
// done is closed, to see whether a refusal comes before the body is read.
type stalledBody struct {
	sent bool
	done chan struct{}
}

func (b *stalledBody) Read(p []byte) (int, error) {
	if !b.sent {
		b.sent = true
		return copy(p, "partial body"), nil
	}
	<-b.done
	return 0, io.ErrUnexpectedEOF
}

// linkCloses: docs/transport-api.md, Requirements. When a connection of a managed link closes, the link is
// over (it cannot be dialled again): the transport exits, so that Warren
// starts it again with new descriptors.
func (h *harness) linkCloses(t *testing.T) {
	specs := h.nw.Join(t, "c1", "c2")
	for i, name := range []string{"link A", "link B"} {
		n := start(t, specs[i], []string{"c1", "c2"}[i])
		(&harness{t: t, nw: h.nw, o: h.o, w1: n, w2: n}).waitHello(n)
		if i == 0 {
			n.aConn.Close()
		} else {
			n.bConn.Close()
		}
		select {
		case <-n.exit:
		case <-time.After(10 * time.Second):
			t.Errorf("%s closed, and the transport was still running 10 s later; a managed link cannot be dialled again, so it must exit and be restarted\n%s", name, n.log.String())
		}
	}
}

// waitHello waits until n answers hello.
func (h *harness) waitHello(n *node) {
	h.t.Helper()
	for deadline := time.Now().Add(60 * time.Second); ; {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		r, err := n.get(ctx, "/v1/transport/hello")
		cancel()
		if err == nil && r.status == 200 {
			return
		}
		if !n.alive() || time.Now().After(deadline) {
			h.t.Fatalf("%s did not answer hello (%v, %v)\n%s", n.name, r, err, n.log.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}
