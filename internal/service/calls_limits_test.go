package service

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
)

// A call that began as a name the node reserved meanwhile does not go out
// under that name.
func TestPendingCallLosesFromWhenTheNodeReservesTheName(t *testing.T) {
	e := newEnv(t, "")
	host := e.childSock("host", "echo")
	node := e.nodeSock("echo")
	ln, _ := node.serve(1, true)
	target := startEcho(t, e.childSock("kit", "echo"))
	conn, err := net.Dial("unix", host.path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprint(conn, "POST /v1/service/call?to=kit.n1&method=m&version=1&from=admin HTTP/1.1\r\nHost: warren\r\nX-Warren: 1\r\nContent-Length: 1\r\n\r\n")
	eventually(t, "the caller to hold an outflight place", func() bool {
		e.node.mu.Lock()
		defer e.node.mu.Unlock()
		return e.node.socks[sockKey{"host.n1", "echo"}].outflight == 1
	})
	// The call is blocked reading its body, after the first check of from.
	refused(t, node.put(ln, "[]", `["admin"]`))
	expect(t, host.call("kit.n1", "m", "", "&from=admin"), apierr.Refused)
	fmt.Fprint(conn, "x")
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode == 200 || response.Header.Get(apierr.Header) != apierr.Refused {
		t.Fatalf("the claim passed: %d %q", response.StatusCode, body)
	}
	if target.last() != nil && target.last().Get("X-Warren-From") == "admin@n1" {
		t.Errorf("the target was called as admin@n1")
	}
}

// A call answer that its caller does not read does not hold the caller's
// outflight place past the call's deadline.
func TestUnreadAnswerFreesTheOutflightPlaceAtTheDeadline(t *testing.T) {
	e := newEnv(t, "")
	caller := e.childSock("host", "echo")
	target := e.childSock("kit", "echo")
	l, _ := target.serve(1, true)
	e.node.mu.Lock()
	s := e.node.socks[sockKey{"host.n1", "echo"}]
	e.node.mu.Unlock()
	next := parseYAML(t, e.dir, baseYAML)
	next.Attach.Children[0].Services[0].Outflight = 1
	if err := e.node.Reload(next); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("unix", caller.path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprint(conn, "POST /v1/service/call?to=kit.n1&method=m&version=1&timeout_ms=500 HTTP/1.1\r\nHost: warren\r\nX-Warren: 1\r\nContent-Length: 0\r\n\r\n")
	r := target.get("/v1/service/calls?lease=" + l + "&wait=25")
	expectOK(t, r)
	expectOK(t, target.post("/v1/service/answer?lease="+l+"&id="+r.hdr.Get("X-Warren-Call"), strings.Repeat("x", MaxCallBody)))
	// The caller never reads the answer: the socket's buffer fills.
	eventually(t, "the place to be free after the deadline", func() bool {
		e.node.mu.Lock()
		defer e.node.mu.Unlock()
		return s.outflight == 0
	})
	// And the place is usable again.
	if r := caller.call("kit.n1", "m", "", ""); r.code() == apierr.Busy {
		t.Errorf("still busy: %s", r.body)
	}
}

type gateBody struct {
	prefix  *bytes.Reader
	waiting chan struct{}
	release chan struct{}
}

func (b *gateBody) Read(p []byte) (int, error) {
	if b.prefix.Len() > 0 {
		return b.prefix.Read(p)
	}
	select {
	case <-b.waiting:
	default:
		close(b.waiting)
	}
	<-b.release
	return 0, io.EOF
}
func (b *gateBody) Close() error { return nil }

// A call that expired while its answer was still being read keeps its places
// until the reader has let go of the buffer.
func TestExpiredAnswerReaderKeepsItsPlaces(t *testing.T) {
	e := newEnv(t, "", func() { ScanEvery = time.Hour; LeaseGrace = time.Hour })
	n := e.node
	n.mu.Lock()
	s := n.socks[sockKey{"kit.n1", "echo"}]
	s.cfg.Inflight, s.cfg.PerRoot = 1, 1
	l := n.newLease(s, 1)
	c := &call{origin: "n1", poolKey: "n1", done: make(chan result, 1), deadline: time.Now().Add(150 * time.Millisecond)}
	if err := n.enqueue(l, c); err != nil {
		t.Fatal(err)
	}
	l.held = nil
	n.handOut(l, c)
	n.mu.Unlock()
	body := &gateBody{prefix: bytes.NewReader(bytes.Repeat([]byte("x"), MaxCallBody-1)), waiting: make(chan struct{}), release: make(chan struct{})}
	r := httptest.NewRequest("POST", "/v1/service/answer?lease="+l.token+"&id="+c.id, body)
	r.ContentLength = MaxCallBody
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { s.handleAnswer(w, r); close(done) }()
	<-body.waiting
	released := false
	release := func() {
		if !released {
			released = true
			close(body.release)
			<-done
		}
	}
	defer release()
	eventually(t, "the call to expire", func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		return c.state == callDone
	})
	n.mu.Lock()
	c2 := &call{origin: "n1", poolKey: "n1", done: make(chan result, 1), deadline: time.Now().Add(time.Second)}
	err := n.enqueue(l, c2)
	n.mu.Unlock()
	if err == nil || err.Code != apierr.Busy {
		t.Fatalf("a replacement was admitted while the old answer was still held: %v", err)
	}
	release()
	n.mu.Lock()
	err = n.enqueue(l, c2)
	n.mu.Unlock()
	if err != nil {
		t.Errorf("the places were not given back: %v", err)
	}
}
