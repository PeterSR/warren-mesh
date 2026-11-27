package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/link"
)

// Spec is how to start a transport process for one node: the command, and its
// environment (settings, paths of a fake network). Descriptors 3 and 4 are
// added by the harness.
type Spec struct {
	Argv []string
	Env  []string
	Dir  string
}

// Network is a fixture of a network that transports join.
type Network interface {
	// Join registers the nodes on the network, which makes them see and reach
	// each other, and returns how to start a transport for each, in order.
	// The harness starts the processes after Join returns. The checks run side
	// by side, so Join is called from several goroutines at once: it must be
	// safe for that.
	Join(t testing.TB, names ...string) []Spec
}

// Leaver is a Network that can make a node vanish from the others' view.
type Leaver interface {
	Leave(t testing.TB, name string)
}

// Strangers is a Network that can send a request to a node's transport from
// a caller the network does not vouch for (a device that is not a member, or
// is not tagged), which the transport must refuse.
type Strangers interface {
	// It returns the answer (or the error of a connection closed on the
	// caller) and the node name the stranger would have, which must never
	// appear in a peer list.
	Stranger(t testing.TB, to string, req *http.Request) (resp *http.Response, name string, err error)
}

// Injector is a Network that can send a request to a node's transport the way
// another node's transport would, as raw network traffic: the harness uses it
// to check what the transport does with what arrives from the network (a
// forged X-Warren-Transport-From, a path that is not a peer path).
type Injector interface {
	// Inject sends req (its URL path and query, headers and body are
	// meaningful) from the joined node `from` to the joined node `to`.
	Inject(t testing.TB, from, to string, req *http.Request) (*http.Response, error)
}

// syncBuf is a buffer safe for the process's output.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// node is one transport process with the fake Warren on its two links.
type node struct {
	t    testing.TB
	name string
	cmd  *exec.Cmd
	exit chan struct{} // closed when the process has exited
	log  *syncBuf
	a    *http.Client // link A: h2c over the one connection, never dialled again
	// broken is set when either connection is closed: a managed link cannot be
	// dialled again, so that is the end of the transport.
	broken atomic.Bool
	// aConn and bConn are this side's ends of the two links.
	aConn, bConn net.Conn

	mu     sync.Mutex
	routes map[string]http.HandlerFunc // by the first path element after /vN/peer/
	seen   map[string][]*seenReq
}

// seenReq is a request that arrived on link B.
type seenReq struct {
	Method, URI string
	Header      http.Header
	Body        []byte
}

// watchConn notes the first read failure of a link's connection.
type watchConn struct {
	net.Conn
	n *node
}

func (c *watchConn) Read(p []byte) (int, error) {
	k, err := c.Conn.Read(p)
	if err != nil {
		c.n.broken.Store(true)
	}
	return k, err
}

// socketpair makes a connected pair: our end as a net.Conn, the transport's as a file.
func socketpair() (net.Conn, *os.File, error) {
	syscall.ForkLock.RLock()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err == nil {
		syscall.CloseOnExec(fds[0])
		syscall.CloseOnExec(fds[1])
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	mine := os.NewFile(uintptr(fds[0]), "harness-link")
	conn, err := net.FileConn(mine)
	mine.Close()
	if err != nil {
		syscall.Close(fds[1])
		return nil, nil, err
	}
	return conn, os.NewFile(uintptr(fds[1]), "transport-link"), nil
}

// start runs the transport with its two links and a fake Warren behind them.
func start(t testing.TB, spec Spec, name string) *node {
	t.Helper()
	n := &node{t: t, name: name, exit: make(chan struct{}), log: &syncBuf{}, routes: map[string]http.HandlerFunc{}, seen: map[string][]*seenReq{}}
	aConn, aFile, err := socketpair()
	if err != nil {
		t.Fatal(err)
	}
	bConn, bFile, err := socketpair()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(spec.Argv[0], spec.Argv[1:]...)
	cmd.Dir = spec.Dir
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH")}, spec.Env...)
	cmd.ExtraFiles = []*os.File{aFile, bFile} // descriptors 3 and 4
	cmd.Stderr = n.log
	cmd.Stdout = n.log
	if err := cmd.Start(); err != nil {
		t.Fatalf("cannot start %v: %v", spec.Argv, err)
	}
	aFile.Close()
	bFile.Close()
	n.cmd = cmd
	go func() { _ = cmd.Wait(); close(n.exit) }()
	t.Cleanup(n.stop)

	// Link A: this fake Warren is the client, over the one connection it has.
	var used atomic.Bool
	n.aConn, n.bConn = aConn, bConn
	wa := &watchConn{Conn: aConn, n: n}
	n.a = link.NewH2CClient(func(context.Context) (net.Conn, error) {
		if used.Swap(true) {
			return nil, errors.New("link A is closed or was already used: a managed link cannot be dialled again")
		}
		return wa, nil
	})
	// Link B: this fake Warren serves it.
	wb := &watchConn{Conn: bConn, n: n}
	go func() { _ = link.ServeConn(wb, http.HandlerFunc(n.serveB)) }()
	return n
}

// stop ends the process.
func (n *node) stop() {
	select {
	case <-n.exit:
		return
	default:
	}
	_ = n.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-n.exit:
	case <-time.After(5 * time.Second):
		_ = n.cmd.Process.Kill()
		<-n.exit
	}
}

func (n *node) alive() bool {
	select {
	case <-n.exit:
		return false
	default:
		return true
	}
}

// on sets the handler of requests that arrive on link B under /vN/peer/<tag>/.
func (n *node) on(tag string, h http.HandlerFunc) {
	n.mu.Lock()
	n.routes[tag] = h
	n.mu.Unlock()
}

// requests are what arrived on link B for a tag.
func (n *node) requests(tag string) []*seenReq {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]*seenReq(nil), n.seen[tag]...)
}

// serveB is the fake Warren's side of link B: it records the request and
// calls the handler of its tag, or answers 200.
func (n *node) serveB(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	tag := ""
	if i := strings.Index(r.URL.Path, "/peer/"); i >= 0 {
		tag, _, _ = strings.Cut(r.URL.Path[i+len("/peer/"):], "/")
	}
	n.mu.Lock()
	n.seen[tag] = append(n.seen[tag], &seenReq{r.Method, r.RequestURI, r.Header.Clone(), body})
	h := n.routes[tag]
	n.mu.Unlock()
	w.Header().Set("X-Warren-Node", n.name)
	if h == nil {
		_, _ = w.Write([]byte("ok"))
		return
	}
	h(w, r.WithContext(context.WithValue(r.Context(), bodyKey{}, body)))
}

type bodyKey struct{}

// bodyOf is the body of a request served by serveB (already read).
func bodyOf(r *http.Request) []byte {
	b, _ := r.Context().Value(bodyKey{}).([]byte)
	return b
}

// reply is an answer that came back on link A.
type reply struct {
	status int
	h      http.Header
	body   []byte
}

func (r *reply) transportError() string { return r.h.Get(link.HeaderError) }

func (r *reply) String() string {
	b := r.body
	if len(b) > 200 {
		b = b[:200]
	}
	return fmt.Sprintf("%d (transport error %q, X-Warren-Error %q) %q", r.status, r.transportError(), r.h.Get("X-Warren-Error"), b)
}

// get makes a GET on link A.
func (n *node) get(ctx context.Context, uri string) (*reply, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "http://transport"+uri, nil)
	if err != nil {
		return nil, err
	}
	return n.do(req)
}

func (n *node) do(req *http.Request) (*reply, error) {
	resp, err := n.a.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &reply{resp.StatusCode, resp.Header, b}, nil
}

// carry asks the transport to carry a request to node `to`, with a deadline
// that is also sent in X-Warren-Deadline-Ms.
func (n *node) carry(ctx context.Context, to, method, uri string, h http.Header, body []byte, deadline time.Duration) (*reply, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://transport"+uri, rd)
	if err != nil {
		return nil, err
	}
	for k, v := range h {
		req.Header[k] = v
	}
	if to != "" {
		req.Header.Set(link.HeaderTo, to)
	}
	if req.Header.Get(link.HeaderDeadline) == "" {
		req.Header.Set(link.HeaderDeadline, strconv.FormatInt(deadline.Milliseconds(), 10))
	}
	return n.do(req)
}

// mustCarry is carry with the test's context and failure on transport errors
// of the HTTP kind.
func (n *node) mustCarry(t testing.TB, to, method, uri string, h http.Header, body []byte) *reply {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r, err := n.carry(ctx, to, method, uri, h, body, 55*time.Second)
	if err != nil {
		t.Fatalf("%s: carrying %s %s to %s failed on link A: %v\ntransport output:\n%s", n.name, method, uri, to, err, n.log.String())
	}
	return r
}
