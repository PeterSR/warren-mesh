package link

import (
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
)

// Incoming is link B: the handler Warren serves to the transport. Only
// /v<N>/peer/ requests are accepted, each with the node the transport vouches
// for, which must be a valid node name and not this node.
type Incoming struct {
	node string
	h    atomic.Pointer[http.Handler]
}

// NewIncoming makes the link B handler for node.
func NewIncoming(node string) *Incoming { return &Incoming{node: node} }

// Set sets the handler behind the checks.
func (in *Incoming) Set(h http.Handler) { in.h.Store(&h) }

func (in *Incoming) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !IsPeerPath(r.URL.Path) {
		apierr.NotFound(w, r)
		return
	}
	from := r.Header.Get(HeaderFrom)
	if !validNode(from) || from == in.node {
		apierr.Write(w, apierr.BadRequest, "X-Warren-Transport-From must be another node's name")
		return
	}
	hp := in.h.Load()
	if hp == nil {
		apierr.Write(w, apierr.NotReady, "Warren is starting")
		return
	}
	// The hop header has done its job; the handler gets the node from the
	// context only.
	r.Header = carryHeaders(r.Header)
	(*hp).ServeHTTP(w, r.WithContext(WithFrom(r.Context(), from)))
}

// Serve serves h2c to every connection ln accepts, closing those admit
// refuses. It returns when ln is closed.
func ServeListener(ln net.Listener, h http.Handler, admit func(net.Conn) bool) error {
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: h, Protocols: &p, HTTP2: h2(), ReadHeaderTimeout: 10 * time.Second}
	return srv.Serve(&admitListener{Listener: ln, admit: admit})
}

type admitListener struct {
	net.Listener
	admit func(net.Conn) bool
}

func (l *admitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.admit == nil || l.admit(c) {
			return c, nil
		}
		_ = c.Close()
	}
}

// oneConn is a listener that hands out one connection, then blocks until
// closed. It lets an http.Server serve a descriptor Warren inherited.
type oneConn struct {
	ch   chan net.Conn
	done chan struct{}
	addr net.Addr
}

func newOneConn(c net.Conn) *oneConn {
	o := &oneConn{ch: make(chan net.Conn, 1), done: make(chan struct{}), addr: c.LocalAddr()}
	o.ch <- c
	return o
}

func (o *oneConn) Accept() (net.Conn, error) {
	select {
	case c := <-o.ch:
		return c, nil
	case <-o.done:
		return nil, net.ErrClosed
	}
}

func (o *oneConn) Close() error {
	select {
	case <-o.done:
	default:
		close(o.done)
	}
	return nil
}

func (o *oneConn) Addr() net.Addr { return o.addr }

// ServeConn serves h2c on one connection, such as a descriptor a managed
// transport inherited, until it closes.
func ServeConn(c net.Conn, h http.Handler) error {
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: h, Protocols: &p, HTTP2: h2(), ReadHeaderTimeout: 10 * time.Second}
	return srv.Serve(newOneConn(c))
}
