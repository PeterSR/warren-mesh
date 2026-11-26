package link

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"
)

// Managed is the link to a transport Warren starts itself. Warren makes two
// connected socket pairs and gives the transport its ends as descriptors 3
// (link A: the transport serves) and 4 (link B: Warren serves). No path
// exists, so no other process can reach the link. Each start of the
// process gets fresh pairs: Prepare makes them.
type Managed struct {
	*Client
	in *Incoming

	mu    sync.Mutex
	connA net.Conn
	srv   *http.Server
	// gen counts the pairs made; a break reported for an older pair is old news.
	gen   int
	fired int // the last generation reported broken
	// broken, if set, is called once when a connection of the current pair
	// breaks while the transport process may still be alive. The daemon has
	// it restart the process, so that Prepare makes new pairs.
	broken func()
}

// OnBroken sets what happens when the connection of a started transport
// breaks. Call it before the first Prepare.
func (m *Managed) OnBroken(f func()) {
	m.mu.Lock()
	m.broken = f
	m.mu.Unlock()
}

// watchConn reports the first read error of a connection to its link.
type watchConn struct {
	net.Conn
	once sync.Once
	fail func()
}

func (c *watchConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil {
		c.once.Do(c.fail)
	}
	return n, err
}

// connBroke is called by a watched connection of pair gen.
func (m *Managed) connBroke(gen int) {
	m.mu.Lock()
	f := m.broken
	if gen != m.gen || gen == m.fired {
		f = nil // old news, or already reported for this pair
	}
	if f != nil {
		m.fired = gen
	}
	m.mu.Unlock()
	if f != nil {
		f()
	}
}

// NewManaged makes a managed link for node.
func NewManaged(node string) *Managed {
	m := &Managed{in: NewIncoming(node)}
	m.Client = NewClient(func(ctx context.Context) (net.Conn, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.connA == nil {
			return nil, errors.New("the transport is not running")
		}
		c := m.connA
		m.connA = nil // one connection per start; HTTP/2 multiplexes
		return c, nil
	})
	return m
}

// Prepare makes new socket pairs for one start of the transport and returns
// the descriptors to hand it, in order (fd 3, fd 4). The caller closes them
// in release once the process has started (or failed to), so Warren holds
// no copy of the transport's ends and notices when it dies.
func (m *Managed) Prepare() (files []*os.File, release func(), err error) {
	aW, aT, err := pair()
	if err != nil {
		return nil, nil, err
	}
	bW, bT, err := pair()
	if err != nil {
		aW.Close()
		aT.Close()
		return nil, nil, err
	}
	m.mu.Lock()
	m.gen++ // first: closing the old pair is not a break of the new one
	gen := m.gen
	m.mu.Unlock()
	m.stopServing()
	m.mu.Lock()
	if m.connA != nil {
		m.connA.Close()
	}
	fail := func() { m.connBroke(gen) }
	aW = &watchConn{Conn: aW, fail: fail}
	bW = &watchConn{Conn: bW, fail: fail}
	m.connA = aW
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: m.in, Protocols: &p, HTTP2: h2(), ReadHeaderTimeout: 10 * time.Second}
	m.srv = srv
	m.mu.Unlock()
	m.Client.Reset()
	go func() { _ = srv.Serve(newOneConn(bW)) }()
	files = []*os.File{aT, bT}
	return files, func() { aT.Close(); bT.Close() }, nil
}

// pair makes a connected unix socket pair: Warren's end as a net.Conn and
// the transport's end as a file to inherit.
func pair() (net.Conn, *os.File, error) {
	// Close-on-exec is set under ForkLock, since darwin has no SOCK_CLOEXEC:
	// a process started meanwhile must not inherit the descriptors.
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
	mine := os.NewFile(uintptr(fds[0]), "warren-link")
	conn, err := net.FileConn(mine) // a duplicate
	mine.Close()
	if err != nil {
		syscall.Close(fds[1])
		return nil, nil, err
	}
	return conn, os.NewFile(uintptr(fds[1]), "transport-link"), nil
}

func (m *Managed) stopServing() {
	m.mu.Lock()
	srv := m.srv
	m.srv = nil
	m.mu.Unlock()
	if srv != nil {
		_ = srv.Close()
	}
}

// Down drops the connections after the transport process exited.
func (m *Managed) Down() {
	m.mu.Lock()
	m.gen++ // what breaks from here on is the exit that is being handled
	m.mu.Unlock()
	m.stopServing()
	m.mu.Lock()
	if m.connA != nil {
		m.connA.Close()
		m.connA = nil
	}
	m.mu.Unlock()
	m.Client.Reset()
}

// Handle implements Link.
func (m *Managed) Handle(h http.Handler) { m.in.Set(h) }

// Close implements Link.
func (m *Managed) Close() error {
	m.Down()
	return m.Client.Close()
}
