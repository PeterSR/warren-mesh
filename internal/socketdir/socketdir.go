// Package socketdir serves leaf unix sockets in directories that other
// parties can write: a child's directory is mounted into a container, so
// whoever is in there may leave a symlink, a file or a socket of their own
// where Warren wants to make one. Everything here is built to survive that:
// the directory is opened without following links and held by descriptor,
// the socket is made through that descriptor, its owner is checked, and an
// entry that is not a socket of ours is never removed or followed.
//
// Warren never creates a directory inside a child's directory.
package socketdir

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/PeterSR/warren-mesh/internal/platform"
)

// MaxConns is how many connections one socket accepts at once. The next one
// is closed at once.
const MaxConns = 64

// Euid is the user Warren runs as. Tests replace it to play other owners.
var Euid = os.Geteuid

// Want is one leaf socket that should exist.
type Want struct {
	// Key identifies the socket to the caller and across Sync calls.
	Key  string
	Path string
	// UID, if set, is the only user whose connections reach Handler, as the
	// kernel reports it (SO_PEERCRED). Others go to Refuse.
	UID *int
	// Mode is the socket's permission bits; default 0666, so users in
	// containers can connect and the directory decides who may.
	Mode os.FileMode
	// Admit, if set, must also approve the caller's uid (false when the
	// kernel cannot say). Link sockets use it to refuse Warren's own user.
	Admit func(uid int, known bool) bool
	// H2C serves HTTP/2 with prior knowledge as well as HTTP/1.
	H2C     bool
	Handler http.Handler
	Refuse  http.HandlerFunc
}

type sock struct {
	wmu      sync.Mutex // guards want; never held with Group.mu waiting on requests
	want     Want
	path     string
	dev, ino uint64
	leaf     leaf
	srv      *http.Server
	lim      *limitListener // its live connections, for dropRefused
	cancel   context.CancelFunc
	done     chan struct{}
}

// MaxHeaderBytes is the most request header a socket reads. Warren's own
// requests have a few hundred bytes of header, so Go's default of 1 MiB per
// connection would only be memory for an attacker to hold.
const MaxHeaderBytes = 16 << 10

// Group keeps a set of leaf sockets served, adding, replacing and removing
// them as Sync says.
type Group struct {
	log *slog.Logger

	mu     sync.Mutex
	socks  map[string]*sock
	warn   map[string]string // last warning per key, so each is logged once
	closed bool
}

// NewGroup makes an empty group.
func NewGroup(log *slog.Logger) *Group {
	if log == nil {
		log = slog.Default()
	}
	return &Group{log: log, socks: map[string]*sock{}, warn: map[string]string{}}
}

// Sync makes the served sockets match wants. A socket that is wanted and
// already served, and whose file is still ours, is left alone. One whose
// file was removed or swapped is served again. It returns the wants that
// could not be served, with why; each failure is logged once.
func (g *Group) Sync(wants []Want) map[string]error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		// A Sync that raced with Close must not bring sockets back.
		failed := map[string]error{}
		for _, w := range wants {
			failed[w.Key] = errors.New("the group is closed")
		}
		return failed
	}
	return g.syncLocked(wants)
}

func (g *Group) syncLocked(wants []Want) map[string]error {
	failed := map[string]error{}
	wanted := map[string]bool{}
	for _, w := range wants {
		wanted[w.Key] = true
	}
	var stop []*sock
	for k, s := range g.socks {
		if !wanted[k] {
			stop = append(stop, s)
			delete(g.socks, k)
		}
	}
	g.stopAll(stop)
	for _, w := range wants {
		if s := g.socks[w.Key]; s != nil {
			if s.want.Path == w.Path && sameFile(s.path, s.dev, s.ino) {
				if w.Mode == 0 {
					w.Mode = 0o666
				}
				if g.applyMode(s, w) {
					g.dropRefused(s, w)
					continue
				}
				g.log.Warn("cannot change the mode of a socket; serving a new one", "socket", s.want.Path)
				delete(g.socks, w.Key)
				g.stop(s)
				goto serveNew
			}
			g.log.Info("socket replaced or removed; serving a new one", "socket", s.want.Path)
			delete(g.socks, w.Key)
			g.stop(s)
		}
	serveNew:
		s, err := g.serve(w)
		if err != nil {
			failed[w.Key] = err
			if g.warn[w.Key] != err.Error() {
				g.warn[w.Key] = err.Error()
				g.log.Warn("cannot serve a socket", "socket", w.Path, "err", err)
			}
			continue
		}
		delete(g.warn, w.Key)
		g.socks[w.Key] = s
	}
	for k := range g.warn {
		if !wanted[k] {
			delete(g.warn, k)
		}
	}
	return failed
}

// refuses is whether a want turns away a connection from this uid.
func refuses(w Want, uid int, known bool) bool {
	if w.UID != nil && (!known || uid != *w.UID) {
		return true
	}
	return w.Admit != nil && !w.Admit(uid, known)
}

// dropRefused closes the live connections of s that the want w would refuse:
// the ones open or in the middle of a long poll when a reload narrowed who is
// admitted. Closing ends their requests at once (their contexts end, and an
// HTTP/2 connection takes its streams with it).
func (g *Group) dropRefused(s *sock, w Want) {
	s.lim.mu.Lock()
	var drop []*countedConn
	for c := range s.lim.conns {
		if refuses(w, c.uid, c.known) {
			drop = append(drop, c)
		}
	}
	s.lim.mu.Unlock()
	for _, c := range drop {
		g.log.Info("closed a connection that the socket no longer admits", "socket", w.Path, "uid", c.uid)
		_ = c.Close()
	}
}

// applyMode makes a held socket take the want w: the handler and the uid
// check, and the mode of the file. The order keeps the socket from being
// open wider than its uid check allows at any moment: a mode that loosens is
// set after the new want (with its check) is in force, one that tightens
// before. It reports false if the chmod failed.
func (g *Group) applyMode(s *sock, w Want) bool {
	old := s.want.Mode
	set := func() {
		s.wmu.Lock()
		s.want = w // handlers and uid are read from the want
		s.wmu.Unlock()
	}
	if w.Mode == old {
		set()
		return true
	}
	if w.Mode&^old != 0 { // some bit is added: the want first
		set()
		return s.leaf.chmod(w.Mode) == nil
	}
	// Tightening. If the file was unlinked since the sameFile check, this
	// chmod by descriptor still succeeds, harmlessly, on the file that is no
	// longer reachable by name; the next reconcile finds the name gone and
	// serves the socket again.
	if err := s.leaf.chmod(w.Mode); err != nil {
		return false
	}
	set()
	return true
}

// Serving reports whether key's socket is being served.
func (g *Group) Serving(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.socks[key] != nil
}

// Close stops every socket and removes its file if still ours.
func (g *Group) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	g.syncLocked(nil)
}

func (g *Group) serve(w Want) (*sock, error) {
	if len(w.Path) > platform.MaxSunPath {
		return nil, fmt.Errorf("path is %d bytes, over the %d byte limit of a unix socket path", len(w.Path), platform.MaxSunPath)
	}
	if w.Mode == 0 {
		w.Mode = 0o666
	}
	ln, s, err := listenLeaf(w.Path, w.Mode)
	if err != nil {
		return nil, err
	}
	s.want = w
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	s.srv = &http.Server{
		Handler:           g.handler(s),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    MaxHeaderBytes,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ConnContext:       withPeerCred,
	}
	if w.H2C {
		var p http.Protocols
		p.SetHTTP1(true)
		p.SetUnencryptedHTTP2(true)
		s.srv.Protocols = &p
		s.srv.HTTP2 = &http.HTTP2Config{MaxConcurrentStreams: 1000}
	}
	// A connection from a user the socket does not admit is closed at accept,
	// before it counts against the cap, so strangers cannot fill the cap and
	// shut the real client out.
	seen := map[int]time.Time{} // uid (-1: unknown) -> when its close was last logged; Accept is one goroutine
	logRefused := func(uid int, known bool) {
		if !known {
			uid = -1
		}
		if t, ok := seen[uid]; ok && time.Since(t) < RefuseLogEvery {
			return
		}
		if len(seen) >= 256 {
			clear(seen)
		}
		seen[uid] = time.Now()
		g.log.Warn("closed a connection from a user this socket does not admit", "socket", w.Path, "uid", uid)
	}
	admit := func(c net.Conn) bool {
		s.wmu.Lock()
		want := s.want
		s.wmu.Unlock()
		if want.UID == nil && want.Admit == nil {
			return true
		}
		uid, ok := platform.ConnUID(c)
		if want.UID != nil && (!ok || uid != *want.UID) {
			logRefused(uid, ok)
			return false
		}
		if want.Admit != nil && !want.Admit(uid, ok) {
			logRefused(uid, ok)
			return false
		}
		return true
	}
	s.lim = &limitListener{Listener: ln, max: MaxConns, admit: admit, conns: map[*countedConn]struct{}{}}
	go func() { _ = s.srv.Serve(s.lim); close(s.done) }()
	return s, nil
}

// RefuseLogEvery is the least time between two log lines about the same
// user being turned away from the same socket.
var RefuseLogEvery = time.Minute

// handler checks the caller's uid, if one is required, then hands over.
// Strangers are normally closed at accept already; this check is defence in
// depth for a connection accepted before a Sync changed the wanted uid.
// It reads the want under its own lock, since Sync may update it.
func (g *Group) handler(s *sock) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.wmu.Lock()
		want := s.want
		s.wmu.Unlock()
		// Every request is checked against the current want, so a connection
		// that was open when a reload changed who is admitted is refused on
		// its next request, and closed. A file mode is only looked at when a
		// connection is made.
		if want.UID != nil {
			if uid, ok := PeerUID(r.Context()); !ok || uid != *want.UID {
				w.Header().Set("Connection", "close")
				if want.Refuse != nil {
					want.Refuse(w, r)
				} else {
					http.Error(w, "forbidden", http.StatusForbidden)
				}
				return
			}
		}
		if want.Admit != nil {
			uid, ok := PeerUID(r.Context())
			if !want.Admit(uid, ok) {
				w.Header().Set("Connection", "close")
				if want.Refuse != nil {
					want.Refuse(w, r)
				} else {
					http.Error(w, "forbidden", http.StatusForbidden)
				}
				return
			}
		}
		want.Handler.ServeHTTP(w, r)
	})
}

// stopAll ends the sockets in parallel: each can take up to its shutdown
// wait, and a node with many children would otherwise wait for the sum.
func (g *Group) stopAll(socks []*sock) {
	var wg sync.WaitGroup
	for _, s := range socks {
		wg.Add(1)
		go func() { defer wg.Done(); g.stop(s) }()
	}
	wg.Wait()
}

// stop ends a socket: its requests at once, long polls too, then the
// listener, then its file if it is still the one we made. Called with mu
// held; it waits only briefly.
func (g *Group) stop(s *sock) {
	s.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if s.srv.Shutdown(ctx) != nil {
		_ = s.srv.Close()
	}
	<-s.done
	if sameFile(s.path, s.dev, s.ino) {
		s.leaf.unlink()
	}
	s.leaf.release()
}

// leaf is what listenLeaf keeps of the file it made: how to remove it and
// let go of what pins it.
type leaf struct {
	unlink  func()
	release func()
	// chmod changes the mode of the socket file this leaf made, by the
	// descriptor held for it where there is one.
	chmod func(os.FileMode) error
}

// sameFile reports whether p is still the socket with this device and
// inode, without following a link.
func sameFile(p string, dev, ino uint64) bool {
	fi, err := os.Lstat(p)
	if err != nil || fi.Mode()&os.ModeSocket == 0 {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && uint64(st.Dev) == dev && uint64(st.Ino) == ino
}

// limitListener closes connections beyond max at once.
type limitListener struct {
	net.Listener
	max int
	// admit, if set, is asked about each connection first; a connection it
	// refuses is closed without counting.
	admit func(net.Conn) bool
	mu    sync.Mutex
	n     int
	// conns are the live connections with the uid they came from, so a Sync
	// that narrows who is admitted can close those it would now refuse.
	conns map[*countedConn]struct{}
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		// Admitted under the same lock that registers the connection, so one
		// admitted under an old want cannot slip past dropRefused (which takes
		// this lock). The lock order is mu, then the socket's want lock.
		l.mu.Lock()
		if l.admit != nil && !l.admit(c) {
			l.mu.Unlock()
			_ = c.Close()
			continue
		}
		if l.n >= l.max {
			l.mu.Unlock()
			_ = c.Close()
			continue
		}
		l.n++
		cc := &countedConn{Conn: c, l: l}
		cc.uid, cc.known = platform.ConnUID(c)
		l.conns[cc] = struct{}{}
		l.mu.Unlock()
		return cc, nil
	}
}

type countedConn struct {
	net.Conn
	l     *limitListener
	once  sync.Once
	uid   int
	known bool
}

func (c *countedConn) Close() error {
	c.once.Do(func() {
		c.l.mu.Lock()
		c.l.n--
		delete(c.l.conns, c)
		c.l.mu.Unlock()
	})
	return c.Conn.Close()
}

// DirState is what AdjustDir found: the permission bits and the owner.
type DirState struct {
	Perm os.FileMode
	Uid  int
}

// ErrNotPlainDir is AdjustDir's answer for a link or a non-directory.
var ErrNotPlainDir = errors.New("not a plain directory")

// ListDirs lists the real directories in dir: links to directories are not
// children.
func ListDirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		// A name with a leading dot is never a child label: the staging
		// directories of a socket being made are such, and are not reported.
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// DirWarnings are the problems with a directory that others could use to
// replace a socket in it, or with the directory above it.
func DirWarnings(dir string) []string {
	var w []string
	fi, err := os.Lstat(dir)
	if err != nil {
		return nil
	}
	if fi.Mode().Perm()&0o022 != 0 && fi.Mode()&os.ModeSticky == 0 {
		w = append(w, "writable by group or others without the sticky bit, so they can replace sockets in it; use an ACL for Warren's user, or chmod 1777")
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != Euid() && st.Uid != 0 {
		w = append(w, "owned by a user other than Warren and root, who can therefore rename or replace anything in it")
	}
	return w
}

// UpperWarnings are the problems with attach.dir or services.dir itself.
func UpperWarnings(dir string, haveUID bool) []string {
	var w []string
	fi, err := os.Stat(dir)
	if err != nil {
		return nil
	}
	if fi.Mode().Perm()&0o022 != 0 {
		w = append(w, "writable by group or others, so they can add, rename or replace directories")
	}
	if fi.Mode().Perm()&0o001 != 0 && !haveUID {
		w = append(w, "other users can enter it, so any local user can reach the sockets below; set uid, or close it to other users")
	}
	return w
}
