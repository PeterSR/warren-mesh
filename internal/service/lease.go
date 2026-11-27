package service

import (
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/grammar"
)

// lease is the right to serve one socket. It is live from `serve` until
// release, takeover, a managed exit, removal, shutdown, or LeaseGrace after
// it last had a poll open or a call handed out.
type lease struct {
	token   string
	version int
	s       *sock
	since   time.Time

	// reserved are places taken for calls from peers whose body is still
	// being read; they count against inflight and the per-root share.
	reserved int
	held     []*call          // accepted, waiting for a poll
	handed   map[string]*call // handed out, not yet answered
	waiters  []*waiter        // polls waiting for a call
	polls    int
	perRoot  map[string]int // held and handed calls per pool key

	grace    *time.Timer
	graceGen int
	endCh    chan struct{}
	ended    bool
}

type waiter struct{ ch chan *call }

type callState int

const (
	callHeld callState = iota
	callHanded
	callDone
)

type result struct {
	body []byte
	err  *apierr.Error
}

// call is one request waiting for, or being handled by, a service.
type call struct {
	id       string
	l        *lease
	state    callState
	body     []byte
	origin   string
	from     string
	to       string
	method   string
	deadline time.Time
	poolKey  string
	done     chan result
	timer    *time.Timer
	// answering is set while an answer body is being read for this call.
	answering bool
	// holding is set when the call ended while its answer was still being
	// read: its per-root place and a socket's reader place stay taken until
	// the reader is done with the buffer (releaseAnswer).
	holding bool
}

// newLease starts a lease on s. mu held.
func (n *Node) newLease(s *sock, version int) *lease {
	if s.relay == n.name { // only the node's own services are reported; a child's would only grow the map
		n.epochs[s.key()]++
	}
	l := &lease{token: grammar.NewID(), version: version, s: s, since: time.Now(), handed: map[string]*call{},
		perRoot: map[string]int{}, endCh: make(chan struct{})}
	s.lease = l
	n.touch(l)
	return l
}

// busy is whether the lease is being used right now.
func (l *lease) busy() bool { return l.polls > 0 || len(l.handed) > 0 }

// touch starts or stops the grace timer after anything that may change
// whether the lease is busy. mu held.
func (n *Node) touch(l *lease) {
	if l.ended {
		return
	}
	if l.busy() {
		if l.grace != nil {
			l.grace.Stop()
			l.grace = nil
		}
		l.graceGen++
		return
	}
	if l.grace != nil {
		return
	}
	l.graceGen++
	gen := l.graceGen
	l.grace = time.AfterFunc(LeaseGrace, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		// A timer of an earlier idle period must not end a lease that has
		// been busy since.
		if !l.ended && l.graceGen == gen && !l.busy() {
			n.endLease(l.s, "grace over")
		}
	})
}

// endLease ends the socket's lease. Calls handed out fail with unknown,
// calls only held fail with no-service, open polls answer gone, and the
// socket's entries become stale. mu held.
func (n *Node) endLease(s *sock, reason string) {
	l := s.lease
	if l == nil || l.ended {
		return
	}
	n.log.Debug("lease ended", "relay", s.relay, "service", s.service, "reason", reason)
	l.ended = true
	if l.grace != nil {
		l.grace.Stop()
	}
	close(l.endCh)
	for _, c := range handedList(l) {
		n.finish(c, result{err: apierr.New(apierr.Unknown, "the service's lease ended after the call was handed out")})
	}
	for _, c := range append([]*call(nil), l.held...) {
		n.finish(c, result{err: apierr.New(apierr.NoService, "the lease ended before the call was handed out")})
	}
	s.lease = nil
	if s.entriesFrom == l {
		s.endedAt = time.Now() // the published entries go stale from now
	}
	svc := n.svc(s.service)
	n.rebuild(svc)
	// Entries are dropped for good StaleFor later. One timer per socket is
	// reset by the end of the lease whose entries are published; leases that
	// never published (a serve loop) must not keep old entries alive.
	if s.entriesFrom == l {
		if s.staleTimer == nil {
			s.staleTimer = time.AfterFunc(StaleFor+10*time.Millisecond, func() {
				n.mu.Lock()
				defer n.mu.Unlock()
				if !n.closed {
					n.rebuild(svc)
				}
			})
		} else {
			s.staleTimer.Reset(StaleFor + 10*time.Millisecond)
		}
	}
}

// leaseErr is the error for a token that has no live lease on s: gone for
// any token that is not live (ended, from before a restart, pushed out by a
// takeover, or on a replaced socket), bad-request for none at all. mu held.
func (s *sock) leaseErr(token string) *apierr.Error {
	if token == "" {
		return apierr.New(apierr.BadRequest, "lease is required")
	}
	return apierr.New(apierr.Gone, "no such lease")
}

// leaseFor finds the live lease with this token on s. mu held.
func (s *sock) leaseFor(token string) *lease {
	if l := s.lease; l != nil && !l.ended && l.token == token {
		return l
	}
	return nil
}

// ---- calls ----

// enqueue puts a call on the target's lease: straight to a waiting poll, or
// held. It answers busy when a pool is full. mu held.
func (n *Node) enqueue(l *lease, c *call) *apierr.Error {
	if e := n.checkRoom(l, c.poolKey); e != nil {
		return e
	}
	n.admit(l, c)
	return nil
}

// checkRoom answers busy when the lease has no place for one more call of
// this pool key. mu held.
func (n *Node) checkRoom(l *lease, key string) *apierr.Error {
	s := l.s
	if len(l.held)+len(l.handed)+l.reserved+s.readers >= s.cfg.Inflight {
		return apierr.New(apierr.Busy, "the service's inflight places are full")
	}
	if l.perRoot[key] >= s.cfg.PerRoot {
		return apierr.New(apierr.Busy, "too many calls from this origin are waiting")
	}
	return nil
}

// reserve takes a place for a call whose body is still to be read (calls
// from peers: the per-root place is taken before the body). mu held.
func (n *Node) reserve(l *lease, key string) *apierr.Error {
	if e := n.checkRoom(l, key); e != nil {
		return e
	}
	l.reserved++
	l.perRoot[key]++
	return nil
}

// unreserve gives a reserved place back. A lease that has ended took its
// places with it. mu held.
func (n *Node) unreserve(l *lease, key string) {
	if l == nil || l.ended {
		return
	}
	l.reserved--
	if l.perRoot[key]--; l.perRoot[key] <= 0 {
		delete(l.perRoot, key)
	}
}

// enqueueReserved puts a call on the lease using the place reserve took. mu held.
func (n *Node) enqueueReserved(l *lease, c *call) {
	l.reserved--
	l.perRoot[c.poolKey]-- // admit counts it again
	n.admit(l, c)
}

// admit puts a call on the lease: the caller has checked there is room. mu held.
func (n *Node) admit(l *lease, c *call) {
	c.l = l
	l.perRoot[c.poolKey]++
	c.timer = time.AfterFunc(time.Until(c.deadline), func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.finish(c, result{err: apierr.New(apierr.Timeout, "no answer in time")})
	})
	if len(l.waiters) > 0 {
		w := l.waiters[0]
		l.waiters = l.waiters[1:]
		n.handOut(l, c)
		w.ch <- c
		return
	}
	c.state = callHeld
	l.held = append(l.held, c)
}

// handOut marks c as handed to a poll. mu held.
func (n *Node) handOut(l *lease, c *call) {
	c.id = grammar.NewID()
	c.state = callHanded
	l.handed[c.id] = c
	n.touch(l)
}

// finish ends a call with a result, once. mu held.
func (n *Node) finish(c *call, r result) {
	if c.state == callDone {
		return
	}
	l := c.l
	switch c.state {
	case callHeld:
		for i, x := range l.held {
			if x == c {
				l.held = append(l.held[:i], l.held[i+1:]...)
				break
			}
		}
	case callHanded:
		delete(l.handed, c.id)
	}
	c.state = callDone
	if c.timer != nil {
		c.timer.Stop()
	}
	if c.answering {
		// Its answer is still being read into a buffer: the places are kept
		// until the reader is done, so the memory bound holds.
		c.holding = true
		l.s.readers++
	} else {
		n.dropPerRoot(l, c.poolKey)
	}
	c.done <- r
	n.touch(l)
}

// dropPerRoot gives back a per-root place. mu held.
func (n *Node) dropPerRoot(l *lease, key string) {
	if l.perRoot[key]--; l.perRoot[key] <= 0 {
		delete(l.perRoot, key)
	}
}

// releaseAnswer gives back the places of a call that ended while its answer
// was being read, now that the reader is done. mu held.
func (n *Node) releaseAnswer(c *call) {
	if !c.holding {
		return
	}
	c.holding = false
	c.l.s.readers--
	n.dropPerRoot(c.l, c.poolKey)
}

// wakeWaiters tells waiting polls to look again (used when draining). mu held.
func (n *Node) wakeWaiters(s *sock) {
	if s.lease == nil {
		return
	}
	for _, w := range s.lease.waiters {
		close(w.ch)
	}
	s.lease.waiters = nil
}

// poolKey is who a call is counted against in the target's per-root pool
// (docs/architecture.md, "Limits and fairness"): a call from a socket on
// this node by its relay id, a call from a peer by the node it came from, so
// one busy node (children and all) has one share. The two never collide: a local relay id ends in this
// node's name, and a node never calls itself.
func poolKey(origin string, local bool) string {
	if local {
		return origin
	}
	return grammar.Root(origin)
}

func handedList(l *lease) []*call {
	out := make([]*call, 0, len(l.handed))
	for _, c := range l.handed {
		out = append(out, c)
	}
	return out
}
