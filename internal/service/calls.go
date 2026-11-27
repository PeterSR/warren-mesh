package service

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/grammar"
)

// handleCall is POST /v1/service/call. A target on this node is served
// here; one on another node is carried to that node's Warren, and only
// from here to there: never peer to peer.
func (s *sock) handleCall(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	n := s.n
	to, method, from := q.Get("to"), q.Get("method"), q.Get("from")
	if to == "" {
		apierr.Write(w, apierr.BadRequest, "to is required")
		return
	}
	if !grammar.Method(method) {
		apierr.Write(w, apierr.BadRequest, "method is required and must be a label of at most 63 bytes")
		return
	}
	version, e := intParam(q, "version", 0, 1, 1<<30, true)
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	timeoutMs, e := intParam(q, "timeout_ms", DefaultTimeoutMs, 1, MaxTimeoutMs, false)
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}

	// The outflight place is taken before the body is read.
	n.mu.Lock()
	if s.outflight >= s.cfg.Outflight {
		n.mu.Unlock()
		apierr.Write(w, apierr.Busy, "this socket has as many calls in flight as its outflight")
		return
	}
	s.outflight++
	n.mu.Unlock()
	defer func() { n.mu.Lock(); s.outflight--; n.mu.Unlock() }()

	n.mu.Lock()
	fullFrom, e := n.checkFrom(s, from) // again, with the same checks, once the body is read
	if e == nil {
		_, _, e = n.resolve(s.service, s.relay, to, version)
	}
	n.mu.Unlock()
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	body, e := readBody(w, r, MaxCallBody)
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}

	n.mu.Lock()
	// Everything the call depends on can change while the body is read (a
	// name reserved, a socket replaced, a target gone), so the checks are made
	// again, under the same lock that enqueues or forwards the call.
	var target *sock
	var remote string
	var c *call
	if n.socks[s.key()] != s {
		e = apierr.New(apierr.Gone, "this socket is no longer served")
	} else {
		fullFrom, e = n.checkFrom(s, from)
	}
	if e == nil {
		target, remote, e = n.resolve(s.service, s.relay, to, version)
	}
	if e == nil && n.draining {
		e = apierr.New(apierr.NotReady, "Warren is shutting down")
	}
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	if e == nil && remote == "" {
		c = &call{origin: s.relay, from: fullFrom, to: to, method: method, body: body,
			deadline: deadline, poolKey: poolKey(s.relay, true), done: make(chan result, 1)}
		e = n.enqueue(target.lease, c)
	}
	n.mu.Unlock()
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	if remote != "" {
		out, e := n.callPeer(r.Context(), remote, outCall{service: s.service, version: version, method: method,
			to: to, origin: s.relay, from: fullFrom, body: body, deadline: deadline})
		if e != nil {
			apierr.WriteErr(w, e)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		writeBy(w, answerWriteBy(deadline))
		_, _ = w.Write(out)
		return
	}
	n.awaitCall(w, r, c)
}

// answerWriteBy is when the write of the answer to a call must be over: the
// call's deadline, or now if that has passed, in either case plus
// CallWriteGrace: max(deadline, now) + CallWriteGrace.
func answerWriteBy(deadline time.Time) time.Time {
	if now := time.Now(); deadline.Before(now) {
		deadline = now
	}
	return deadline.Add(CallWriteGrace)
}

// awaitCall waits for the answer to a call that was accepted and writes it.
func (n *Node) awaitCall(w http.ResponseWriter, r *http.Request, c *call) {
	select {
	case res := <-c.done:
		if res.err != nil {
			apierr.WriteErr(w, res.err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		// The caller has until the call's deadline, and a short grace for an
		// answer accepted just before it, to take the answer; after that the
		// place it holds is free.
		writeBy(w, answerWriteBy(c.deadline))
		_, _ = w.Write(res.body)
	case <-r.Context().Done():
		// The caller left. A held call is dropped; one already handed out
		// runs to its deadline and its answer is discarded.
		n.mu.Lock()
		if c.state == callHeld {
			n.finish(c, result{err: apierr.New(apierr.Timeout, "the caller left")})
		}
		n.mu.Unlock()
	}
}

// outCall is a call to carry to another node.
type outCall struct {
	service, method, to, origin, from string
	version                           int
	body                              []byte
	deadline                          time.Time
}

// callPeer carries a call to node's Warren and returns the target's answer.
// Failures keep their meaning: a call that never left is offline, one that
// may have arrived is unknown, a deadline is timeout, and what the other
// Warren said keeps its code.
func (n *Node) callPeer(ctx context.Context, node string, oc outCall) ([]byte, *apierr.Error) {
	n.mu.Lock()
	ps := n.peerStates[node]
	proto, why := 0, ""
	switch {
	case ps == nil:
		why = "no such peer"
	case ps.compat != "":
		why = ps.compat
	case !ps.up || ps.proto == 0:
		why = "the peer is not up"
	default:
		proto = ps.proto
	}
	n.mu.Unlock()
	if proto == 0 {
		return nil, apierr.New(apierr.Offline, node+": "+why)
	}
	q := url.Values{"service": {oc.service}, "version": {strconv.Itoa(oc.version)}, "method": {oc.method},
		"to": {oc.to}, "origin": {oc.origin},
		"timeout_ms": {strconv.FormatInt(max(1, time.Until(oc.deadline).Milliseconds()), 10)}}
	if oc.from != "" {
		q.Set("from", oc.from)
	}
	body := oc.body
	if body == nil {
		body = []byte{}
	}
	resp, e := n.peerDoUntil(ctx, node, proto, "POST", "call?"+q.Encode(), body, oc.deadline)
	if e == errAnswerTooBig {
		// The call was sent and its answer could not come back: the outcome
		// is lost, not a body that was too large.
		return nil, apierr.New(apierr.Unknown, "the answer from "+node+" is over the size a transport carries")
	}
	if e != nil && e.Code == apierr.Unsupported {
		// The peer has no such endpoint (an older Warren), so nothing was
		// done with the call. "Unsupported" is for this socket's own API,
		// and must not be taken for a missing feature of the caller's call.
		return nil, apierr.New(apierr.Offline, node+" does not support calls: "+e.Detail)
	}
	if e != nil {
		return nil, e
	}
	if len(resp.Body) > MaxCallBody {
		return nil, apierr.New(apierr.Unknown, node+" answered with more than a call may carry")
	}
	return resp.Body, nil
}

// checkFrom validates the optional from and returns the full entry address
// the call is made on behalf of ("" for none): a name this socket may
// publish, not held by an earlier publisher by precedence and not reserved
// by the node for another socket. It need not be listed yet. mu held.
func (n *Node) checkFrom(s *sock, from string) (string, *apierr.Error) {
	if from == "" {
		return "", nil
	}
	name, ns := from, s.entryNS()
	if strings.Contains(from, "@") {
		var ok bool
		if name, ns, ok = grammar.SplitAddress(from); !ok {
			return "", apierr.New(apierr.BadRequest, "from must be a name or name@namespace")
		}
	} else if !grammar.EntryName(name) {
		return "", apierr.New(apierr.BadRequest, "from is not an entry name")
	}
	if !s.mayPublishIn(ns) {
		return "", apierr.New(apierr.Refused, "this socket may not publish in "+ns)
	}
	if !grammar.MatchNames(s.childNames(), name) {
		return "", apierr.New(apierr.Refused, "the child's names patterns do not allow "+name)
	}
	if s.flat() {
		// A flat child shares the node's namespace, so earlier publishers win.
		node := n.socks[sockKey{n.name, s.service}]
		if node != nil {
			for _, nm := range node.heldNames {
				if nm == name {
					return "", apierr.New(apierr.Refused, name+" is held by the node's own service")
				}
			}
			for _, nm := range node.heldReserved {
				if nm == name {
					return "", apierr.New(apierr.Refused, name+" is reserved by the node's own service")
				}
			}
		}
		now := time.Now()
		for _, o := range n.socksOf(s.service) {
			if o == s {
				break
			}
			if !o.flat() || !o.visible(now) {
				continue
			}
			for _, e := range o.entries {
				if e.Name == name && e.Namespace == n.name {
					return "", apierr.New(apierr.Refused, name+" is held by an earlier child")
				}
			}
		}
	}
	return grammar.JoinAddress(name, ns), nil
}

// resolve finds the socket a call goes to and runs the checks at the
// target in their order: service exists, allow, live lease, version, then
// the entry. The origin is the relay id the call is made from, on this node
// or (for a call from a peer) on the peer. A target on another node has no
// socket here: remote is that node, for the caller to carry the call to
// (the other node makes the checks). mu held.
func (n *Node) resolve(service, origin, to string, version int) (target *sock, remote string, e *apierr.Error) {
	var owner *sock
	name := ""
	if strings.Contains(to, "@") {
		var ns string
		var ok bool
		if name, ns, ok = grammar.SplitAddress(to); !ok {
			return nil, "", apierr.New(apierr.BadRequest, "to must be a relay id or name@namespace")
		}
		if root := grammar.Root(ns); root != n.name {
			r, e := n.foreign(root)
			return nil, r, e
		}
		if t := n.addrOwner(service, to); t != nil {
			owner = t
		} else {
			owner = n.namespaceOwner(ns, service)
			if owner == nil {
				return nil, "", apierr.New(apierr.NoTarget, "no such entry")
			}
		}
	} else {
		if !grammar.RelayID(to) {
			return nil, "", apierr.New(apierr.BadRequest, "to must be a relay id or name@namespace")
		}
		if root := grammar.Root(to); root != n.name {
			r, e := n.foreign(root)
			return nil, r, e
		}
		if to == n.name {
			owner = n.socks[sockKey{to, service}]
		} else {
			if n.children[grammar.ChildLabel(to)] == nil {
				return nil, "", apierr.New(apierr.NoTarget, "no such relay")
			}
			owner = n.socks[sockKey{to, service}]
		}
		if owner == nil {
			return nil, "", apierr.New(apierr.Refused, "the target has no such service")
		}
	}
	switch {
	case !grammar.Allowed(owner.cfg.Allow, origin):
		return nil, "", apierr.New(apierr.Refused, "the target's allow does not admit this caller")
	case owner.lease == nil:
		return nil, "", apierr.New(apierr.NoService, "the target has the service but no live lease")
	case owner.lease.version != version:
		return nil, "", apierr.New(apierr.Version, "the target serves another major version")
	}
	if name != "" && n.addrOwner(service, to) == nil {
		return nil, "", apierr.New(apierr.NoTarget, "no such entry")
	}
	return owner, "", nil
}

// addrOwner is the socket that publishes the entry at address to, or nil.
// It never makes state for a service name: the names come from peers too.
// mu held.
func (n *Node) addrOwner(service, to string) *sock {
	if svc := n.svcs[service]; svc != nil {
		return svc.addr[to]
	}
	return nil
}

// namespaceOwner is the socket whose namespace ns is, for an entry that is
// not listed: the checks at the target still come before no-target. Nil
// when ns is nobody's here. mu held.
func (n *Node) namespaceOwner(ns, service string) *sock {
	if ns == n.name {
		return n.socks[sockKey{n.name, service}]
	}
	if label := grammar.ChildLabel(ns); label != "" {
		if ch := n.children[label]; ch != nil && !ch.flat {
			return n.socks[sockKey{ns, service}]
		}
		if n.cfg.NamespaceTaken(label) {
			return n.socks[sockKey{n.name, service}]
		}
	}
	return nil
}

// foreign is where a call to a target on another node goes: to that node if
// the transport lists it, offline if it was a peer a moment ago, no-target
// if it is not known at all. mu held.
func (n *Node) foreign(root string) (string, *apierr.Error) {
	for _, p := range n.peers {
		if p == root {
			return root, nil
		}
	}
	if ps := n.peerStates[root]; ps != nil {
		return "", apierr.New(apierr.Offline, "the node "+root+" is not reachable now")
	}
	return "", apierr.New(apierr.NoTarget, "no such node")
}
