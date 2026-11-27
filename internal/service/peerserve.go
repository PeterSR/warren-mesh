package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/grammar"
	"github.com/PeterSR/warren-mesh/internal/link"
)

// The server side of the peer protocol (docs/architecture.md, "Peer
// protocol"): what other nodes ask of this one, arriving on link B with the
// node the transport vouches for.

// peerHandler is the handler on link B. Whatever the answer, it carries
// this node's name, version and features, and the protocol of the path, so
// the asker can check who answered and what.
func (n *Node) peerHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set(link.HeaderNode, n.name)
		h.Set(VersionHeader, Version)
		h.Set(FeaturesHeader, strings.Join(Features, ","))
		proto, ok := pathProtocol(r.URL.Path)
		hp := n.peerHandlers[proto]
		if !ok || hp == nil {
			// An unknown protocol version is an unknown path.
			h.Set(PeerProtocolHeader, strconv.Itoa(n.protocols[len(n.protocols)-1]))
			apierr.NotFound(w, r)
			return
		}
		h.Set(PeerProtocolHeader, strconv.Itoa(proto))
		if got := r.Header.Get(PeerProtocolHeader); got != strconv.Itoa(proto) {
			apierr.Write(w, apierr.BadRequest, fmt.Sprintf("%s must be %d on a /v%d/peer request, not %q (this node speaks peer protocols %v)",
				PeerProtocolHeader, proto, proto, grammar.CleanText(got, 16), n.protocols))
			return
		}
		// Until a hello has passed nobody is a peer yet: the transport is not
		// known to be the one this node is configured for.
		n.mu.Lock()
		passed := n.helloPassed
		n.mu.Unlock()
		if !passed {
			apierr.Write(w, apierr.NotReady, "Warren has not accepted the transport yet")
			return
		}
		n.notePeer(link.From(r.Context()), r.Header)
		hp.ServeHTTP(w, r)
	})
}

// pathProtocol is N of a /v<N>/peer/ path.
func pathProtocol(p string) (int, bool) {
	if !link.IsPeerPath(p) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(p, "/v")[:strings.Index(p[2:], "/")])
	return n, err == nil
}

// notePeer remembers what a peer announced on a request, for a peer this
// node knows.
func (n *Node) notePeer(node string, h http.Header) {
	n.mu.Lock()
	defer n.mu.Unlock()
	ps := n.peerStates[node]
	if ps == nil || !ps.up {
		return
	}
	if v := h.Get(VersionHeader); v != "" {
		ps.version = cleanVersion(v)
	}
	if _, ok := h["X-Warren-Features"]; ok {
		ps.features = cleanFeatures(strings.Split(h.Get(FeaturesHeader), ","))
	}
}

// servePeerV1 is protocol 1.
func (n *Node) servePeerV1(w http.ResponseWriter, r *http.Request) {
	from := link.From(r.Context())
	rest := strings.TrimPrefix(r.URL.Path, "/v1/peer/")
	n.mu.Lock()
	draining := n.draining
	n.mu.Unlock()
	if draining {
		apierr.Write(w, apierr.NotReady, "Warren is shutting down")
		return
	}
	switch r.Method + " " + rest {
	case "GET hello":
		writeJSON(w, wireHello{Node: n.name, Version: Version, PeerProtocol: n.protocols, Features: nonNilStrs(Features)})
	case "GET services":
		n.peerServices(w, r, from)
	case "GET entries":
		n.peerEntries(w, r, from)
	case "POST call":
		n.peerCall(w, r, from)
	default:
		apierr.NotFound(w, r)
	}
}

// takePollSlot takes one of the MaxListPolls places of a peer's long polls
// of one list.
func (n *Node) takePollSlot(key string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.pollSlots[key] >= MaxListPolls {
		return false
	}
	n.pollSlots[key]++
	return true
}

func (n *Node) dropPollSlot(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.pollSlots[key]--; n.pollSlots[key] <= 0 {
		delete(n.pollSlots, key)
	}
}

// ---- the list of services ----

func (n *Node) servicesVersion() string { return fmt.Sprintf("%s.s%d", n.boot[:8], n.svcsVer) }

// scheduleServices publishes a changed set of services to peers after the
// debounce. mu held.
func (n *Node) scheduleServices() {
	if n.closed {
		return
	}
	cur := n.ourServices()
	// This node's own polls follow every change of its services at once,
	// whatever the debounce of what peers are told is doing.
	if !slices.Equal(cur, n.svcsSeen) {
		n.svcsSeen = cur
		n.reconcileAllPeers()
	}
	if n.svcsTimer != nil || slices.Equal(cur, n.svcNames) {
		return
	}
	n.svcsTimer = time.AfterFunc(PeerDebounce, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.svcsTimer = nil
		if !n.closed {
			n.svcsSeen = n.ourServices()
			n.reconcileAllPeers()
			n.publishServices()
		}
	})
}

// publishServices makes the current set of services the one peers are sent.
// mu held.
func (n *Node) publishServices() {
	names := n.ourServices()
	if n.svcsVer > 0 && slices.Equal(names, n.svcNames) {
		return
	}
	n.svcNames = names
	n.svcsVer++
	close(n.svcsNotify)
	n.svcsNotify = make(chan struct{})
}

func (n *Node) peerServices(w http.ResponseWriter, r *http.Request, from string) {
	wait, e := waitParam(r.URL.Query())
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	since := r.URL.Query().Get("since")
	key := from + "|services"
	if !n.takePollSlot(key) {
		apierr.Write(w, apierr.Busy, fmt.Sprintf("at most %d long polls of the list of services per peer", MaxListPolls))
		return
	}
	defer n.dropPollSlot(key)
	var timer <-chan time.Time
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		timer = t.C
	}
	timedOut := false
	for {
		n.mu.Lock()
		ver, ch, names, draining := n.servicesVersion(), n.svcsNotify, n.svcNames, n.draining
		n.mu.Unlock()
		if draining {
			apierr.Write(w, apierr.NotReady, "Warren is shutting down")
			return
		}
		if wait == 0 || since != ver || timedOut {
			writeJSON(w, map[string]any{"node": n.name, "version": ver, "services": nonNilStrs(names)})
			return
		}
		select {
		case <-ch:
		case <-timer:
			timedOut = true
		case <-r.Context().Done():
			return
		case <-n.stopCh:
			apierr.Write(w, apierr.NotReady, "Warren is shutting down")
			return
		}
	}
}

// ---- the list of entries ----

func (n *Node) peerVersion(svc *svcState) string {
	return fmt.Sprintf("%s.p%d", n.boot[:8], svc.peerVer)
}

// schedulePeers hands a changed local list to peers after the debounce: the
// bytes of the list then are the ones every poll of that version is served.
// mu held.
func (n *Node) schedulePeers(svc *svcState) {
	if n.closed || svc.peerTimer != nil {
		return
	}
	svc.peerTimer = time.AfterFunc(PeerDebounce, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		svc.peerTimer = nil
		if n.closed {
			return
		}
		svc.peerEntries, svc.peerReserved = svc.localEntries, svc.localReserved
		svc.peerVer++
		close(svc.peerNotify)
		svc.peerNotify = make(chan struct{})
	})
}

func (n *Node) peerEntries(w http.ResponseWriter, r *http.Request, from string) {
	q := r.URL.Query()
	wait, e := waitParam(q)
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	service, since := q.Get("service"), q.Get("since")
	if !grammar.Service(service) {
		apierr.Write(w, apierr.BadRequest, "service is required")
		return
	}
	n.mu.Lock()
	has := slices.Contains(n.ourServices(), service)
	n.mu.Unlock()
	if !has {
		apierr.Write(w, apierr.Refused, "no such service")
		return
	}
	key := from + "|entries|" + service
	if !n.takePollSlot(key) {
		apierr.Write(w, apierr.Busy, fmt.Sprintf("at most %d long polls of one list of entries per peer", MaxListPolls))
		return
	}
	defer n.dropPollSlot(key)
	var timer <-chan time.Time
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		timer = t.C
	}
	timedOut := false
	for {
		n.mu.Lock()
		svc := n.svc(service)
		ver, ch, entries, reserved, draining := n.peerVersion(svc), svc.peerNotify, svc.peerEntries, svc.peerReserved, n.draining
		n.mu.Unlock()
		if draining {
			apierr.Write(w, apierr.NotReady, "Warren is shutting down")
			return
		}
		if wait == 0 || since != ver || timedOut {
			// The cached bytes of this version are written as they are.
			vb, _ := json.Marshal(ver)
			sb, _ := json.Marshal(service)
			nb, _ := json.Marshal(n.name)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"node":%s,"version":%s,"service":%s,"reserved":`, nb, vb, sb)
			_, _ = w.Write(reserved)
			_, _ = w.Write([]byte(`,"entries":`))
			_, _ = w.Write(entries)
			_, _ = w.Write([]byte("}"))
			return
		}
		select {
		case <-ch:
		case <-timer:
			timedOut = true
		case <-r.Context().Done():
			return
		case <-n.stopCh:
			apierr.Write(w, apierr.NotReady, "Warren is shutting down")
			return
		}
	}
}

// ---- calls from a peer ----

// peerCall is POST /v1/peer/call: a call from a socket on peer node `from`
// (or a child of it) to a relay or entry on this node. The origin and the
// from are vouched for, the place in the target's pool is taken (per origin
// root) before the body is read, and the call then goes the way a local
// one does. A node never passes a call on to another peer.
func (n *Node) peerCall(w http.ResponseWriter, r *http.Request, from string) {
	q := r.URL.Query()
	start := time.Now()
	service, method, to, origin, fromEntry := q.Get("service"), q.Get("method"), q.Get("to"), q.Get("origin"), q.Get("from")
	if !grammar.Service(service) || !grammar.Method(method) || to == "" {
		apierr.Write(w, apierr.BadRequest, "service, method and to are required")
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
	if e := vouchCall(from, origin, fromEntry); e != nil {
		apierr.WriteErr(w, e)
		return
	}
	var root string
	if strings.Contains(to, "@") {
		_, ns, ok := grammar.SplitAddress(to)
		if !ok {
			apierr.Write(w, apierr.BadRequest, "to must be a relay id or name@namespace")
			return
		}
		root = grammar.Root(ns)
	} else if !grammar.RelayID(to) {
		apierr.Write(w, apierr.BadRequest, "to must be a relay id or name@namespace")
		return
	} else {
		root = grammar.Root(to)
	}
	if root != n.name {
		apierr.Write(w, apierr.NoTarget, "this node does not pass calls on to other nodes")
		return
	}
	key := poolKey(origin, false)
	// A service this node does not have is refused before anything else:
	// what a peer names must never make state here.
	n.mu.Lock()
	has := slices.Contains(n.ourServices(), service)
	n.mu.Unlock()
	if !has {
		apierr.Write(w, apierr.Refused, "the target has no such service")
		return
	}

	// The place is taken before the body is read.
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		apierr.Write(w, apierr.NotReady, "Warren is shutting down")
		return
	}
	target, _, e := n.resolve(service, origin, to, version)
	var reserved *lease
	if e == nil {
		reserved = target.lease
		e = n.reserve(reserved, key)
	}
	n.mu.Unlock()
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	body, e := readBody(w, r, MaxCallBody)
	n.mu.Lock()
	if e != nil {
		n.unreserve(reserved, key)
		n.mu.Unlock()
		apierr.WriteErr(w, e)
		return
	}
	// Targets can change while the body is read, so resolve again.
	target, _, e = n.resolve(service, origin, to, version)
	if e == nil && n.draining {
		e = apierr.New(apierr.NotReady, "Warren is shutting down")
	}
	var c *call
	if e == nil {
		c = &call{origin: origin, from: fromEntry, to: to, method: method, body: body,
			deadline: start.Add(time.Duration(timeoutMs) * time.Millisecond), poolKey: key, done: make(chan result, 1)}
		if target.lease == reserved {
			n.enqueueReserved(reserved, c)
		} else {
			n.unreserve(reserved, key)
			e = n.enqueue(target.lease, c)
		}
	} else {
		n.unreserve(reserved, key)
	}
	n.mu.Unlock()
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	n.awaitCall(w, r, c)
}
