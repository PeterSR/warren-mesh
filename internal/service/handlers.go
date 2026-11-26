package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
)

// ServeHTTP is one service socket's API.
func (s *sock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var h func(http.ResponseWriter, *http.Request)
	switch r.Method + " " + r.URL.Path {
	case "GET /v1/service/self":
		h = s.handleSelf
	case "GET /v1/service/status":
		h = s.handleStatus
	case "POST /v1/service/serve":
		h = s.handleServe
	case "GET /v1/service/calls":
		h = s.handleCalls
	case "POST /v1/service/answer":
		h = s.handleAnswer
	case "POST /v1/service/release":
		h = s.handleRelease
	case "POST /v1/service/call":
		h = s.handleCall
	case "PUT /v1/service/entries":
		h = s.handlePut
	case "GET /v1/service/view":
		h = s.handleView
	default:
		apierr.NotFound(w, r)
		return
	}
	if r.Header.Get("X-Warren") != "1" {
		apierr.Write(w, apierr.BadRequest, "every request needs the header X-Warren: 1")
		return
	}
	// Bodies need a Content-Length; chunked ones are refused.
	if (r.Method == http.MethodPost || r.Method == http.MethodPut) && r.ContentLength < 0 {
		apierr.Write(w, apierr.BadRequest, "a Content-Length is required; chunked bodies are refused")
		return
	}
	h(w, r)
}

func writeJSON(w http.ResponseWriter, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		apierr.Write(w, apierr.Unknown, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

// ---- parameters ----

func intParam(q url.Values, name string, def, min, max int, required bool) (int, *apierr.Error) {
	v := q.Get(name)
	if v == "" {
		if required {
			return 0, apierr.New(apierr.BadRequest, name+" is required")
		}
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min || n > max {
		return 0, apierr.New(apierr.BadRequest, fmt.Sprintf("%s must be a whole number from %d to %d", name, min, max))
	}
	return n, nil
}

// waitParam is the wait in whole seconds, 0 to 25, default 25.
func waitParam(q url.Values) (time.Duration, *apierr.Error) {
	n, err := intParam(q, "wait", maxWait, 0, maxWait, false)
	return time.Duration(n) * WaitUnit, err
}

// readBody reads a request body of at most max bytes under a read deadline.
func readBody(w http.ResponseWriter, r *http.Request, max int) ([]byte, *apierr.Error) {
	if r.ContentLength > int64(max) {
		return nil, apierr.New(apierr.TooLarge, fmt.Sprintf("the body is over %d bytes", max))
	}
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(BodyReadTimeout))
	b, err := io.ReadAll(io.LimitReader(r.Body, int64(max)+1))
	if err != nil {
		// The deadline stays set until the answer is written: net/http tries to
		// read what is left of an unread body before it answers, and would wait
		// for ever on a client that stalled.
		return nil, apierr.New(apierr.BadRequest, "cannot read the body: "+err.Error())
	}
	_ = rc.SetReadDeadline(time.Time{})
	if len(b) > max {
		return nil, apierr.New(apierr.TooLarge, fmt.Sprintf("the body is over %d bytes", max))
	}
	return b, nil
}

// ---- self and status ----

func (s *sock) handleSelf(w http.ResponseWriter, r *http.Request) {
	n := s.n
	n.mu.Lock()
	defer n.mu.Unlock()
	nss := s.namespaces()
	if nss == nil {
		nss = []string{}
	}
	writeJSON(w, map[string]any{
		"relay": s.relay, "node": n.name, "service": s.service, "flat": s.flat(),
		"entry_namespace": s.entryNS(), "namespaces": nss,
		"boot": n.boot, "max_call_body": MaxCallBody,
	})
}

func (s *sock) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.n.Status())
}

// Status is /v1/service/status.
func (n *Node) Status() map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()
	peers := n.peerStatus()
	services := []map[string]any{}
	for _, s := range n.sortedSocks() {
		services = append(services, map[string]any{"relay": s.relay, "name": s.service, "lease": s.lease != nil})
	}
	features := Features
	return map[string]any{
		"node": n.name, "boot": n.boot,
		"warren":    map[string]any{"version": Version, "peer_protocol": n.protocols, "features": features},
		"transport": map[string]any{"name": n.transport, "version": n.transportVersion},
		"peers":     peers,
		"link_up":   n.linkUp,
		"children":  n.childList(),
		"services":  services,
	}
}

// ---- leases ----

func (s *sock) handleServe(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	version, e := intParam(q, "version", 0, 1, 1<<30, true)
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	take := q.Get("take") == "1"
	n := s.n
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.draining {
		apierr.Write(w, apierr.NotReady, "Warren is shutting down")
		return
	}
	if s.lease != nil && !take {
		apierr.Write(w, apierr.AlreadyServed, "another lease serves this socket; wait, or take over with take=1")
		return
	}
	if time.Since(s.lastServe) < ServeInterval {
		apierr.Write(w, apierr.Busy, "one serve per second per socket")
		return
	}
	s.lastServe = time.Now()
	n.endLease(s, "taken over")
	l := n.newLease(s, version)
	writeJSON(w, map[string]any{"lease": l.token, "inflight": s.cfg.Inflight})
}

func (s *sock) handleRelease(w http.ResponseWriter, r *http.Request) {
	n := s.n
	n.mu.Lock()
	defer n.mu.Unlock()
	token := r.URL.Query().Get("lease")
	l := s.leaseFor(token)
	if l == nil {
		apierr.WriteErr(w, s.leaseErr(token))
		return
	}
	n.endLease(s, "released")
	w.WriteHeader(http.StatusNoContent)
}

// handleCalls is the long poll that hands a call to the service.
func (s *sock) handleCalls(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	wait, e := waitParam(q)
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	n := s.n
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		apierr.Write(w, apierr.NotReady, "Warren is shutting down")
		return
	}
	l := s.leaseFor(q.Get("lease"))
	if l == nil {
		e := s.leaseErr(q.Get("lease"))
		n.mu.Unlock()
		apierr.WriteErr(w, e)
		return
	}
	if l.polls >= s.cfg.Inflight {
		n.mu.Unlock()
		apierr.Write(w, apierr.Busy, "this lease has as many polls open as its inflight")
		return
	}
	if len(l.held) > 0 {
		c := l.held[0]
		l.held = l.held[1:]
		n.handOut(l, c)
		n.mu.Unlock()
		writeCall(w, c)
		return
	}
	if wait == 0 {
		n.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	wt := &waiter{ch: make(chan *call, 1)}
	l.waiters = append(l.waiters, wt)
	l.polls++
	n.touch(l)
	n.mu.Unlock()

	timer := time.NewTimer(wait)
	defer timer.Stop()
	var c *call
	end := ""
	select {
	case c = <-wt.ch:
		if c == nil {
			end = apierr.NotReady
		}
	case <-l.endCh:
		end = apierr.Gone
	case <-timer.C:
	case <-r.Context().Done():
		end = "client"
	case <-n.stopCh:
		end = apierr.NotReady
	}

	n.mu.Lock()
	l.polls--
	for i, x := range l.waiters {
		if x == wt {
			l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
			break
		}
	}
	if c == nil {
		// A call may have been given to this poll just as it ended.
		select {
		case c = <-wt.ch:
		default:
		}
	}
	if c != nil && end == "client" {
		// The poll's client left: put the call back for the next poll.
		n.requeue(l, c)
		c = nil
	}
	n.touch(l)
	n.mu.Unlock()

	switch {
	case c != nil:
		writeCall(w, c)
	case end == apierr.Gone:
		apierr.Write(w, apierr.Gone, "the lease ended")
	case end == apierr.NotReady:
		apierr.Write(w, apierr.NotReady, "Warren is shutting down")
	case end == "client":
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// requeue returns a handed-out call that never reached its service to the
// front of the line. mu held.
func (n *Node) requeue(l *lease, c *call) {
	if c.state != callHanded || l.ended {
		return
	}
	delete(l.handed, c.id)
	if len(l.waiters) > 0 {
		wt := l.waiters[0]
		l.waiters = l.waiters[1:]
		n.handOut(l, c)
		wt.ch <- c
		return
	}
	c.state = callHeld
	l.held = append([]*call{c}, l.held...)
	n.touch(l)
}

func writeCall(w http.ResponseWriter, c *call) {
	n := c.l.s.n
	n.mu.Lock()
	left := time.Until(c.deadline).Milliseconds()
	id, origin, from, to, method, body := c.id, c.origin, c.from, c.to, c.method, c.body
	n.mu.Unlock()
	if left < 1 {
		left = 1
	}
	h := w.Header()
	h.Set("X-Warren-Call", id)
	h.Set("X-Warren-Origin", origin)
	if from != "" {
		h.Set("X-Warren-From", from)
	}
	h.Set("X-Warren-To", to)
	h.Set("X-Warren-Method", method)
	h.Set("X-Warren-Deadline-Ms", strconv.FormatInt(left, 10))
	h.Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(body)
}

func (s *sock) handleAnswer(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	n := s.n
	// Lease and id are checked before the body is read.
	n.mu.Lock()
	l := s.leaseFor(q.Get("lease"))
	var c *call
	if l != nil {
		c = l.handed[q.Get("id")]
	}
	if l == nil {
		le := s.leaseErr(q.Get("lease"))
		n.mu.Unlock()
		apierr.WriteErr(w, le)
		return
	}
	if c == nil {
		n.mu.Unlock()
		apierr.Write(w, apierr.Gone, "no such call (ended, answered or past its deadline)")
		return
	}
	if fail := q.Get("fail"); fail != "" {
		// The service gives the call up: it ends as unknown for the caller at
		// once, and the place is free. No body.
		defer n.mu.Unlock()
		if fail != "1" || r.ContentLength != 0 || len(r.TransferEncoding) > 0 {
			apierr.Write(w, apierr.BadRequest, "fail is 1, and the request has no body")
			return
		}
		n.finish(c, result{err: apierr.New(apierr.Unknown, "the service gave the call up")})
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if c.answering {
		n.mu.Unlock()
		apierr.Write(w, apierr.Busy, "another answer for this call is being read")
		return
	}
	c.answering = true
	n.mu.Unlock()
	body, e := readBody(w, r, MaxCallBody)
	n.mu.Lock()
	defer n.mu.Unlock()
	c.answering = false
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	if c.state != callHanded {
		apierr.Write(w, apierr.Gone, "the call has ended")
		return
	}
	n.finish(c, result{body: body})
	w.WriteHeader(http.StatusNoContent)
}

// ---- entries and view ----

func (s *sock) handlePut(w http.ResponseWriter, r *http.Request) {
	n := s.n
	token := r.URL.Query().Get("lease")
	// The lease and the rate are checked before the body is read, and only
	// one body is read at a time.
	n.mu.Lock()
	l := s.leaseFor(token)
	switch {
	case l == nil:
		e := s.leaseErr(token)
		n.mu.Unlock()
		apierr.WriteErr(w, e)
		return
	case time.Since(s.lastPut) < PutInterval:
		n.mu.Unlock()
		apierr.Write(w, apierr.Busy, "one PUT per second per socket")
		return
	case s.putBusy:
		n.mu.Unlock()
		apierr.Write(w, apierr.Busy, "another PUT body is being read")
		return
	}
	s.lastPut = time.Now()
	s.putBusy = true
	flat := s.flat()
	n.mu.Unlock()
	defer func() { n.mu.Lock(); s.putBusy = false; n.mu.Unlock() }()

	if r.ContentLength > MaxPutBody {
		apierr.Write(w, apierr.TooLarge, "the body is over 4 MiB")
		return
	}
	// A flat child waits for the node's own services to publish first.
	if flat && !n.waitReady(r, s.service) {
		apierr.Write(w, apierr.NotReady, "waiting for the node's services to publish first")
		return
	}
	body, e := readBody(w, r, MaxPutBody)
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	n.mu.Lock()
	entryNS := s.entryNS()
	n.mu.Unlock()
	req, e := parsePut(body, entryNS)
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	n.mu.Lock()
	refused, e := n.applyPut(s, l, req)
	n.mu.Unlock()
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	writeJSON(w, map[string]any{"refused": refused})
}

// waitReady blocks until the service's node publishers have published, or
// BootHold has passed since boot. It returns false if the request ends.
func (n *Node) waitReady(r *http.Request, service string) bool {
	for {
		n.mu.Lock()
		svc := n.svc(service)
		if svc.ready || n.closed {
			n.mu.Unlock()
			return true
		}
		ch := svc.notify
		n.mu.Unlock()
		select {
		case <-ch:
		case <-r.Context().Done():
			return false
		case <-n.stopCh:
			return false
		}
	}
}

func (s *sock) handleView(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	wait, e := waitParam(q)
	if e != nil {
		apierr.WriteErr(w, e)
		return
	}
	since := q.Get("since")
	n := s.n
	n.mu.Lock()
	if n.draining {
		n.mu.Unlock()
		apierr.Write(w, apierr.NotReady, "Warren is shutting down")
		return
	}
	if s.viewPolls >= MaxViewPolls {
		n.mu.Unlock()
		apierr.Write(w, apierr.Busy, "at most 4 view polls per socket")
		return
	}
	s.viewPolls++
	n.mu.Unlock()
	defer func() { n.mu.Lock(); s.viewPolls--; n.mu.Unlock() }()

	var timer <-chan time.Time
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		timer = t.C
	}
	for {
		n.mu.Lock()
		svc := n.svc(s.service)
		cur := n.versionString(svc)
		// since empty, or from another boot, or already behind: answer now.
		if wait == 0 || since != cur || n.draining {
			parts := n.viewParts(svc, s)
			draining := n.draining
			n.mu.Unlock()
			if draining {
				apierr.Write(w, apierr.NotReady, "Warren is shutting down")
				return
			}
			parts.write(w)
			return
		}
		ch := svc.notify
		n.mu.Unlock()
		select {
		case <-ch:
		case <-timer:
			n.mu.Lock()
			parts := n.viewParts(svc, s)
			n.mu.Unlock()
			parts.write(w)
			return
		case <-r.Context().Done():
			return
		case <-n.stopCh:
			apierr.Write(w, apierr.NotReady, "Warren is shutting down")
			return
		}
	}
}

// viewParts is a view: the pieces are references to the bytes shared by every
// poll of this version (the big parts are never changed in place, only
// replaced), so taking them under mu copies nothing.
type viewParts struct {
	prefix   string
	reserved []byte
	lost     string
	entries  []byte
}

// viewParts takes the pieces of the view. The big parts, entries and
// reserved, are serialized once per version and shared; only this socket's
// lost list and the header are fresh. mu held.
func (n *Node) viewParts(svc *svcState, s *sock) viewParts {
	lost := svc.lostJSON[s.key()]
	if lost == "" {
		lost = "[]"
	}
	ver, _ := json.Marshal(n.versionString(svc))
	boot, _ := json.Marshal(n.boot)
	return viewParts{
		prefix:   fmt.Sprintf(`{"version":%s,"boot":%s,"ready":%t,"reserved":`, ver, boot, svc.ready),
		reserved: svc.sharedReserved, lost: lost, entries: svc.sharedEntries,
	}
}

// write sends the view without assembling it into one buffer.
func (v viewParts) write(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, v.prefix)
	_, _ = w.Write(v.reserved)
	_, _ = io.WriteString(w, `,"lost":`+v.lost+`,"entries":`)
	_, _ = w.Write(v.entries)
	_, _ = io.WriteString(w, "}")
}
