package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Lease is the right to serve a socket.
type Lease struct {
	Token string `json:"lease"`
	// Inflight is how many calls may be held for the lease or handed out and
	// not yet answered; it is also how many polls the lease may have open.
	Inflight int `json:"inflight"`
}

// Serve asks for the lease on the socket (POST /v1/service/serve) for a
// major version of the service. While another lease is live the answer is
// already-served, unless take is set, which takes over at once. At most one
// serve per second per socket is accepted (busy otherwise). Most services use
// a Server, which does all of this.
func (c *Client) Serve(ctx context.Context, version int, take bool) (*Lease, error) {
	q := url.Values{"version": {strconv.Itoa(version)}}
	if take {
		q.Set("take", "1")
	}
	r, err := c.do(ctx, http.MethodPost, "/v1/service/serve", q, nil, 0)
	if err != nil {
		return nil, err
	}
	var l Lease
	if err := json.Unmarshal(r.body, &l); err != nil || l.Token == "" {
		return nil, &Error{Code: CodeUnknown, Detail: "serve answered with something that is not a lease"}
	}
	return &l, nil
}

// Call is a call handed to a service.
type Call struct {
	// ID is unique per lease; it is what Answer takes.
	ID string
	// Origin is the relay id the call came from.
	Origin string
	// From is the entry address the call was made on behalf of, if any.
	From string
	// To is the full entry address, or the relay id, the call was sent to.
	To     string
	Method string
	// Deadline is when the caller gives up; an answer after it gets gone.
	Deadline time.Time
	Body     []byte
}

// Poll waits up to wait (whole seconds, at most 25) for a call
// (GET /v1/service/calls). It returns nil, nil when none came. A lease may
// have at most inflight polls open (busy otherwise); gone means the lease
// has ended.
func (c *Client) Poll(ctx context.Context, lease string, wait time.Duration) (*Call, error) {
	wait = min(max(wait, 0), MaxWait)
	secs := int(wait / time.Second)
	q := url.Values{"lease": {lease}, "wait": {strconv.Itoa(secs)}}
	r, err := c.do(ctx, http.MethodGet, "/v1/service/calls", q, nil, time.Duration(secs)*time.Second+viewRequestSlack)
	if err != nil {
		return nil, err
	}
	if r.status == http.StatusNoContent {
		return nil, nil
	}
	call := &Call{
		ID: r.header.Get("X-Warren-Call"), Origin: r.header.Get("X-Warren-Origin"),
		From: r.header.Get("X-Warren-From"), To: r.header.Get("X-Warren-To"),
		Method: r.header.Get("X-Warren-Method"), Body: r.body,
	}
	if ms, err := strconv.ParseInt(r.header.Get("X-Warren-Deadline-Ms"), 10, 64); err == nil {
		call.Deadline = time.Now().Add(time.Duration(ms) * time.Millisecond)
	} else {
		call.Deadline = time.Now().Add(DefaultCallTimeout)
	}
	return call, nil
}

// Answer gives the answer to a call (POST /v1/service/answer). The body is
// at most MaxCallBody bytes. A second answer for the same call, or one after
// its deadline, gets gone; an answer while another for the same call is being
// read gets busy.
func (c *Client) Answer(ctx context.Context, lease, id string, body []byte) error {
	_, err := c.do(ctx, http.MethodPost, "/v1/service/answer", url.Values{"lease": {lease}, "id": {id}}, body, 0)
	return err
}

// Fail gives a handed-out call up (POST /v1/service/answer with fail=1): the
// caller gets unknown at once, and the call's place is free. Use it when the
// service cannot answer; saying nothing leaves the caller waiting for its
// deadline and the place held until then. A Server does this when a handler
// returns an error or panics.
func (c *Client) Fail(ctx context.Context, lease, id string) error {
	_, err := c.do(ctx, http.MethodPost, "/v1/service/answer", url.Values{"lease": {lease}, "id": {id}, "fail": {"1"}}, nil, 0)
	return err
}

// Release gives the lease up (POST /v1/service/release).
func (c *Client) Release(ctx context.Context, lease string) error {
	_, err := c.do(ctx, http.MethodPost, "/v1/service/release", url.Values{"lease": {lease}}, nil, 0)
	return err
}

// Handler answers a call. The context ends at the call's deadline, or when
// the Server is stopped and its drain time has passed. The returned bytes
// are the answer: the caller receives only a body, so a service that wants
// to report a failure to the caller puts it in the body in its own format.
//
// A non-nil error means the call is given up: the Server logs it, tells
// Warren (Fail), and the caller gets unknown at once; the Server goes on with
// the next call. The same holds for a handler that panics. A handler whose
// context has ended by the time it returns (the stop cut it off) is not
// answered either: an answer then would be for a call the Server has already
// given up, and the lease is released, which fails the call as unknown.
type Handler func(ctx context.Context, call *Call) ([]byte, error)

// ServeOptions configure a Server.
type ServeOptions struct {
	// Version is the major version of the service this process serves.
	Version int
	// Take takes the lease over from a live one the first time the Server
	// serves, instead of waiting for it to end. Later serves (after a lost
	// lease) always wait: two processes with Take would fight forever.
	Take bool
	// Drain is how long, after the context ends, calls already handed out may
	// finish before the lease is released. Default 5 seconds.
	Drain time.Duration
	// OnLease, if set, is called after every successful serve, before the
	// polls start, on the goroutine that runs the session: it must not block
	// for anywhere near the lease grace (20 s), or the lease ends unpolled.
	// The entries are published after it returns. bootChanged is true when
	// Warren restarted since the previous lease.
	OnLease func(self *Self, lease *Lease, bootChanged bool)
	// OnPut, if set, is called with the outcome of every PUT of entries:
	// what was refused or is displaced, or the error.
	OnPut func(refused []Refusal, err error)
	// Log receives what the Server does about failures; nil logs nothing.
	Log *slog.Logger
}

// Server serves a socket: it holds the lease, runs polls in parallel,
// answers, publishes entries, and mends itself, until its context ends.
type Server struct {
	c *Client
	o ServeOptions
	h Handler

	mu      sync.Mutex
	entries *EntriesRequest // nil: nothing to publish yet
	gen     int             // bumped by SetEntries
	wake    chan struct{}   // poked by SetEntries
	boot    string
}

// NewServer makes a Server; Run starts it.
func (c *Client) NewServer(o ServeOptions, h Handler) *Server {
	if o.Drain == 0 {
		o.Drain = 5 * time.Second
	}
	if o.Log == nil {
		o.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Server{c: c, o: o, h: h, wake: make(chan struct{}, 1)}
}

// SetEntries sets the list to publish, now (if a lease is live) and again
// after every serve, since a new lease starts with no published list. It
// may be called before and during Run. Without any call, nothing is
// published. Publishing is paced to one PUT per second, newest list wins.
//
// A list whose entry data is not valid JSON is refused here with a
// bad-request *Error and not stored: it could never be published.
func (s *Server) SetEntries(e EntriesRequest) error {
	for _, en := range e.Entries {
		if len(en.Data) > 0 && !json.Valid(en.Data) {
			return &Error{Code: CodeBadRequest, Detail: "entry " + en.Name + ": data is not valid JSON"}
		}
	}
	// A copy that shares nothing with the caller's slices.
	cp := EntriesRequest{Reserved: append([]string(nil), e.Reserved...)}
	if e.Entries != nil {
		cp.Entries = make([]Entry, len(e.Entries))
		for i, en := range e.Entries {
			cp.Entries[i] = Entry{Name: en.Name, Namespace: en.Namespace, Data: append(json.RawMessage(nil), en.Data...)}
		}
	}
	s.mu.Lock()
	s.entries = &cp
	s.gen++
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
}

// errLeaseLost ends a session: serve again.
type errLeaseLost struct{ why string }

func (e *errLeaseLost) Error() string { return "lease lost: " + e.why }

// Run serves until ctx ends (then it drains and releases the lease and
// returns nil) or until a failure that trying again cannot mend, such as
// refused or bad-request on serve, which it returns. Everything else is
// retried with backoff: already-served (wait for the other lease to end),
// busy, not-ready, a Warren that is not there or restarts, a lease that
// ended (gone).
func (s *Server) Run(ctx context.Context) error {
	back := newBackoff(500*time.Millisecond, 10*time.Second)
	first := true
	for ctx.Err() == nil {
		l, err := s.c.Serve(ctx, s.o.Version, s.o.Take && first)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if _, isServer := AsError(err); isServer && !IsCode(err, CodeAlreadyServed, CodeBusy, CodeNotReady) {
				return err
			}
			s.o.Log.Warn("cannot serve yet", "err", err)
			sleep(ctx, back.next())
			continue
		}
		first = false
		started := time.Now()
		err = s.session(ctx, l)
		if time.Since(started) > 10*time.Second {
			back.reset()
		}
		if ctx.Err() != nil {
			break
		}
		if ll, ok := err.(*errLeaseLost); ok {
			s.o.Log.Warn("the lease ended; serving again", "why", ll.why)
			sleep(ctx, back.next())
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// session runs one lease until it is lost, a failure ends it, or ctx ends.
func (s *Server) session(ctx context.Context, l *Lease) error {
	self, err := s.c.Self(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.releaseQuietly(l)
			return &errLeaseLost{"cannot read self: " + err.Error()}
		}
		s.releaseQuietly(l)
		return nil
	}
	s.mu.Lock()
	changed := s.boot != "" && s.boot != self.Boot
	s.boot = self.Boot
	s.mu.Unlock()
	if s.o.OnLease != nil {
		s.o.OnLease(self, l, changed)
	}

	sctx, stop := context.WithCancel(ctx)                           // ends the polls and the publisher
	hctx, hcancel := context.WithCancel(context.WithoutCancel(ctx)) // ends handlers
	defer hcancel()
	lost := make(chan error, 1)
	fail := func(err error) {
		select {
		case lost <- err:
		default:
		}
	}
	var pollers, putter sync.WaitGroup
	putter.Add(1)
	go func() { defer putter.Done(); s.publish(sctx, l, fail) }()
	// A lease may have at most inflight polls open, and a socket sets inflight
	// to at most MaxInflight, so more pollers than that never help.
	n := min(max(l.Inflight, 1), MaxInflight)
	for i := 0; i < n; i++ {
		pollers.Add(1)
		go func() { defer pollers.Done(); s.poller(sctx, hctx, l, fail) }()
	}
	done := make(chan struct{})
	go func() { pollers.Wait(); close(done) }()

	var sessionErr error
	select {
	case sessionErr = <-lost:
		stop()
		hcancel() // the lease is gone: answers would only be refused
		<-done
	case <-ctx.Done():
		stop()
		// Calls handed out may finish; then the rest is cut.
		select {
		case <-done:
		case <-time.After(s.o.Drain):
			hcancel()
			<-done
		}
	}
	putter.Wait()
	stop()
	// Whatever ended the session, the lease is given up (a lost one answers
	// gone, which is fine): a process that serves again must not wait for the
	// old lease's grace.
	s.releaseQuietly(l)
	if ctx.Err() != nil {
		return nil
	}
	return sessionErr
}

func (s *Server) releaseQuietly(l *Lease) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.c.Release(ctx, l.Token); err != nil && !IsCode(err, CodeGone) {
		s.o.Log.Debug("release failed", "err", err)
	}
}

// poller is one of the lease's parallel polls: take a call, handle it,
// answer, poll again.
func (s *Server) poller(sctx, hctx context.Context, l *Lease, fail func(error)) {
	failures := 0
	for sctx.Err() == nil {
		call, err := s.c.Poll(sctx, l.Token, MaxWait)
		if err != nil {
			if sctx.Err() != nil {
				return
			}
			switch {
			case IsCode(err, CodeGone):
				fail(&errLeaseLost{"a poll was told gone"})
				return
			case IsCode(err, CodeBusy, CodeNotReady):
				sleep(sctx, time.Second)
			default:
				if _, isServer := AsError(err); isServer {
					fail(err) // bad-request and the like: serving again will not help
					return
				}
				// The socket could not be reached or broke. A few failures
				// in a row end the lease; one is retried.
				if failures++; failures >= 3 {
					fail(&errLeaseLost{"the socket failed: " + err.Error()})
					return
				}
				sleep(sctx, time.Duration(failures)*200*time.Millisecond)
			}
			continue
		}
		failures = 0
		if call == nil {
			continue
		}
		s.handle(hctx, l, call)
	}
}

// errPanicked marks a handler that panicked (already logged).
var errPanicked = errors.New("handler panicked")

func (s *Server) handle(hctx context.Context, l *Lease, call *Call) {
	cctx, cancel := context.WithDeadline(hctx, call.Deadline)
	defer cancel()
	var out []byte
	var herr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				// No answer: the call is given up below, and the service is up for the next call.
				s.o.Log.Error("a handler panicked; the call is left unanswered", "method", call.Method, "panic", r)
				herr = errPanicked
			}
		}()
		out, herr = s.h(cctx, call)
	}()
	if herr != nil {
		if herr != errPanicked {
			s.o.Log.Warn("a handler failed; the call is given up", "method", call.Method, "err", herr)
		}
		// Tell Warren, so the caller is not left to wait for its deadline and
		// the place is free at once. Nothing to do if that fails: the call
		// then ends at its deadline or with the lease.
		if hctx.Err() == nil {
			fctx, fcancel := context.WithTimeout(context.WithoutCancel(hctx), 5*time.Second)
			defer fcancel()
			if err := s.c.Fail(fctx, l.Token, call.ID); err != nil && !IsCode(err, CodeGone) {
				s.o.Log.Warn("cannot give a call up", "method", call.Method, "err", err)
			}
		}
		return
	}
	if hctx.Err() != nil {
		// The Server cut this handler off (the drain time passed, or the lease
		// is gone). Whatever it returned is not an answer: the lease is
		// released next, and that fails the call as unknown.
		s.o.Log.Warn("a handler was cut off by the stop; the call is left unanswered", "method", call.Method)
		return
	}
	if len(out) > MaxCallBody {
		s.o.Log.Error("an answer is over 1 MiB; the call is left unanswered", "method", call.Method, "bytes", len(out))
		return
	}
	// The answer is sent even when the Server is draining: that is what the
	// drain time is for. Its own bound is the call's deadline plus a little.
	actx, acancel := context.WithTimeout(context.WithoutCancel(hctx), time.Until(call.Deadline)+2*time.Second)
	defer acancel()
	if err := s.c.Answer(actx, l.Token, call.ID, out); err != nil && !IsCode(err, CodeGone) {
		s.o.Log.Warn("cannot answer a call", "method", call.Method, "err", err)
	}
}

// publish keeps the lease's entries published: after the lease starts, and
// whenever SetEntries changes the list, at most one PUT per second.
func (s *Server) publish(ctx context.Context, l *Lease, fail func(error)) {
	sent := 0 // the generation last published, 0 for none
	var last time.Time
	back := newBackoff(time.Second, 10*time.Second)
	for {
		s.mu.Lock()
		e, gen := s.entries, s.gen
		s.mu.Unlock()
		if e != nil && gen != sent {
			if wait := MinPutInterval + 100*time.Millisecond - time.Since(last); wait > 0 && !last.IsZero() {
				if !sleep(ctx, wait) {
					return
				}
				continue // the list may have changed while waiting
			}
			last = time.Now()
			refused, err := s.c.PutEntries(ctx, l.Token, *e)
			if ctx.Err() != nil {
				return
			}
			switch {
			case err == nil:
				sent = gen
				back.reset()
				if s.o.OnPut != nil {
					s.o.OnPut(refused, nil)
				}
			case IsCode(err, CodeGone):
				fail(&errLeaseLost{"the entries were refused: gone"})
				return
			case IsCode(err, CodeBusy, CodeNotReady):
				s.o.Log.Debug("entries not accepted yet", "err", err)
				if !sleep(ctx, back.next()) {
					return
				}
				continue
			default:
				if _, isServer := AsError(err); !isServer {
					// Unreachable: the polls will notice and end the lease.
					if !sleep(ctx, back.next()) {
						return
					}
					continue
				}
				// A list Warren refuses outright (too large, bad-request) is
				// reported once; the next SetEntries tries a new one.
				sent = gen
				s.o.Log.Error("entries refused", "err", err)
				if s.o.OnPut != nil {
					s.o.OnPut(nil, err)
				}
			}
			continue
		}
		select {
		case <-s.wake:
		case <-ctx.Done():
			return
		}
	}
}
