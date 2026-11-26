package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/config"
	"github.com/PeterSR/warren-mesh/internal/grammar"
	"github.com/PeterSR/warren-mesh/internal/link"
)

// The client side of the peer protocol (docs/architecture.md, "Peer
// protocol"): one session per peer the transport lists. A session says hello, then long-polls the peer's list of
// services and, for each service this node has too, its list of entries.
// Everything a peer says is checked by vouching (vouch.go) before use.

// Timing and limits of the peer protocol. Variables, so tests can shorten
// them.
var (
	// PeerDebounce delays a change of this node's lists to its peers, so a
	// burst of PUTs becomes one update.
	PeerDebounce = 1 * time.Second
	// PollGap is the least time between two polls of one list, so a peer that
	// answers at once every time cannot make a busy loop.
	PollGap = 250 * time.Millisecond
	// PeerBackoff bounds the wait before a failed session starts over.
	PeerBackoffMin, PeerBackoffMax = 500 * time.Millisecond, 10 * time.Second
	// PeerAnswerMargin is how long past a long poll's wait its answer may take.
	PeerAnswerMargin = 10 * time.Second
	// HelloWithin is how long a peer's hello may take.
	HelloWithin = 10 * time.Second
	// ClockTick is how often the clock is compared with the monotonic one,
	// and ClockJump how far ahead the wall clock must be to count as a jump.
	ClockTick = 5 * time.Second
	ClockJump = 3 * time.Second
	// PeerRetryFor is how long polls that the transport could not carry (not
	// sent, or busy) are tried again before the session is given up.
	PeerRetryFor = 30 * time.Second
	// DropLogEvery is the least time between two lines about dropped entries
	// of one peer's lists.
	DropLogEvery = time.Minute
)

// Fixed limits of the peer protocol.
const (
	// MaxPeerServices is how many services a node has and lists to peers.
	MaxPeerServices = config.MaxServices
	// MaxListPolls is how many long polls of one list one peer may have open.
	MaxListPolls = 2
	// MaxListBody is the largest list answer: 1000 entries of at most 3 KiB
	// data, plus names and origins.
	MaxListBody = 4 << 20
	// MaxVersionLen is the longest version string accepted from a peer.
	MaxVersionLen = 128
	// PeerProtocolHeader is on every peer request and answer: the protocol
	// version of the path.
	PeerProtocolHeader = "X-Warren-Peer"
	VersionHeader      = "X-Warren-Version"
	FeaturesHeader     = "X-Warren-Features"
)

// implementedProtocols are the peer protocols this build can speak as a
// client. A node announces what it can speak (Options.PeerProtocols, for
// tests); a version it announces without an implementation here is never
// chosen.
var implementedProtocols = map[int]bool{1: true}

// peerState is what a node knows of one peer. Guarded by Node.mu.
type peerState struct {
	node   string
	listed bool // the transport lists it now
	up     bool // hello worked and its polls work
	// downSince is when it was last up and went away; its entries are shown
	// stale for StaleFor from then.
	downSince time.Time
	proto     int    // the protocol chosen with it, 0 before hello
	compat    string // why no common protocol exists, "" if fine
	lastErr   string // the last failure, to log a change only
	version   string
	protocols []int
	features  []string
	lists     map[string]*remoteList
	dropLogAt time.Time // the last line about dropped entries of this peer
	// Lines about the peer going up and down are rate limited: when the last
	// was logged, what state it said, how many changes were held back since,
	// and the timer that logs the state the window ends in.
	stateLogAt time.Time
	loggedUp   bool
	suppressed int
	stateTimer *time.Timer

	cancel context.CancelFunc // stops its run; nil when none runs
	gen    int                // changes whenever a run starts or stops
	sess   *session
	timer  *time.Timer // drops its stale entries
}

// remoteList is a peer's list for one service, after vouching.
type remoteList struct {
	entries  []listed
	reserved []string
	// stale is set on the lists of an earlier session when a new one starts:
	// they are shown stale until the new session's first answer replaces them.
	stale bool
}

// visible reports whether the peer's entries are shown: while it is up, and
// stale for StaleFor after it went away.
func (ps *peerState) visible(now time.Time) bool {
	return ps.up || (!ps.downSince.IsZero() && now.Sub(ps.downSince) < StaleFor)
}

// session is one run of hello and polls against a peer.
type session struct {
	ps     *peerState
	gen    int
	proto  int
	ctx    context.Context
	cancel context.CancelFunc
	err    error // the first failure of any poll
	// svcs are the services the peer lists; pollers is one cancel per
	// service being polled.
	svcs    []string
	listed  bool // the peer's list of services has arrived
	pollers map[string]context.CancelFunc
}

// fail ends the session with err, if nothing failed before. mu held.
func (s *session) fail(err error) {
	if s.err == nil {
		s.err = err
	}
	s.cancel()
}

// sortedPeers returns the known peers by name. mu held.
func (n *Node) sortedPeers() []*peerState {
	out := make([]*peerState, 0, len(n.peerStates))
	for _, ps := range n.peerStates {
		out = append(out, ps)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].node < out[j].node })
	return out
}

// ourServices are the service names this node has sockets for, sorted. mu held.
func (n *Node) ourServices() []string {
	set := map[string]bool{}
	for k := range n.socks {
		set[k.service] = true
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	if len(out) > MaxPeerServices {
		out = out[:MaxPeerServices]
	}
	return out
}

// syncPeers makes the sessions match the peers the transport lists: one for
// each, none for the rest (which are marked down). mu held.
func (n *Node) syncPeers() {
	if n.ctx == nil || n.closed {
		return
	}
	want := map[string]bool{}
	for _, p := range n.peers {
		if grammar.Label(p) && p != n.name {
			want[p] = true
		}
	}
	if len(want) > AssumedPeers {
		n.warnOnce("many-peers", "more peers than the stream and memory estimates assume", "peers", len(want), "assumed", AssumedPeers)
	}
	for name := range want {
		ps := n.peerStates[name]
		if ps == nil {
			ps = &peerState{node: name, lists: map[string]*remoteList{}}
			n.peerStates[name] = ps
		}
		ps.listed = true
		if ps.cancel == nil {
			n.startPeer(ps)
		}
	}
	for name, ps := range n.peerStates {
		if want[name] {
			continue
		}
		ps.listed = false
		n.stopPeer(ps)
		n.markDown(ps, "the transport no longer lists it")
	}
}

func (n *Node) startPeer(ps *peerState) {
	ps.gen++
	gen := ps.gen
	ctx, cancel := context.WithCancel(n.ctx)
	ps.cancel = cancel
	n.wg.Add(1)
	go n.runPeer(ctx, ps, gen)
}

// stopPeer ends the peer's run and its session. mu held.
func (n *Node) stopPeer(ps *peerState) {
	if ps.cancel != nil {
		ps.cancel()
		ps.cancel = nil
	}
	ps.gen++
	if ps.sess != nil {
		ps.sess.cancel()
		ps.sess = nil
	}
}

// restartPolls starts every poll over: after a clock jump the requests in
// flight are almost certainly dead, and their timers stood still. mu held.
func (n *Node) restartPolls(why string) {
	n.log.Info("restarting polls", "why", why)
	for _, ps := range n.peerStates {
		n.stopPeer(ps)
	}
	if n.linkCancel != nil {
		n.linkCancel()
	}
	n.syncPeers()
}

// peerStateLog logs a change of a peer's state, at most one line per peer per
// DropLogEvery: a peer that comes and goes cannot fill the log. A change that
// is held back is counted, and when the window ends the state the peer is in
// then is logged if it is not the one last logged, so the log never ends on
// the wrong state. mu held.
func (n *Node) peerStateLog(ps *peerState, level slog.Level, msg string, args ...any) {
	if ps.stateLogAt.IsZero() || time.Since(ps.stateLogAt) >= DropLogEvery {
		n.emitPeerState(ps, level, msg, args...)
		return
	}
	ps.suppressed++
	if ps.stateTimer == nil {
		ps.stateTimer = time.AfterFunc(time.Until(ps.stateLogAt.Add(DropLogEvery))+10*time.Millisecond, func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			ps.stateTimer = nil
			if n.closed || ps.up == ps.loggedUp {
				ps.suppressed = 0
				return
			}
			if ps.up {
				n.emitPeerState(ps, slog.LevelInfo, "peer up", "node", ps.node, "version", ps.version)
			} else {
				n.emitPeerState(ps, slog.LevelWarn, "peer down", "node", ps.node, "why", "state after the changes held back")
			}
		})
	}
}

func (n *Node) emitPeerState(ps *peerState, level slog.Level, msg string, args ...any) {
	ps.stateLogAt, ps.loggedUp = time.Now(), ps.up
	if ps.suppressed > 0 {
		args = append(args, "held_back_changes", ps.suppressed)
		ps.suppressed = 0
	}
	n.log.Log(context.Background(), level, msg, args...)
}

// markDown records that a peer is gone: its entries are stale from now, and
// dropped StaleFor later. mu held.
func (n *Node) markDown(ps *peerState, why string) {
	if ps.up {
		ps.up = false
		ps.downSince = time.Now()
		n.peerStateLog(ps, slog.LevelWarn, "peer down", "node", ps.node, "why", why)
		n.rebuildPeer(ps)
		if ps.timer != nil {
			ps.timer.Stop()
		}
		ps.timer = time.AfterFunc(StaleFor+10*time.Millisecond, func() { n.expirePeer(ps) })
		return
	}
	if !ps.listed && len(ps.lists) == 0 && ps.timer == nil {
		delete(n.peerStates, ps.node)
	}
}

// expirePeer drops the entries of a peer that has been away for StaleFor.
func (n *Node) expirePeer(ps *peerState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed || ps.up || time.Since(ps.downSince) < StaleFor {
		return
	}
	ps.timer = nil
	svcs := ps.serviceNames()
	ps.lists = map[string]*remoteList{}
	for _, s := range svcs {
		n.rebuild(n.svc(s))
	}
	if !ps.listed && n.peerStates[ps.node] == ps {
		delete(n.peerStates, ps.node)
	}
}

func (ps *peerState) serviceNames() []string {
	out := make([]string, 0, len(ps.lists))
	for s := range ps.lists {
		out = append(out, s)
	}
	return out
}

// rebuildPeer works out the views of the services the peer has lists for. mu held.
func (n *Node) rebuildPeer(ps *peerState) {
	for s := range ps.lists {
		n.rebuild(n.svc(s))
	}
}

// ---- running a peer ----

// runPeer is a peer's life: sessions one after another, with backoff after a
// failure, until its context ends.
func (n *Node) runPeer(ctx context.Context, ps *peerState, gen int) {
	defer n.wg.Done()
	backoff := PeerBackoffMin
	for ctx.Err() == nil {
		healthy, err := n.session(ctx, ps, gen)
		if ctx.Err() != nil {
			return
		}
		n.peerFailed(ps, gen, err)
		// Only a session that worked for a while earns a short backoff: a
		// peer that answers hello and then fails its polls is not hammered.
		if healthy {
			backoff = PeerBackoffMin
			n.mu.Lock()
			ps.lastErr = ""
			n.mu.Unlock()
		}
		// Jitter keeps nodes from polling a restarted one in step.
		wait := backoff/2 + time.Duration(rand.Int64N(int64(backoff/2)+1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		backoff = min(backoff*2, PeerBackoffMax)
	}
}

// peerFailed marks the peer down after a failed session. A change of the
// failure is logged; the same one again is not.
func (n *Node) peerFailed(ps *peerState, gen int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ps.gen != gen || n.closed {
		return
	}
	if ps.sess != nil {
		ps.sess.cancel()
		ps.sess = nil
	}
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	// The reason is logged when it changes; it is forgotten once a session
	// has been healthy, so a peer that fails the same way in a loop is one line.
	if msg != ps.lastErr {
		ps.lastErr = msg
		n.log.Warn("peer not usable", "node", ps.node, "err", err)
	}
	n.markDown(ps, msg)
}

// session is one hello plus its polls; it returns when any of them fails.
func (n *Node) session(ctx context.Context, ps *peerState, gen int) (healthy bool, err error) {
	proto, err := n.hello(ctx, ps, gen)
	if err != nil {
		return false, err
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := &session{ps: ps, gen: gen, proto: proto, ctx: sctx, cancel: cancel, pollers: map[string]context.CancelFunc{}}
	n.mu.Lock()
	if ps.gen != gen || n.closed {
		n.mu.Unlock()
		return false, ctx.Err()
	}
	ps.sess = s
	n.mu.Unlock()
	var goodAt time.Time // the first good answer; the session is healthy a while after
	defer func() { healthy = !goodAt.IsZero() && time.Since(goodAt) >= PeerBackoffMax }()
	since := ""
	var rt retrier
	for sctx.Err() == nil {
		resp, e := n.peerDo(sctx, ps.node, proto, "GET", "services?"+pollQuery(since, ""), nil, maxWait*WaitUnit+PeerAnswerMargin)
		if e != nil {
			if retryable(e) && sctx.Err() == nil && rt.wait(sctx) {
				continue
			}
			if sctx.Err() != nil {
				break // ended by another poll's failure: that is the reason
			}
			return false, e
		}
		rt.reset()
		var body struct {
			Node     string   `json:"node"`
			Version  string   `json:"version"`
			Services []string `json:"services"`
		}
		if err := decodeList(resp.Body, &body); err != nil {
			return false, fmt.Errorf("services list: %w", err)
		}
		if body.Node != ps.node || body.Version == "" || len(body.Version) > MaxVersionLen || body.Services == nil {
			return false, fmt.Errorf("services list: answered as %q with version %q", body.Node, body.Version)
		}
		var names []string
		seenSvc := map[string]bool{}
		for _, sv := range body.Services {
			if len(names) >= MaxPeerServices {
				break
			}
			if grammar.Service(sv) && !seenSvc[sv] {
				seenSvc[sv] = true
				names = append(names, sv)
			}
		}
		n.mu.Lock()
		if ps.sess != s {
			n.mu.Unlock()
			break
		}
		s.svcs, s.listed = names, true
		n.reconcilePollers(s)
		n.mu.Unlock()
		since = body.Version
		if goodAt.IsZero() {
			goodAt = time.Now()
		}
		if err := sleepCtx(sctx, PollGap); err != nil {
			break
		}
	}
	n.mu.Lock()
	err = s.err
	n.mu.Unlock()
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = errors.New("the session ended")
	}
	return false, err
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func pollQuery(since, service string) string {
	q := url.Values{"since": {since}, "wait": {strconv.Itoa(maxWait)}}
	if service != "" {
		q.Set("service", service)
	}
	return q.Encode()
}

// decodeList decodes a list answer: malformed or oversized is an error,
// never an empty list.
// errListTooBig is a list answer over MaxListBody: that one list is dropped,
// and the session goes on.
var errListTooBig = errors.New("list too big")

func decodeList(b []byte, v any) error {
	if len(b) > MaxListBody {
		return fmt.Errorf("%w: the answer is over %d bytes", errListTooBig, MaxListBody)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("malformed answer: %w", err)
	}
	return nil
}

// reconcilePollers starts a poller for each service both the peer and this
// node have, at most MaxPeerServices, and stops those of services that
// went away (dropping the peer's list for them). mu held.
func (n *Node) reconcilePollers(s *session) {
	if s.ps.sess != s || !s.listed {
		return // before the peer's list of services there is nothing to match
	}
	ours := n.ourServices()
	var want []string
	for _, sv := range s.svcs {
		if slices.Contains(ours, sv) {
			want = append(want, sv)
		}
	}
	for sv, cancel := range s.pollers {
		if !slices.Contains(want, sv) {
			cancel()
			delete(s.pollers, sv)
			if s.ps.lists[sv] != nil {
				delete(s.ps.lists, sv)
				n.rebuild(n.svc(sv))
			}
		}
	}
	// A list of a service nobody polls any more (the peer dropped it while it
	// was away, or this node did) is not kept: it would stay listed as live.
	for sv := range s.ps.lists {
		if !slices.Contains(want, sv) {
			delete(s.ps.lists, sv)
			n.rebuild(n.svc(sv))
		}
	}
	for _, sv := range want {
		if _, ok := s.pollers[sv]; ok {
			continue
		}
		pctx, cancel := context.WithCancel(s.ctx)
		s.pollers[sv] = cancel
		n.wg.Add(1)
		go n.pollEntries(pctx, s, sv)
	}
}

// reconcileAllPeers is reconcilePollers for every session, after this
// node's own services changed. mu held.
func (n *Node) reconcileAllPeers() {
	for _, ps := range n.peerStates {
		if ps.sess != nil {
			n.reconcilePollers(ps.sess)
		}
	}
}

// retrier spaces out the retries of a poll that the transport could not
// carry now (not sent, or busy): a growing wait that starts at PollGap, and
// gives up after PeerRetryFor of failures in a row.
type retrier struct {
	first time.Time
	delay time.Duration
}

func (r *retrier) reset() { r.first, r.delay = time.Time{}, 0 }

// wait sleeps before the next try; false when the retries are used up or ctx ended.
func (r *retrier) wait(ctx context.Context) bool {
	if r.first.IsZero() {
		r.first, r.delay = time.Now(), PollGap
	} else if time.Since(r.first) > PeerRetryFor {
		return false
	}
	d := r.delay
	r.delay = min(r.delay*2, PeerBackoffMax)
	return sleepCtx(ctx, d) == nil
}

// retryable is whether a failed poll is tried again without ending the
// session: the transport did not send it (a limit of its own, or no
// connection for the moment), which says nothing about the peer.
func retryable(e *apierr.Error) bool { return e.Code == apierr.Offline || e.Code == apierr.Busy }

// failSession ends the session with err unless the poll was stopped on purpose.
func (n *Node) failSession(ctx context.Context, s *session, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ctx.Err() == nil {
		s.fail(err)
	}
}

// pollEntries long-polls one peer's entries for one service.
func (n *Node) pollEntries(ctx context.Context, s *session, service string) {
	defer n.wg.Done()
	since := ""
	var rt retrier
	for ctx.Err() == nil {
		resp, e := n.peerDo(ctx, s.ps.node, s.proto, "GET", "entries?"+pollQuery(since, service), nil, maxWait*WaitUnit+PeerAnswerMargin)
		if e == nil {
			ver, changed, err := n.acceptEntries(s, service, resp.Body)
			if errors.Is(err, errListTooBig) {
				// Only this list is lost: it is dropped, and asked for again
				// later, in case the peer has cut it down by then.
				n.dropList(ctx, s, service, "the list is over the size limit")
				if sleepCtx(ctx, PeerBackoffMax) != nil {
					return
				}
				continue
			}
			if err != nil {
				n.failSession(ctx, s, fmt.Errorf("entries of %s: %w", service, err))
				return
			}
			rt.reset()
			// After a changed answer the next poll waits a debounce, so a
			// peer that changes all the time is not asked all the time.
			gap := PollGap
			if changed || since == "" {
				gap = PeerDebounce
			}
			since = ver
			if sleepCtx(ctx, gap) != nil {
				return
			}
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if e.Code == apierr.TooLarge {
			// The answer was too big to carry: the same as a list over the limit.
			n.dropList(ctx, s, service, "the list is over the size limit")
			if sleepCtx(ctx, PeerBackoffMax) != nil {
				return
			}
			continue
		}
		if retryable(e) {
			if rt.wait(ctx) {
				continue
			}
			n.failSession(ctx, s, fmt.Errorf("entries of %s: %w", service, e))
			return
		}
		if e.Code == apierr.Refused {
			// The peer no longer has the service (its list of services lags by
			// the debounce), or has it again before that list says so: drop the
			// list, keep the session, and ask again after a while.
			n.dropList(ctx, s, service, "refused by the peer")
			if sleepCtx(ctx, PeerBackoffMax) != nil {
				return
			}
			continue
		}
		n.failSession(ctx, s, fmt.Errorf("entries of %s: %w", service, e))
		return
	}
}

// dropList forgets the peer's list for a service, if this poll is still
// current. The session goes on.
func (n *Node) dropList(ctx context.Context, s *session, service, why string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ctx.Err() != nil || s.ps.sess != s {
		return
	}
	n.warnOnce("droplist|"+s.ps.node+"|"+service+"|"+why, "dropped a peer's list", "node", s.ps.node, "service", service, "why", why)
	if s.ps.lists[service] != nil {
		delete(s.ps.lists, service)
		n.rebuild(n.svc(service))
	}
}

// acceptEntries vouches for a peer's list and stores what passes. It
// returns the list's version.
func (n *Node) acceptEntries(s *session, service string, body []byte) (version string, changed bool, err error) {
	var l struct {
		Node     string              `json:"node"`
		Version  string              `json:"version"`
		Service  string              `json:"service"`
		Reserved map[string][]string `json:"reserved"`
		Entries  *[]wireEntry        `json:"entries"`
	}
	if err := decodeList(body, &l); err != nil {
		return "", false, err
	}
	node := s.ps.node
	switch {
	case l.Node != node:
		return "", false, fmt.Errorf("answered as %q, not %s", l.Node, node)
	case len(l.Version) > MaxVersionLen:
		return "", false, fmt.Errorf("the list's version is over %d bytes", MaxVersionLen)
	case l.Service != service || l.Version == "" || l.Entries == nil:
		return "", false, fmt.Errorf("not the list asked for (service %q, version %q)", l.Service, l.Version)
	}
	// Vouching works on what a peer chose and costs time: not under mu.
	counts, first := map[string]int{}, []string{}
	entries, reserved := vouchList(node, *l.Entries, l.Reserved, func(what, reason string) {
		counts[reason]++
		if len(first) < 3 {
			first = append(first, grammar.CleanText(what, 80))
		}
	})
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(counts) > 0 {
		keys := make([]string, 0, len(counts))
		for r := range counts {
			keys = append(keys, r)
		}
		sort.Strings(keys)
		var sum []string
		for _, r := range keys {
			sum = append(sum, fmt.Sprintf("%s=%d", r, counts[r]))
		}
		// One line per list at most, the same set of reasons again is not
		// logged again, and one peer gets one line per DropLogEvery: what a
		// peer chooses must not fill the log or push out other warnings.
		key := "vouch|" + node + "|" + service + "|" + strings.Join(keys, ",")
		if !n.warned[key] && time.Since(s.ps.dropLogAt) >= DropLogEvery {
			s.ps.dropLogAt = time.Now()
			n.warnOnce(key, "dropped entries of a peer's list", "node", node, "service", service, "dropped", strings.Join(sum, " "), "first", first)
		}
	}
	if _, polled := s.pollers[service]; s.ps.sess != s || !polled || n.closed {
		return l.Version, false, nil // the session or the poll has ended meanwhile
	}
	old := s.ps.lists[service]
	nl := &remoteList{entries: entries, reserved: reserved}
	if old != nil && !old.stale && sameList(old, nl) {
		return l.Version, false, nil // nothing to rebuild
	}
	s.ps.lists[service] = nl
	n.rebuild(n.svc(service))
	return l.Version, true, nil
}

// sameList is whether two lists say the same.
func sameList(a, b *remoteList) bool {
	return slices.Equal(a.reserved, b.reserved) && slices.EqualFunc(a.entries, b.entries, func(x, y listed) bool {
		return x.Address == y.Address && x.Origin == y.Origin && x.Stale == y.Stale && bytes.Equal(x.Data, y.Data)
	})
}

// ---- hello and requests ----

type wireHello struct {
	Node         string   `json:"node"`
	Version      string   `json:"version"`
	PeerProtocol []int    `json:"peer_protocol"`
	Features     []string `json:"features"`
}

// hello asks the peer about itself and chooses the protocol: the highest
// both announce. Hello is sent on the lowest version this node speaks, the
// one every release of the window has. It marks the peer up.
func (n *Node) hello(ctx context.Context, ps *peerState, gen int) (int, error) {
	// Hello goes on the lowest version this node speaks; a peer that does
	// not have that path (it speaks only newer ones) is tried on the next.
	var resp *link.Response
	for _, p := range n.protocols {
		r, e := n.peerDo(ctx, ps.node, p, "GET", "hello", nil, HelloWithin)
		if e == nil {
			resp = r
			break
		}
		if e.Code != apierr.Unsupported {
			return 0, e
		}
	}
	if resp == nil {
		return 0, n.incompatible(ps, gen, fmt.Sprintf("the peer speaks none of this node's peer protocols %v", n.protocols))
	}
	var h wireHello
	if err := json.Unmarshal(resp.Body, &h); err != nil {
		return 0, fmt.Errorf("hello: malformed answer: %w", err)
	}
	if h.Node != ps.node {
		return 0, fmt.Errorf("hello: answered as %q, not %s", h.Node, ps.node)
	}
	proto := 0
	for _, p := range n.protocols {
		if slices.Contains(h.PeerProtocol, p) && implementedProtocols[p] && p > proto {
			proto = p
		}
	}
	if proto == 0 {
		return 0, n.incompatible(ps, gen, fmt.Sprintf("no common peer protocol: the peer speaks %v, this node %v", h.PeerProtocol, n.protocols))
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if ps.gen != gen || n.closed {
		return 0, ctx.Err()
	}
	// What the peer said in an earlier session is stale until this session's
	// first answer replaces it.
	for _, rl := range ps.lists {
		rl.stale = true
	}
	n.rebuildPeer(ps)
	ps.proto, ps.compat = proto, ""
	ps.version, ps.protocols, ps.features = cleanVersion(h.Version), cleanInts(h.PeerProtocol), cleanFeatures(h.Features)
	if !ps.up {
		ps.up, ps.downSince = true, time.Time{}
		if ps.timer != nil {
			ps.timer.Stop()
			ps.timer = nil
		}
		n.peerStateLog(ps, slog.LevelInfo, "peer up", "node", ps.node, "version", ps.version, "protocol", proto)
		n.rebuildPeer(ps)
	}
	return proto, nil
}

// incompatible records, and logs, that a peer shares no peer protocol with
// this node. Its entries are never shown and calls to it fail as offline.
func (n *Node) incompatible(ps *peerState, gen int, why string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ps.gen == gen && ps.compat != why {
		ps.compat = why
		n.log.Error("peer refused: "+why, "node", ps.node)
	}
	return errors.New(why)
}

func cleanVersion(s string) string { return grammar.CleanText(s, 64) }

func cleanInts(in []int) []int {
	out := []int{}
	for _, v := range in {
		if v >= 1 && len(out) < 16 {
			out = append(out, v)
		}
	}
	return out
}

func cleanFeatures(in []string) []string {
	out := []string{}
	for _, f := range in {
		if f = grammar.CleanText(f, 32); f != "" && len(out) < 32 {
			out = append(out, f)
		}
	}
	return out
}

// errAnswerTooBig is what peerDoUntil returns for an answer over the size a
// transport carries back. The list pollers drop just that list; a call
// reports it as unknown, since the call was sent and its answer is lost. It
// is shared and compared by pointer, so nobody may change it.
var errAnswerTooBig = apierr.New(apierr.TooLarge, "the answer is over the size a transport carries")

// peerDo sends one peer request to node through the link and returns the
// answer if it is a success from that node. Every failure is an *apierr.Error:
// a request that never left is offline, one that may have is unknown, a
// deadline that passed is timeout, a peer's own error keeps its code. An
// answer that does not come from the node reached, or does not carry the
// protocol of the request, is refused and never decoded.
func (n *Node) peerDo(ctx context.Context, node string, proto int, method, rest string, body []byte, within time.Duration) (*link.Response, *apierr.Error) {
	deadline := time.Now().Add(within)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	return n.peerDoUntil(ctx, node, proto, method, rest, body, deadline)
}

func (n *Node) peerDoUntil(ctx context.Context, node string, proto int, method, rest string, body []byte, deadline time.Time) (*link.Response, *apierr.Error) {
	// The deadline is enforced here, whatever the link does with it.
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	h := http.Header{}
	h.Set(PeerProtocolHeader, strconv.Itoa(proto))
	h.Set(VersionHeader, Version)
	h.Set(FeaturesHeader, strings.Join(Features, ","))
	if body != nil {
		h.Set("Content-Type", "application/octet-stream")
	}
	resp, err := n.link.Carry(ctx, node, &link.Request{Method: method, URI: fmt.Sprintf("/v%d/peer/%s", proto, rest), Header: h, Body: body, Deadline: deadline})
	if err != nil {
		var ce *link.CarryError
		switch {
		case errors.As(err, &ce) && ce.TooBig:
			return nil, errAnswerTooBig
		case errors.As(err, &ce) && ce.Kind == link.Busy:
			return nil, apierr.New(apierr.Busy, "the transport is busy: "+ce.Detail)
		case errors.As(err, &ce) && ce.Kind == link.NotSent:
			return nil, apierr.New(apierr.Offline, "the request never left: "+ce.Detail)
		case !time.Now().Before(deadline) || errors.Is(err, context.DeadlineExceeded):
			return nil, apierr.New(apierr.Timeout, "no answer from "+node+" in time")
		default:
			return nil, apierr.New(apierr.Unknown, "the request may have reached "+node+": "+err.Error())
		}
	}
	if got := resp.Header.Get(link.HeaderNode); got != node {
		return nil, apierr.New(apierr.Unknown, fmt.Sprintf("the answer came from %q, not from %s", grammar.CleanText(got, 64), node))
	}
	if resp.Status == http.StatusOK {
		if got := resp.Header.Get(PeerProtocolHeader); got != strconv.Itoa(proto) {
			return nil, apierr.New(apierr.Unknown, fmt.Sprintf("%s answered peer protocol %q to a protocol %d request; refused", node, grammar.CleanText(got, 16), proto))
		}
		return resp, nil
	}
	code := resp.Header.Get(apierr.Header)
	var b struct {
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(resp.Body, &b)
	if !apierr.Known(code) {
		return nil, apierr.New(apierr.Unknown, fmt.Sprintf("%s answered status %d without a known error code", node, resp.Status))
	}
	return nil, apierr.New(code, b.Detail)
}

// ---- clock ----

// watchClock restarts every poll when the wall clock jumps ahead of the
// monotonic one, which is what a resume from suspend looks like.
func (n *Node) watchClock() {
	defer n.wg.Done()
	t := time.NewTicker(ClockTick)
	defer t.Stop()
	wall, mono := n.clock()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		}
		w, m := n.clock()
		gap := (w - wall) - (m - mono)
		wall, mono = w, m
		if gap > ClockJump {
			n.mu.Lock()
			if !n.closed {
				n.restartPolls(fmt.Sprintf("the clock jumped %v ahead of the monotonic one (resume from suspend?)", gap.Round(time.Second)))
			}
			n.mu.Unlock()
		}
	}
}

// systemClock is the wall clock and the monotonic clock, as durations from
// a fixed start.
func systemClock() func() (time.Duration, time.Duration) {
	start := time.Now()
	return func() (time.Duration, time.Duration) {
		now := time.Now()
		return now.Round(0).Sub(start.Round(0)), now.Sub(start)
	}
}

// peerFeature reports whether node announced the feature. A node uses a
// feature with a peer only when the peer announced it.
func (n *Node) peerFeature(node, feature string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	ps := n.peerStates[node]
	return ps != nil && ps.up && slices.Contains(ps.features, feature)
}

// peerStatus is the status entry of each known peer. mu held.
func (n *Node) peerStatus() []map[string]any {
	out := []map[string]any{}
	now := time.Now()
	for _, ps := range n.sortedPeers() {
		if !ps.listed && !ps.visible(now) {
			continue
		}
		e := map[string]any{"node": ps.node, "up": ps.up, "version": ps.version,
			"peer_protocol": nonNilInts(ps.protocols), "features": nonNilStrs(ps.features)}
		if ps.compat != "" {
			e["error"] = ps.compat
		}
		out = append(out, e)
	}
	return out
}

func nonNilInts(v []int) []int {
	if v == nil {
		return []int{}
	}
	return v
}

func nonNilStrs(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}
