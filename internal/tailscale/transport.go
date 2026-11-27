package tailscale

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/link"
	"github.com/PeterSR/warren-mesh/internal/socketdir"
	"github.com/PeterSR/warren-mesh/internal/version"
)

// Settings are the transport's block of the Warren config, handed over in
// WARREN_TRANSPORT_SETTINGS as JSON.
type Settings struct {
	// Tag is the tailnet tag a device must carry to be a peer, with or
	// without the "tag:" prefix.
	Tag string `json:"tag"`
	// Listen is where to listen for peers; default: the tailnet address on
	// Port. It is the published port inside Docker.
	Listen string `json:"listen"`
	// Port is where peers listen, default 7778.
	Port int `json:"port"`
	// Socket is tailscaled's local API socket.
	Socket string `json:"socket"`
}

// Limits and timing. Variables, so tests can shorten them.
var (
	// RefreshEvery is how often tailscaled is asked who this device and its
	// peers are, and the listener is checked against the address.
	RefreshEvery = 10 * time.Second
	// WhoisFor is how long a caller's whois answer is reused.
	WhoisFor = time.Minute
	// DialTimeout is how long connecting to a peer may take.
	DialTimeout = 10 * time.Second
	// BodyRead is how long a peer may take to send a request body.
	BodyRead = 30 * time.Second
	// ClockTick is how often the wall clock is compared with the monotonic
	// one, and ClockJump how far ahead it must be to count as a resume from
	// suspend.
	ClockTick = 5 * time.Second
	ClockJump = 3 * time.Second
	// In-flight requests from the tailnet: at most MaxPerCaller from one node
	// and MaxInflight in all, and the bodies being buffered at most
	// MaxBodyPerCaller and MaxBodyTotal bytes (a request is counted by its
	// declared length, or MaxBody if it declares none). Each is refused with
	// busy (and not-sent) when over, before its body is read. The memory this
	// transport holds for peers is at most MaxBodyTotal plus the answers of
	// the requests in flight, each read from Warren up to maxAnswer (8 MiB)
	// before it is judged too big and held until it is written out.
	//
	// Warren's long polls and hello (GET hello, services, entries) have places
	// of their own, so a burst of calls never starves them: Warren already
	// bounds them per peer (MaxListPolls open per list, at most MaxServices
	// lists), and MaxPollsPerCaller / MaxPollsTotal leave room for that.
	//
	// MaxInflight + MaxPollsTotal stay within the streams link B allows
	// (link.MinStreams), so Warren is never asked for more than it has.
	MaxPerCaller      = 128
	MaxInflight       = 256
	MaxPollsPerCaller = 2*(2+32) + 12
	MaxPollsTotal     = 700
	MaxBodyPerCaller  = 16 << 20
	MaxBodyTotal      = 64 << 20
	// Connections from the tailnet: at most MaxConnsPerIP from one address
	// and MaxConns in all, and an idle one is closed after ConnIdle.
	//
	// HTTP/1.1 takes one connection per request in flight, so the cap per
	// address must hold a caller's calls and polls (MaxPerCaller +
	// MaxPollsPerCaller) and its idle keep-alives, with headroom; and a peer's
	// own client never opens more than MaxClientConnsPerPeer to one peer, which
	// stays within it: the cap is never what rejects a request. MaxConns is
	// room for AssumedPeers peers at the cap.
	MaxConnsPerIP = 256
	MaxConns      = 2560
	// MaxClientConnsPerPeer bounds this transport's connections to one peer.
	MaxClientConnsPerPeer = 224
	ConnIdle              = 30 * time.Second
	// DefaultDeadline applies to a carried request that came without one.
	DefaultDeadline = 30 * time.Second
)

const (
	// DefaultPort is where peers listen unless the settings say otherwise.
	DefaultPort = 7778
	// MaxBody is the max_body this transport announces: more than the
	// largest peer list answer (4 MiB) plus headers.
	MaxBody = 5 << 20
	// maxAnswer bounds what is read back from a peer.
	maxAnswer = 8 << 20
	maxWait   = 25
	// callMargin is the room above Warren's own timeouts that a peer's
	// deadline may have (the same as Warren's PeerAnswerMargin).
	callMargin = 10 * time.Second
)

// Transport is warren-tailscale. It is the Backend of the /v1/transport API
// (link A), the client of Warren's link B, and the server and client of the
// peer traffic on the tailnet.
type Transport struct {
	set  Settings
	tag  string
	log  *slog.Logger
	api  *localAPI
	port int

	ready chan struct{} // closed once tailscaled has said who this device is
	// fatal carries an error that must end the transport (a rename).
	fatal chan error

	mu       sync.Mutex
	selfName string
	selfIP   string
	peers    map[string]string // node name -> tailnet address
	ver      int
	wake     chan struct{} // closed, then replaced, when the peer list changes
	fails    int           // refreshes that failed in a row
	lastErr  string
	hc       *http.Client // to peers; bound to the tailnet address
	whois    map[string]whoisEntry
	whoisGen uint64
	srv      *http.Server
	bound    string
	listenOK bool // a listen attempt failed and was logged
	linkB    *http.Client

	// The client to peers is made again, and what was in flight on the old
	// one cancelled, when the address changes or the machine resumes from
	// suspend: its connections are dead, or leave from the wrong address.
	dialer  *net.Dialer
	gctx    context.Context
	gcancel context.CancelFunc
	clock   func() (wall, mono time.Duration)
	// clockTick and clockJump are ClockTick and ClockJump as they were at New.
	clockTick, clockJump time.Duration

	// Admission of requests from the tailnet.
	lim        limits
	conns      int
	connsBy    map[string]int
	flights    map[string]*whoisFlight
	pollsTotal int
	pollsBy    map[string]int
	refusedAt  map[string]time.Time // the last "refused a caller" line per address
	inflight   int
	bodyTotal  int64
	byCaller   map[string]int
	bodyBy     map[string]int64
}

// clockOverride replaces the clock of new transports (tests).
var clockOverride func() (wall, mono time.Duration)

func systemClock() func() (time.Duration, time.Duration) {
	start := time.Now()
	return func() (time.Duration, time.Duration) {
		now := time.Now()
		return now.Round(0).Sub(start.Round(0)), now.Sub(start)
	}
}

// newClientLocked makes a fresh client to peers and cancels everything that
// was in flight on the old one. mu held.
func (t *Transport) newClientLocked() {
	if t.gcancel != nil {
		t.gcancel()
	}
	if t.hc != nil {
		t.hc.CloseIdleConnections()
	}
	t.gctx, t.gcancel = context.WithCancel(context.Background())
	t.hc = newPeerClient(t.dialer)
}

// watchClock makes the client again when the wall clock jumps ahead of the
// monotonic one, as it does on a resume from suspend: pooled connections are
// dead and requests in flight will never be answered.
func (t *Transport) watchClock(ctx context.Context) {
	tk := time.NewTicker(t.clockTick)
	defer tk.Stop()
	wall, mono := t.clock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		w, m := t.clock()
		gap := (w - wall) - (m - mono)
		wall, mono = w, m
		if gap > t.clockJump {
			t.mu.Lock()
			if t.dialer != nil {
				t.log.Info("clock jumped (resume from suspend?); dropping connections to peers", "gap", gap.Round(time.Second))
				t.newClientLocked()
				t.whois = map[string]whoisEntry{}
				t.whoisGen++
			}
			t.mu.Unlock()
		}
	}
}

// limits are the limits of admission, as the package variables were at New.
type limits struct {
	perCaller, inflight, pollsPerCaller, pollsTotal int
	bodyPerCaller, bodyTotal                        int64
	connsPerIP, conns                               int
}

// SetLimits changes the limits of a running transport (tests).
func (t *Transport) SetLimits(f func(perCaller, inflight *int)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	f(&t.lim.perCaller, &t.lim.inflight)
}

// admit takes a place for a request from caller whose body is about to be
// read. cost is the body bytes it may hold. It reports false when a limit is
// reached.
func (t *Transport) admit(caller string, cost int64, poll bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	l := t.lim
	if poll {
		if t.pollsTotal >= l.pollsTotal || t.pollsBy[caller] >= l.pollsPerCaller {
			return false
		}
		t.pollsTotal++
		t.pollsBy[caller]++
		return true
	}
	if t.inflight >= l.inflight || t.byCaller[caller] >= l.perCaller ||
		t.bodyTotal+cost > l.bodyTotal || t.bodyBy[caller]+cost > l.bodyPerCaller {
		return false
	}
	t.inflight++
	t.byCaller[caller]++
	t.bodyTotal += cost
	t.bodyBy[caller] += cost
	return true
}

func (t *Transport) release(caller string, cost int64, poll bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if poll {
		t.pollsTotal--
		if t.pollsBy[caller]--; t.pollsBy[caller] <= 0 {
			delete(t.pollsBy, caller)
		}
		return
	}
	t.inflight--
	t.bodyTotal -= cost
	if t.byCaller[caller]--; t.byCaller[caller] <= 0 {
		delete(t.byCaller, caller)
	}
	if t.bodyBy[caller] -= cost; t.bodyBy[caller] <= 0 {
		delete(t.bodyBy, caller)
	}
}

type whoisEntry struct {
	node   string
	tagged bool
	until  time.Time
}

// New makes a transport; Run (or RunManaged, RunExternal) starts it.
func New(set Settings, log *slog.Logger) *Transport {
	if log == nil {
		log = slog.Default()
	}
	port := set.Port
	if port == 0 {
		port = DefaultPort
	}
	t := &Transport{set: set, tag: normaliseTag(set.Tag), log: log, api: newLocalAPI(set.Socket), port: port,
		ready: make(chan struct{}), fatal: make(chan error, 1), peers: map[string]string{}, wake: make(chan struct{}), whois: map[string]whoisEntry{},
		clock: systemClock(), clockTick: ClockTick, clockJump: ClockJump, byCaller: map[string]int{}, bodyBy: map[string]int64{}, pollsBy: map[string]int{}, refusedAt: map[string]time.Time{},
		connsBy: map[string]int{}, flights: map[string]*whoisFlight{},
		lim: limits{MaxPerCaller, MaxInflight, MaxPollsPerCaller, MaxPollsTotal, int64(MaxBodyPerCaller), int64(MaxBodyTotal), MaxConnsPerIP, MaxConns}}
	if clockOverride != nil {
		t.clock = clockOverride
	}
	return t
}

// ---- running ----

// serveA serves the transport API on link A with h. It returns a channel
// closed when the link ends, and a function that stops serving.
type serveA func(h http.Handler) (closed <-chan struct{}, stop func(), err error)

// run is the life of the transport once its links exist: refresh from
// tailscaled, listen for peers, serve link A, until ctx ends or link A does.
func (t *Transport) run(ctx context.Context, serve serveA, dialB func(ctx context.Context) (net.Conn, error)) error {
	t.linkB = link.NewH2CClient(dialB)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	closed, stop, err := serve(link.TransportHandler(t))
	if err != nil {
		return err
	}
	defer stop()
	// A managed link B has one connection that cannot be dialled again, and
	// requests that arrive together before it exists would each want one. A
	// first request makes it (Warren answers a path it does not have with
	// unsupported; the answer does not matter, the connection does).
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	if req, err := http.NewRequestWithContext(wctx, "GET", "http://warren/v1/transport/", nil); err == nil {
		if resp, err := t.linkB.Do(req); err == nil {
			resp.Body.Close()
		}
	}
	wcancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); t.refreshLoop(ctx) }()
	go func() { defer wg.Done(); t.watchClock(ctx) }()
	defer func() {
		cancel()
		wg.Wait()
		t.stopListening() // after the loop that can bind: no listener outlives run
	}()
	select {
	case <-ctx.Done():
		return nil
	case <-closed:
		return errors.New("a link to Warren closed")
	case err := <-t.fatal:
		return err
	}
}

// watchedConn reports when its connection breaks.
type watchedConn struct {
	net.Conn
	once sync.Once
	fn   func()
}

func (c *watchedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil {
		c.once.Do(c.fn)
	}
	return n, err
}

// RunManaged runs on the two connected descriptors a managed transport is
// given: a is link A (served here) and b is link B (Warren serves; this is
// its client). Neither link may close: a managed link cannot be dialled
// again, so a closed one ends the transport, and Warren restarts it.
func (t *Transport) RunManaged(ctx context.Context, a, b net.Conn) error {
	closed := make(chan struct{})
	var once sync.Once
	brk := func() { once.Do(func() { close(closed) }) }
	wa, wb := &watchedConn{Conn: a, fn: brk}, &watchedConn{Conn: b, fn: brk}
	var used bool
	var mu sync.Mutex
	dialB := func(context.Context) (net.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		if used {
			return nil, errors.New("link B has closed")
		}
		used = true
		return wb, nil
	}
	return t.run(ctx, func(h http.Handler) (<-chan struct{}, func(), error) {
		go func() { _ = link.ServeConn(wa, h) }()
		return closed, func() { a.Close(); b.Close() }, nil
	}, dialB)
}

// RunExternal runs as a transport someone else started: link A is served on
// the unix socket at linkA, link B is dialled at linkB. Only warrenUID may
// use link A, and link B must be served by warrenUID.
func (t *Transport) RunExternal(ctx context.Context, linkA, linkB string, warrenUID int) error {
	if warrenUID < 0 {
		return errors.New("the uid of Warren is required")
	}
	grp := socketdir.NewGroup(t.log)
	dialB := func(ctx context.Context) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, "unix", linkB)
		if err != nil {
			return nil, err
		}
		if uid, ok := socketdir.ConnUID(c); !ok || uid != warrenUID {
			c.Close()
			return nil, fmt.Errorf("link_b %s is not served by uid %d", linkB, warrenUID)
		}
		return c, nil
	}
	return t.run(ctx, func(h http.Handler) (<-chan struct{}, func(), error) {
		w := socketdir.Want{Key: "link_a", Path: linkA, Handler: h, H2C: true,
			Admit: func(uid int, known bool) bool { return known && uid == warrenUID }}
		if failed := grp.Sync([]socketdir.Want{w}); failed["link_a"] != nil {
			return nil, nil, fmt.Errorf("link_a: %w", failed["link_a"])
		}
		// The sockets are kept served again if the file is removed.
		stopCh := make(chan struct{})
		go func() {
			tk := time.NewTicker(3 * time.Second)
			defer tk.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-tk.C:
					grp.Sync([]socketdir.Want{w})
				}
			}
		}()
		return make(chan struct{}), func() { close(stopCh); grp.Close() }, nil
	}, dialB)
}

// ---- following tailscaled ----

func (t *Transport) refreshLoop(ctx context.Context) {
	tk := time.NewTicker(RefreshEvery)
	defer tk.Stop()
	for {
		t.refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
	}
}

// refresh asks tailscaled who this device and its peers are, publishes a
// change to the peer list, and makes the listener match the address.
func (t *Transport) refresh(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	self, devs, err := t.api.snapshot(rctx, t.tag)
	cancel()
	if ctx.Err() != nil {
		return
	}
	t.mu.Lock()
	if err != nil {
		t.fails++
		if msg := err.Error(); msg != t.lastErr {
			t.lastErr = msg
			t.log.Warn("cannot read the tailnet from tailscaled", "err", err)
		}
		// A short failure keeps the peers; a lasting one (tailscaled is gone)
		// drops them, so Warren sees them down instead of failing calls.
		if t.fails >= 3 && len(t.peers) > 0 {
			t.peers = map[string]string{}
			t.changed()
		}
		t.mu.Unlock()
		return
	}
	t.fails, t.lastErr = 0, ""
	if t.selfName != "" && t.selfName != self.Name {
		// Warren checked this name against its own at start, and every
		// request is vouched for under it. A different name now means
		// nothing Warren was told is true any more: the transport ends with
		// an error, and Warren starts it again, which makes it refuse the
		// transport loudly until the config and the device agree.
		t.log.Error("this device was renamed while running; exiting", "from", t.selfName, "to", self.Name)
		select {
		case t.fatal <- fmt.Errorf("this device was renamed from %q to %q", t.selfName, self.Name):
		default:
		}
		t.mu.Unlock()
		return
	}
	moved := t.selfIP != self.IP
	t.selfName = self.Name
	if moved {
		t.selfIP = self.IP
		// Requests must leave from our tailnet address, which is what the
		// far side checks with whois. On the host they are bound to it; in a
		// container (no such address) the host's NAT sends them from it.
		d := &net.Dialer{Timeout: DialTimeout}
		if isLocalIP(self.IP) {
			d.LocalAddr = &net.TCPAddr{IP: net.ParseIP(self.IP)}
		}
		t.dialer = d
		t.newClientLocked()
		// What was learned about callers was learned from the old address.
		t.whois = map[string]whoisEntry{}
		t.whoisGen++
	}
	peers := map[string]string{}
	for _, d := range devs {
		if d.IP == self.IP || d.Name == self.Name {
			continue // this device is never a peer
		}
		if _, dup := peers[d.Name]; !dup {
			peers[d.Name] = d.IP
		}
	}
	if !samePeers(peers, t.peers) {
		t.peers = peers
		t.changed()
	}
	// Forget cached callers whose name or tag changed, so a rename or a tag
	// added or removed counts from now rather than in a minute.
	byIP := map[string]string{}
	for name, ip := range peers {
		byIP[ip] = name
	}
	for ip, e := range t.whois {
		if name, ok := byIP[ip]; e.tagged != ok || (ok && e.node != name) {
			delete(t.whois, ip)
			t.whoisGen++
		}
	}
	select {
	case <-t.ready:
	default:
		close(t.ready)
	}
	t.mu.Unlock()
	t.ensureListening(ctx)
}

func samePeers(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// changed bumps the peer list version and wakes long polls. mu held.
func (t *Transport) changed() {
	t.ver++
	close(t.wake)
	t.wake = make(chan struct{})
}

func newPeerClient(d *net.Dialer) *http.Client {
	return &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			Proxy: nil, // a proxy from the environment would see the traffic
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				c, err := d.DialContext(ctx, network, addr)
				if err != nil {
					return nil, &dialFailure{err}
				}
				return c, nil
			},
			MaxIdleConnsPerHost: 8, MaxConnsPerHost: MaxClientConnsPerPeer, IdleConnTimeout: 90 * time.Second,
			DisableCompression: true,
		},
	}
}

// dialFailure marks an error of connecting to a peer: nothing was sent.
type dialFailure struct{ err error }

func (e *dialFailure) Error() string { return e.err.Error() }
func (e *dialFailure) Unwrap() error { return e.err }

// ---- the listener ----

// ensureListening makes the listener match the address it should have: the
// configured one, or the tailnet address on the port. It is rebound when
// that changes (a new tailnet address), and a failed bind is tried again at
// the next refresh.
func (t *Transport) ensureListening(ctx context.Context) {
	t.mu.Lock()
	addr := t.set.Listen
	if addr == "" {
		addr = net.JoinHostPort(t.selfIP, strconv.Itoa(t.port))
	}
	if addr == t.bound && t.srv != nil {
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.mu.Lock()
		if !t.listenOK {
			t.listenOK = true
			t.log.Warn("cannot listen for peers yet", "listen", addr, "err", err)
		}
		t.mu.Unlock()
		return
	}
	t.stopListening()
	srv := &http.Server{Handler: http.HandlerFunc(t.handleNetwork), ReadHeaderTimeout: min(10*time.Second, ConnIdle), IdleTimeout: ConnIdle,
		BaseContext: func(net.Listener) context.Context { return ctx }}
	t.mu.Lock()
	t.srv, t.bound, t.listenOK = srv, addr, false
	t.mu.Unlock()
	go func() { _ = srv.Serve(&limitListener{Listener: ln, t: t}) }()
	t.log.Info("listening for peers", "listen", addr)
}

func (t *Transport) stopListening() {
	t.mu.Lock()
	srv := t.srv
	t.srv, t.bound = nil, ""
	t.mu.Unlock()
	if srv != nil {
		// Close, not Shutdown: long polls in flight are meant to be cut, and
		// they are started over from the new address.
		_ = srv.Close()
	}
}

// Bound is the address being listened on, for tests.
func (t *Transport) Bound() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.bound
}

// ---- the transport API (link A) ----

// Hello implements link.Backend. It waits for tailscaled to say who this
// device is; without that there is no node name, and Warren refuses a hello
// that names none.
func (t *Transport) Hello(ctx context.Context) link.Hello {
	select {
	case <-t.ready:
	case <-ctx.Done():
		// The caller has gone (or its own deadline passed): Warren treats a
		// hello that is not answered as the link being down, which is the truth.
		// An answer with no node name would be a refusal instead.
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return link.Hello{Version: link.APIVersion, Name: "tailscale", SoftwareVersion: version.Version, Node: t.selfName, MaxBody: MaxBody}
}

// Peers implements link.Backend.
func (t *Transport) Peers(done <-chan struct{}, since string, wait time.Duration) link.PeerList {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		t.mu.Lock()
		names := make([]string, 0, len(t.peers))
		for n := range t.peers {
			names = append(names, n)
		}
		sort.Strings(names)
		l := link.PeerList{Version: strconv.Itoa(t.ver), Peers: names}
		wake := t.wake
		t.mu.Unlock()
		if l.Version != since || wait == 0 {
			return l
		}
		select {
		case <-wake:
		case <-timer.C:
			return l
		case <-done:
			return l
		}
	}
}

// Carry implements link.Backend: the request goes to the transport of node
// `to` over the tailnet, as plain HTTP to its address and port. A request
// that never got a connection is not-sent; once connected, any failure is
// lost. The deadline is Warren's, and no shorter one is imposed.
func (t *Transport) Carry(r *http.Request, to string, req *link.Request) (*link.Response, error) {
	t.mu.Lock()
	ip, hc, gctx := t.peers[to], t.hc, t.gctx
	t.mu.Unlock()
	if ip == "" || hc == nil {
		return nil, &link.CarryError{Kind: link.NotSent, Detail: "no such peer on the tailnet"}
	}
	dl := req.Deadline
	if dl.IsZero() {
		dl = time.Now().Add(DefaultDeadline)
	}
	// r's context ends when Warren resets the stream: the request is abandoned.
	ctx, cancel := context.WithDeadline(r.Context(), dl)
	defer cancel()
	// Cancelled too when the client is made again (a new address, a resume).
	defer context.AfterFunc(gctx, cancel)()
	u := "http://" + net.JoinHostPort(ip, strconv.Itoa(t.port)) + req.URI
	hreq, err := http.NewRequestWithContext(ctx, req.Method, u, bytes.NewReader(req.Body))
	if err != nil {
		return nil, &link.CarryError{Kind: link.NotSent, Detail: err.Error()}
	}
	for k, v := range link.CarryHeaders(req.Header) {
		hreq.Header[k] = v
	}
	hreq.Header.Set(link.HeaderDeadline, strconv.FormatInt(max(1, time.Until(dl).Milliseconds()), 10))
	resp, err := hc.Do(hreq)
	if err != nil {
		var df *dialFailure
		if errors.As(err, &df) {
			return nil, &link.CarryError{Kind: link.NotSent, Detail: df.err.Error()}
		}
		return nil, &link.CarryError{Kind: link.Lost, Detail: err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswer+1))
	if err != nil {
		return nil, &link.CarryError{Kind: link.Lost, Detail: err.Error()}
	}
	if len(body) > maxAnswer {
		return nil, &link.CarryError{Kind: link.Lost, Detail: fmt.Sprintf("the answer is over %d bytes", maxAnswer), TooBig: true}
	}
	// The other transport says the request never reached its Warren, or that
	// it may have.
	if kind := resp.Header.Get(link.HeaderError); kind != "" {
		tooBig := kind == link.TooBigKind
		if kind != link.NotSent && kind != link.Busy {
			kind = link.Lost
		}
		return nil, &link.CarryError{Kind: kind, Detail: detailOf(body), TooBig: tooBig}
	}
	return &link.Response{Status: resp.StatusCode, Header: link.CarryHeaders(resp.Header), Body: body}, nil
}

func detailOf(body []byte) string {
	var b struct {
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(body, &b)
	return b.Detail
}

// ---- peer traffic from the tailnet ----

// handleNetwork serves a request from another transport: the caller is
// identified by whois, this transport's own X-Warren-Transport-From is set
// from that and nothing the caller sent in X-Warren-Transport-* headers
// survives, and the request is handed to Warren on link B.
func (t *Transport) handleNetwork(w http.ResponseWriter, r *http.Request) {
	caller, ok := t.authenticate(r)
	if !ok {
		t.logRefused(r.RemoteAddr)
		answerNotSent(w, http.StatusForbidden, apierr.Refused, "not a peer")
		return
	}
	if !link.IsPeerPath(r.URL.Path) {
		answerNotSent(w, http.StatusNotFound, apierr.Unsupported, "no such endpoint: "+r.Method+" "+r.URL.Path)
		return
	}
	if r.ContentLength > MaxBody {
		answerNotSent(w, http.StatusRequestEntityTooLarge, apierr.TooLarge, "the request is over max_body")
		return
	}
	cost := int64(0)
	if r.ContentLength != 0 {
		cost = MaxBody
		if r.ContentLength > 0 {
			cost = r.ContentLength
		}
	}
	poll := isPoll(r)
	if poll && (r.ContentLength != 0 || len(r.TransferEncoding) > 0) {
		// A poll has no body, and its place costs none: a body is never read.
		answerNotSent(w, http.StatusBadRequest, apierr.BadRequest, "a hello or a list poll has no body")
		return
	}
	if !t.admit(caller, cost, poll) {
		answerBusy(w, "too many requests from the tailnet in flight")
		return
	}
	defer t.release(caller, cost, poll)
	// A peer sends its body in good time, or its place is given up.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(BodyRead))
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBody+1))

	if err != nil {
		// A body that stalls, is cut short or does not match its length is the
		// peer's mistake, not a body that is too large.
		// The read deadline stays until the answer is written: net/http tries to
		// read what is left of an unread body before it answers, and must not
		// wait for a peer that stalled.
		answerNotSent(w, http.StatusBadRequest, apierr.BadRequest, "the request body could not be read")
		return
	}
	_ = rc.SetReadDeadline(time.Time{})
	if len(body) > MaxBody {
		answerNotSent(w, http.StatusRequestEntityTooLarge, apierr.TooLarge, "the request is over max_body")
		return
	}
	ctx := r.Context()
	if ms, err := strconv.Atoi(r.Header.Get(link.HeaderDeadline)); err == nil && ms > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, askedTimeout(ms, poll))
		defer cancel()
	}
	hreq, err := http.NewRequestWithContext(ctx, r.Method, "http://warren"+r.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		answerNotSent(w, http.StatusBadRequest, apierr.BadRequest, err.Error())
		return
	}
	// Only Content-Type and X-Warren-* travel, and the hop-local X-Warren-Transport-*
	// ones are dropped before this transport sets its own.
	for k, v := range link.CarryHeaders(r.Header) {
		hreq.Header[k] = v
	}
	hreq.Header.Set(link.HeaderFrom, caller)
	resp, err := t.linkB.Do(hreq)
	if err != nil {
		if link.IsDialError(err) {
			answerNotSent(w, http.StatusBadGateway, apierr.Offline, "Warren is not reachable on its link")
			return
		}
		w.Header().Set(link.HeaderError, link.Lost)
		writeJSONStatus(w, http.StatusBadGateway, map[string]string{"error": link.Lost, "detail": "the link to Warren failed after the request was sent"})
		return
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswer+1))
	if err == nil && len(out) > maxAnswer {
		// The request was sent and answered, and the answer is over what
		// this transport carries back: told as too-big, so the other side
		// (and Warren) can tell it from a lost request.
		w.Header().Set(link.HeaderError, link.TooBigKind)
		writeJSONStatus(w, http.StatusBadGateway, map[string]string{"error": link.TooBigKind, "detail": fmt.Sprintf("Warren's answer is over %d bytes", maxAnswer)})
		return
	}
	if err != nil {
		w.Header().Set(link.HeaderError, link.Lost)
		writeJSONStatus(w, http.StatusBadGateway, map[string]string{"error": link.Lost, "detail": "Warren's answer could not be read"})
		return
	}
	for k, v := range link.CarryHeaders(resp.Header) {
		w.Header()[k] = v
	}
	// A peer that stops reading must not hold its place and the answer: the
	// write ends at the request's own (capped) deadline, or DefaultDeadline
	// from now, whichever is sooner.
	wd := time.Now().Add(DefaultDeadline)
	if d, ok := ctx.Deadline(); ok && d.Before(wd) {
		wd = d
	}
	_ = rc.SetWriteDeadline(wd)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(out)
}

// askedTimeout is how long a peer may make this transport work on its
// request: what it asked for in X-Warren-Deadline-Ms, but at most what Warren
// itself ever asks. A call may take at most 300 s, a poll at most 25 s, each
// plus a margin, so a peer cannot hold a place for as long as it likes.
func askedTimeout(ms int, poll bool) time.Duration {
	limit := 300*time.Second + callMargin
	if poll {
		limit = maxWait*time.Second + callMargin
	}
	// Compared as milliseconds first: a huge number would overflow a Duration.
	if int64(ms) >= int64(limit/time.Millisecond) {
		return limit
	}
	return time.Duration(ms) * time.Millisecond
}

// isPoll is whether r is one of Warren's bounded long polls and hellos.
func isPoll(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	i := strings.Index(r.URL.Path, "/peer/")
	if i < 0 {
		return false
	}
	switch r.URL.Path[i+len("/peer/"):] {
	case "hello", "services", "entries":
		return true
	}
	return false
}

// answerBusy refuses a request for this transport's own limits, before it is
// sent anywhere: Warren reports busy, and may try again.
func answerBusy(w http.ResponseWriter, detail string) {
	w.Header().Set(link.HeaderError, link.Busy)
	w.Header().Set(apierr.Header, apierr.Busy)
	writeJSONStatus(w, http.StatusTooManyRequests, map[string]string{"error": apierr.Busy, "detail": detail})
}

// logRefused logs a refused caller at most once per address per minute, so a
// scan cannot fill the log.
func (t *Transport) logRefused(addr string) {
	ip, _, err := net.SplitHostPort(addr)
	if err != nil {
		ip = addr
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if time.Since(t.refusedAt[ip]) < time.Minute {
		return
	}
	if len(t.refusedAt) > 1000 {
		clear(t.refusedAt)
	}
	t.refusedAt[ip] = time.Now()
	t.log.Warn("refused a caller", "addr", addr)
}

func answerNotSent(w http.ResponseWriter, status int, code, detail string) {
	w.Header().Set(link.HeaderError, link.NotSent)
	w.Header().Set(apierr.Header, code)
	writeJSONStatus(w, status, map[string]string{"error": code, "detail": detail})
}

func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// authenticate is the machine name of the calling device, if it carries the
// tag and is not this device. The answer for an address is cached for a
// minute.
func (t *Transport) authenticate(r *http.Request) (string, bool) {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "", false
	}
	t.mu.Lock()
	e, cached := t.whois[ip]
	selfIP, selfName, gen := t.selfIP, t.selfName, t.whoisGen
	t.mu.Unlock()
	// Anything else on this machine reaches us from one of our own
	// addresses. It is not a peer.
	if selfIP == "" || ip == selfIP {
		return "", false
	}
	if cached && time.Now().Before(e.until) {
		return e.node, e.tagged && e.node != "" && e.node != selfName
	}
	// One question per address at a time: a burst of requests from one device
	// waits for a single answer.
	t.mu.Lock()
	if f := t.flights[ip]; f != nil {
		t.mu.Unlock()
		select {
		case <-f.done:
		case <-r.Context().Done():
			return "", false
		}
		if f.canceled {
			return t.authenticate(r) // the leader's answer is not one to share
		}
		return f.node, f.tagged && f.node != "" && f.node != selfName
	}
	f := &whoisFlight{done: make(chan struct{})}
	t.flights[ip] = f
	t.mu.Unlock()
	// The question is not cancelled with the request that happened to ask it:
	// others wait for the answer.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer cancel()
	node, tagged, err := t.api.whois(wctx, ip, t.tag)
	canceled := errors.Is(err, context.Canceled)
	ttl := WhoisFor
	if err != nil {
		// Not a tailnet address (or tailscaled is away): refuse, and do not
		// ask again for every request.
		t.log.Debug("whois", "ip", ip, "err", err)
		node, tagged, ttl = "", false, 5*time.Second
	}
	f.node, f.tagged, f.canceled = node, tagged, canceled
	t.mu.Lock()
	delete(t.flights, ip)
	t.mu.Unlock()
	close(f.done)
	t.mu.Lock()
	if len(t.whois) > 1000 {
		clear(t.whois)
	}
	if gen == t.whoisGen && !canceled { // else something changed meanwhile: ask again next time
		t.whois[ip] = whoisEntry{node: node, tagged: tagged, until: time.Now().Add(ttl)}
	}
	t.mu.Unlock()
	return node, tagged && node != "" && node != selfName
}

// limitListener closes connections beyond the per-address and overall caps
// as they arrive.
type limitListener struct {
	net.Listener
	t *Transport
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		ip, _, _ := net.SplitHostPort(c.RemoteAddr().String())
		t := l.t
		t.mu.Lock()
		over := t.conns >= t.lim.conns || t.connsBy[ip] >= t.lim.connsPerIP
		if !over {
			t.conns++
			t.connsBy[ip]++
		}
		t.mu.Unlock()
		if over {
			c.Close()
			continue
		}
		return &countedConn{Conn: c, t: t, ip: ip}, nil
	}
}

type countedConn struct {
	net.Conn
	t    *Transport
	ip   string
	once sync.Once
}

func (c *countedConn) Close() error {
	c.once.Do(func() {
		c.t.mu.Lock()
		c.t.conns--
		if c.t.connsBy[c.ip]--; c.t.connsBy[c.ip] <= 0 {
			delete(c.t.connsBy, c.ip)
		}
		c.t.mu.Unlock()
	})
	return c.Conn.Close()
}

// whoisFlight is one whois question in flight for an address, shared by
// every request from it meanwhile.
type whoisFlight struct {
	done   chan struct{}
	node   string
	tagged bool
	// canceled is a result that must not be shared or kept.
	canceled bool
}
