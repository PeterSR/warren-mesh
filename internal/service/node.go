// Package service is the whole /v1/service API of one node: identity and
// status, leases and calls, entries and the merged view, precedence and the
// persisted names, and the limits that keep one child from starving the
// rest. It serves sockets through socketdir and reaches other nodes only
// through a link.Link.
//
// All state is guarded by one mutex (Node.mu). Handlers take it for short
// steps and never hold it while reading a body, waiting for a poll or
// writing to a client.
package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/config"
	"github.com/PeterSR/warren-mesh/internal/grammar"
	"github.com/PeterSR/warren-mesh/internal/link"
	"github.com/PeterSR/warren-mesh/internal/socketdir"
	"github.com/PeterSR/warren-mesh/internal/version"
)

// What this Warren reports about itself from the start.
const (
	Version = version.Version
)

// PeerProtocols and Features are what status announces.
var (
	PeerProtocols = []int{1}
	Features      = []string{}
)

// Timing and limits. Variables, so tests can shorten them.
var (
	// ScanEvery is how often attach.dir is scanned for new child directories.
	ScanEvery = 3 * time.Second
	// LeaseGrace is how long a lease outlives its last poll or handed call.
	LeaseGrace = 20 * time.Second
	// StaleFor is how long entries of a gone service stay, marked stale.
	StaleFor = 60 * time.Second
	// BootHold is how long after boot a flat child's PUT may be held for the
	// node's own services to publish first.
	BootHold = 10 * time.Second
	// ServeInterval is the least time between leases granted on one socket.
	ServeInterval = 1 * time.Second
	// PutInterval is the least time between PUTs on one socket.
	PutInterval = 1 * time.Second
	// DrainWait is how long shutdown waits for handed-out calls to be answered.
	DrainWait = 5 * time.Second
	// WaitUnit is the unit of the wait parameter (whole seconds).
	WaitUnit = time.Second
	// BodyReadTimeout is the read deadline for a body on a service socket.
	BodyReadTimeout = 30 * time.Second
	// ResponseWriteTimeout bounds writing one response on a service socket,
	// but for the answer to a call, which is bounded by the call's deadline.
	ResponseWriteTimeout = 30 * time.Second
	// CallWriteGrace is how long after its deadline the answer to a call may
	// still be written, so that one accepted just before the deadline is
	// delivered.
	CallWriteGrace = 2 * time.Second
)

// Other fixed limits (docs/architecture.md, "Limits and fairness").
const (
	MaxCallBody      = grammar.MaxCallBody
	MaxPutBody       = 4 << 20
	MaxEntryData     = 3 << 10
	MaxPutEntries    = 1000
	MaxPutReserved   = 64
	MaxViewPolls     = 4
	MaxListEntries   = 1000
	DefaultTimeoutMs = 30000
	MaxTimeoutMs     = 300000
	maxWait          = 25
	callMemory       = 2 << 20 // a request and a response
	namesFileVersion = 1
)

type sockKey struct{ relay, service string }

func (k sockKey) String() string { return k.relay + "/" + k.service }

// Options start a Node.
type Options struct {
	Config *config.Config
	// Link reaches other nodes; nil means this node is on its own.
	Link          link.Link
	Log           *slog.Logger
	TransportName string
	// PeerProtocols are the peer protocols this node announces and serves; the
	// default is PeerProtocols. PeerHandlers gives the handler of a version
	// this build has no implementation of (tests of the version window).
	PeerProtocols []int
	PeerHandlers  map[int]http.Handler
	// Clock gives the wall clock and the monotonic clock as durations from a
	// fixed start; tests use it to make the wall clock jump.
	Clock func() (wall, mono time.Duration)
}

// Node is one Warren.
type Node struct {
	log  *slog.Logger
	name string
	boot string
	link link.Link
	// recMu serializes reconciles; mu guards everything below.
	recMu sync.Mutex
	mu    sync.Mutex

	cfg              *config.Config
	bootAt           time.Time
	socks            map[sockKey]*sock
	children         map[string]*child
	svcs             map[string]*svcState
	group            *socketdir.Group
	peers            []string
	linkUp           bool // the transport link works (true when there is none)
	transport        string
	transportVersion string
	draining         bool
	closed           bool
	stopCh           chan struct{}
	started          bool
	warned           map[string]bool
	timers           []*time.Timer
	persisted        map[string]persistedNames
	// epochs count the leases each socket has had this boot. They live in the
	// node, not the socket, so they survive the socket being removed and
	// made again by a reload.
	epochs map[sockKey]int
	// names.json is written outside mu: snapshots are numbered under mu and
	// an older one never overwrites a newer one.
	namesLast []byte // the bytes last queued, or found on disk at start
	// namesFailed is set when the last write of names.json failed; the scan
	// loop then queues the snapshot again, so a disk that was full for a
	// moment does not leave the file stale until the next PUT.
	namesFailed bool
	namesWarned bool // the failure has been logged; cleared by a good write
	namesSeq    int
	writeMu     sync.Mutex
	written     int // guarded by writeMu
	maxList     int // the cut of the node's list; tests shorten it
	wg          sync.WaitGroup

	// The peer protocol.
	ctx          context.Context // ends with the node; set by Start
	protocols    []int           // announced, ascending
	peerHandlers map[int]http.Handler
	peerStates   map[string]*peerState
	pollSlots    map[string]int // open long polls from peers, per peer and list
	linkCancel   context.CancelFunc
	clock        func() (wall, mono time.Duration)
	svcNames     []string // the services peers are told of
	helloPassed  bool     // a hello has passed: the link is for peers
	svcsSeen     []string // the services the peer polls were last matched to
	svcsVer      int
	svcsNotify   chan struct{}
	svcsTimer    *time.Timer
}

// child is a child directory that is being served.
type child struct {
	label    string
	flat     bool
	template bool
	cfg      *config.Child // nil for template children
	services []config.Service
	max      int // max_entries
	order    int // position in config order, or -1 for template
}

// New makes a Node; call Start to serve.
func New(o Options) (*Node, error) {
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	n := &Node{
		log: log, name: o.Config.Node, boot: grammar.NewID(), link: o.Link, cfg: o.Config,
		socks: map[sockKey]*sock{}, epochs: map[sockKey]int{}, children: map[string]*child{}, svcs: map[string]*svcState{},
		linkUp: o.Link == nil, stopCh: make(chan struct{}), warned: map[string]bool{}, transport: o.TransportName,
		group: socketdir.NewGroup(log), bootAt: time.Now(), maxList: MaxListEntries,
		peerStates: map[string]*peerState{}, pollSlots: map[string]int{}, svcsNotify: make(chan struct{}), clock: systemClock(),
	}
	n.protocols = append([]int(nil), o.PeerProtocols...)
	if len(n.protocols) == 0 {
		n.protocols = append([]int(nil), PeerProtocols...)
	}
	slices.Sort(n.protocols)
	n.peerHandlers = map[int]http.Handler{1: http.HandlerFunc(n.servePeerV1)}
	for p := range n.peerHandlers {
		if !slices.Contains(n.protocols, p) {
			delete(n.peerHandlers, p) // a version this node does not announce is not served
		}
	}
	for p, h := range o.PeerHandlers {
		n.peerHandlers[p] = h
	}
	if o.Clock != nil {
		n.clock = o.Clock
	}
	// The file is read only from a directory that is safe to read it from.
	if _, err := os.Stat(o.Config.StateDir); err == nil {
		if err := n.checkStateDir(o.Config.StateDir); err != nil {
			return nil, err
		}
	}
	n.loadNames()
	return n, nil
}

// Boot is the id that changes on every start.
func (n *Node) Boot() string { return n.boot }

// Name is the node's name.
func (n *Node) Name() string { return n.name }

// Start makes the node's directories, serves its sockets and begins
// scanning for children.
func (n *Node) Start() error {
	n.mu.Lock()
	cfg := n.cfg
	n.mu.Unlock()
	if len(cfg.Services.List) > 0 {
		// Closed to other users, unless some node service names a uid: that
		// user has to pass through the directory (0711, no listing) to reach
		// its socket. The sockets of the services without a uid stay closed
		// to others either way (see serviceSocketMode).
		mode := os.FileMode(0o700)
		if anyNodeUID(cfg) {
			mode = 0o711
		}
		if err := os.MkdirAll(cfg.Services.Dir, mode); err != nil {
			return fmt.Errorf("services.dir: %w", err)
		}
	}
	if cfg.Attach.Dir != "" {
		if err := os.MkdirAll(cfg.Attach.Dir, 0o755); err != nil {
			return fmt.Errorf("attach.dir: %w", err)
		}
	}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return fmt.Errorf("state_dir: %w", err)
	}
	if err := n.checkStateDir(cfg.StateDir); err != nil {
		return err
	}
	removeLeftoverTemps(cfg.StateDir)
	n.log.Info("memory bound", "bytes", MemoryBound(cfg))
	n.Reconcile()
	n.mu.Lock()
	n.started = true
	n.mu.Unlock()
	if n.link != nil {
		n.mu.Lock()
		n.ctx = stopContext(n.stopCh)
		n.publishServices()
		n.mu.Unlock()
		n.link.Handle(n.peerHandler())
		n.wg.Add(2)
		go n.watchPeers()
		go n.watchClock()
	}
	n.wg.Add(1)
	go n.scanLoop()
	// When the boot hold ends, held PUTs and ready flags need a push.
	t := time.AfterFunc(BootHold, func() { n.mu.Lock(); n.rebuildAll(); n.mu.Unlock() })
	n.mu.Lock()
	n.timers = append(n.timers, t)
	n.mu.Unlock()
	return nil
}

func (n *Node) scanLoop() {
	defer n.wg.Done()
	t := time.NewTicker(ScanEvery)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.Reconcile()
			n.mu.Lock()
			if n.namesFailed {
				n.namesFailed = false
				n.saveNames()
			}
			n.mu.Unlock()
			if r, ok := n.link.(interface{ Resync() }); ok {
				r.Resync() // an external link's socket may have been removed
			}
		}
	}
}

// watchPeers keeps the peer list from the transport. When the link fails or
// closes, every peer is marked down and the link is tried again with
// backoff (a managed transport is restarted by its supervisor, an external
// one is dialled again by the client). A transport that comes back is
// checked with hello again, since it may be another one.
func (n *Node) watchPeers() {
	defer n.wg.Done()
	ctx := stopContext(n.stopCh)
	const minBackoff, maxBackoff = 500 * time.Millisecond, 10 * time.Second
	backoff := minBackoff
	since := ""
	helloOK := false
	lastHelloErr := ""
	down := func(why string, err error) {
		n.mu.Lock()
		was := n.linkUp
		n.peers, n.linkUp = nil, false
		if why == "hello" {
			n.helloPassed = false
		}
		n.syncPeers() // every peer is marked down
		n.mu.Unlock()
		if was {
			n.log.Warn("transport link is down; every peer is marked down", "why", why, "err", err)
		}
		select {
		case <-n.stopCh:
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	for ctx.Err() == nil {
		if !helloOK {
			hctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			h, err := n.link.Hello(hctx)
			cancel()
			if err == nil {
				err = link.CheckHello(h, n.name)
			}
			if err != nil {
				// A refused hello is logged on its own: down() only speaks
				// when the link was up, which it is not after a restart.
				if msg := err.Error(); msg != lastHelloErr {
					lastHelloErr = msg
					n.log.Warn("transport hello refused or failed", "err", err)
				}
				down("hello", err)
				continue
			}
			lastHelloErr = ""
			n.mu.Lock()
			n.helloPassed = true
			if h.Name != "" {
				n.transport = h.Name
			}
			n.transportVersion = h.SoftwareVersion
			n.mu.Unlock()
			helloOK = true
			since = ""
		}
		pctx, pcancel := context.WithCancel(ctx)
		n.mu.Lock()
		n.linkCancel = pcancel
		n.mu.Unlock()
		pl, err := n.link.Peers(pctx, since, maxWait*time.Second)
		restarted := pctx.Err() != nil // restartPolls cancelled this poll
		pcancel()
		if ctx.Err() != nil {
			return
		}
		if restarted {
			since = ""
			continue
		}
		if err != nil {
			helloOK = false
			down("peers", err)
			continue
		}
		backoff = minBackoff
		since = pl.Version
		n.mu.Lock()
		n.peers = append([]string(nil), pl.Peers...)
		n.linkUp = true
		n.syncPeers()
		n.mu.Unlock()
	}
}

// Drain stops handing out calls, fails calls still waiting for a poll with
// not-ready, and waits up to DrainWait for handed-out calls to be answered.
func (n *Node) Drain() {
	n.mu.Lock()
	n.draining = true
	for _, s := range n.socks {
		if s.lease != nil {
			for _, c := range append([]*call(nil), s.lease.held...) {
				n.finish(c, result{err: apierr.New(apierr.NotReady, "Warren is shutting down")})
			}
		}
		// Polls waiting for a call are told so now.
		n.wakeWaiters(s)
	}
	for _, svc := range n.svcs {
		n.notify(svc)
		close(svc.peerNotify) // peers' long polls are told too
		svc.peerNotify = make(chan struct{})
	}
	close(n.svcsNotify)
	n.svcsNotify = make(chan struct{})
	n.mu.Unlock()
	deadline := time.Now().Add(DrainWait)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		busy := false
		for _, s := range n.socks {
			if s.lease != nil && len(s.lease.handed) > 0 {
				busy = true
			}
		}
		n.mu.Unlock()
		if !busy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Close ends every lease, stops serving and stops the node's goroutines.
func (n *Node) Close() {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.closed = true
	n.draining = true
	close(n.stopCh)
	for _, t := range n.timers {
		t.Stop()
	}
	for _, s := range n.socks {
		if s.lease != nil {
			n.endLease(s, "shutdown")
		}
		if s.staleTimer != nil {
			s.staleTimer.Stop()
		}
	}
	for _, svc := range n.svcs {
		n.notify(svc)
		if svc.peerTimer != nil {
			svc.peerTimer.Stop()
		}
	}
	if n.svcsTimer != nil {
		n.svcsTimer.Stop()
	}
	for _, ps := range n.peerStates {
		if ps.timer != nil {
			ps.timer.Stop()
		}
		if ps.stateTimer != nil {
			ps.stateTimer.Stop()
		}
	}
	n.mu.Unlock()
	n.group.Close()
	n.wg.Wait()
}

// ProcessExited ends the lease of the service at relay at once: its managed
// process is gone, so waiting out the grace would only keep callers waiting.
func (n *Node) ProcessExited(relay, service string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if s := n.socks[sockKey{relay, service}]; s != nil && s.lease != nil {
		n.endLease(s, "process exited")
	}
}

// LeaseInfo describes the lease of one of this node's own services: whether
// one is held now, when the current one began, and how many leases the socket
// has had since Warren started (the epoch, which only grows within a boot, so
// a poller can tell a new lease from the old one).
func (n *Node) LeaseInfo(service string) (held bool, since time.Time, epoch int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	k := sockKey{n.name, service}
	if s := n.socks[k]; s != nil && s.lease != nil && !s.lease.ended {
		return true, s.lease.since, n.epochs[k]
	}
	return false, time.Time{}, n.epochs[k]
}

// ServicePath is the socket a managed node service is given.
func (n *Node) ServicePath(service string) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.cfg.ServicePath(service)
}

// Config returns the current configuration.
func (n *Node) Config() *config.Config {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.cfg
}

// Reload applies a new configuration without a restart or a new boot.
func (n *Node) Reload(next *config.Config) error {
	n.recMu.Lock()
	n.mu.Lock()
	if err := config.CheckReload(n.cfg, next); err != nil {
		n.mu.Unlock()
		n.recMu.Unlock()
		return err
	}
	n.cfg = next
	n.mu.Unlock()
	n.recMu.Unlock()
	n.Reconcile()
	// What a child may publish, and the order that decides precedence, come
	// from the config: every view is worked out again.
	n.mu.Lock()
	n.rebuildAll()
	n.scheduleServices()
	n.mu.Unlock()
	return nil
}

// warnOnce logs a warning once per key until clearWarn. mu held.
func (n *Node) warnOnce(key, msg string, args ...any) {
	if n.warned[key] {
		return
	}
	if len(n.warned) > 1000 { // keys come from names others chose
		clear(n.warned)
	}
	n.warned[key] = true
	n.log.Warn(msg, args...)
}

// StreamNeed is how many HTTP/2 streams on a link Warren's config can use at
// once: every inflight and outflight place may be a call carried to or from
// a peer, plus a few long polls per peer. The links allow link.MinStreams.
func StreamNeed(cfg *config.Config) int {
	need := 16 // the transport's peer list poll, hello and a few more
	// Per peer: this node long-polls a list of services and one list of
	// entries per service (link A), and the peer does the same to this node,
	// up to MaxListPolls each (link B). Peers are not known in advance, so
	// room for AssumedPeers of them is kept.
	distinct := map[string]bool{}
	add := func(s config.Service, copies int) {
		need += copies * (s.Inflight + s.Outflight)
		distinct[s.Name] = true
	}
	for _, s := range cfg.Services.List {
		add(s, 1)
	}
	for _, ch := range cfg.Attach.Children {
		for _, s := range ch.Services {
			add(s, 1)
		}
	}
	if d := cfg.Attach.Default; d != nil {
		templates := cfg.Attach.MaxChildren - len(cfg.Attach.Children)
		if templates < 0 {
			templates = 0
		}
		for _, s := range d.Services {
			add(s, templates)
		}
	}
	return need + AssumedPeers*(1+len(distinct))*(1+MaxListPolls)
}

// AssumedPeers is how many peers the stream check and the memory bound of the
// config leave room for. A mesh with more peers has a higher real bound;
// `warren status` says which number the bound assumes.
const AssumedPeers = 10

// MaxListRead is the most Warren reads of one carried answer before it
// judges it too big (the link's own limit), so a list poll in flight holds up
// to this much, plus its decoded copy.
const MaxListRead = 8 << 20

// MemoryBound is the most memory the configured limits allow in flight:
// every inflight and outflight place holds a request and a response, every
// PUT 4 MiB, each socket keeps its stored entries (twice over, while a new
// lease replaces them), and each service caches a serialized view, a list for
// peers and the lists of the peers (room for AssumedPeers of them).
func MemoryBound(cfg *config.Config) int64 {
	var total int64
	perSock := func(s config.Service) int64 {
		// Calls in flight, a PUT being read, and the stored entries: the
		// most a socket may hold, and a stale copy while a new lease replaces them.
		return int64(s.Inflight+s.Outflight)*callMemory + MaxPutBody + 2*MaxPutEntries*MaxEntryData +
			socketdir.MaxConns*(socketdir.MaxHeaderBytes+(8<<10)) // header plus buffers per connection
	}
	views := map[string]bool{}
	for _, s := range cfg.Services.List {
		total += perSock(s)
		views[s.Name] = true
	}
	for _, ch := range cfg.Attach.Children {
		for _, s := range ch.Services {
			total += perSock(s)
			views[s.Name] = true
		}
	}
	if d := cfg.Attach.Default; d != nil {
		templates := cfg.Attach.MaxChildren - len(cfg.Attach.Children)
		if templates < 0 {
			templates = 0
		}
		for _, s := range d.Services {
			total += int64(templates) * perSock(s)
			views[s.Name] = true
		}
	}
	// Serialized views, one per service, and the lists of the peers: each
	// service keeps its own list for peers (a snapshot) and one list per peer.
	total += int64(len(views)) * MaxPutBody * int64(2+AssumedPeers)
	// A list poll of a peer being answered: the raw answer, read up to what
	// a transport carries (MaxListRead), and its decoded copy, for each
	// service and each peer.
	total += int64(len(views)) * int64(AssumedPeers) * 2 * MaxListRead
	return total
}

// ---- persisted names ----

type persistedNames struct {
	Names    []string `json:"names"`
	Reserved []string `json:"reserved"`
}

type namesFile struct {
	Version  int                       `json:"version"`
	Services map[string]persistedNames `json:"services"`
}

func (n *Node) namesPath() string { return filepath.Join(n.cfg.StateDir, "names.json") }

// loadNames reads names.json. A file in another format is discarded, with a
// log line: the cost is the precedence protection right after this boot.
func (n *Node) loadNames() {
	n.persisted = map[string]persistedNames{}
	data, err := os.ReadFile(n.namesPath())
	if err != nil {
		if !os.IsNotExist(err) {
			n.log.Warn("cannot read names.json; starting without it", "err", err)
		}
		return
	}
	n.namesLast = data
	var f namesFile
	if err := json.Unmarshal(data, &f); err != nil || f.Version != namesFileVersion {
		n.log.Warn("names.json is in another format; discarding it", "err", err)
		return
	}
	for svc, p := range f.Services {
		var ok persistedNames
		for _, nm := range p.Names {
			if grammar.EntryName(nm) {
				ok.Names = append(ok.Names, nm)
			}
		}
		for _, nm := range p.Reserved {
			if grammar.EntryName(nm) {
				ok.Reserved = append(ok.Reserved, nm)
			}
		}
		n.persisted[svc] = ok
	}
}

// saveNames queues a write of names.json; the file is replaced atomically
// and durably, outside mu. mu held.
func (n *Node) saveNames() {
	f := namesFile{Version: namesFileVersion, Services: map[string]persistedNames{}}
	for k, s := range n.socks {
		if s.child == nil && (len(s.heldNames) > 0 || len(s.heldReserved) > 0) {
			f.Services[k.service] = persistedNames{Names: s.heldNames, Reserved: s.heldReserved}
		}
	}
	// What is persisted is what the file says, never a stale copy from boot:
	// a socket made again for a service starts from it.
	n.persisted = maps.Clone(f.Services)
	data, _ := json.Marshal(f)
	if n.closed || bytes.Equal(data, n.namesLast) {
		return
	}
	n.namesLast = data
	n.namesSeq++
	seq, p := n.namesSeq, n.namesPath()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.writeMu.Lock()
		defer n.writeMu.Unlock()
		if seq > n.written {
			if err := writeDurably(p, data); err != nil {
				// Said once until a write succeeds, though the scan loop tries
				// again every few seconds.
				n.mu.Lock()
				if !n.namesWarned {
					n.namesWarned = true
					n.log.Warn("cannot write names.json; trying again until it works", "err", err)
				}
				// Forget what was queued, so the next snapshot is not skipped
				// as unchanged and the write is tried again.
				if n.namesSeq == seq {
					n.namesLast = nil
					n.namesFailed = true
				}
				n.mu.Unlock()
			} else {
				n.mu.Lock()
				n.namesWarned = false
				n.mu.Unlock()
			}
			n.written = seq
		}
	}()
}

// flushNames waits until every queued names.json snapshot is written (tests).
func (n *Node) flushNames() {
	n.mu.Lock()
	seq := n.namesSeq
	n.mu.Unlock()
	for {
		n.writeMu.Lock()
		done := n.written >= seq
		n.writeMu.Unlock()
		if done {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// writeDurably replaces the file at p with data so that after a crash it is
// the old content or the new, never less: the temporary file is synced
// before the rename, and the directory after it.
func writeDurably(p string, data []byte) (err error) {
	// The temporary file is made exclusively, with a name nobody can guess, so
	// a link or a file put there beforehand is never followed or written.
	f, err := os.CreateTemp(filepath.Dir(p), filepath.Base(p)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(p))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// CheckStateDir looks at state_dir: one that group or others may write to is
// refused (they could replace what Warren keeps in it). owner is the uid of a
// directory that is not owned by Warren's user (to be reported), else -1.
func CheckStateDir(dir string) (owner int, err error) {
	fi, err := os.Stat(dir)
	if err != nil {
		return -1, fmt.Errorf("state_dir: %w", err)
	}
	if !fi.IsDir() {
		return -1, fmt.Errorf("state_dir %s is not a directory", dir)
	}
	if fi.Mode().Perm()&0o022 != 0 {
		return -1, fmt.Errorf("state_dir %s is writable by its group or by others (mode %04o); make it 0700", dir, fi.Mode().Perm())
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != socketdir.Euid() {
		return int(st.Uid), nil
	}
	return -1, nil
}

// checkStateDir is CheckStateDir that reports a foreign owner in the log.
func (n *Node) checkStateDir(dir string) error {
	owner, err := CheckStateDir(dir)
	if err != nil {
		return err
	}
	if owner >= 0 {
		n.mu.Lock()
		n.warnOnce("state-dir-owner", "state_dir is not owned by Warren's user", "dir", dir, "owner", owner)
		n.mu.Unlock()
	}
	return nil
}

// removeLeftoverTemps removes the temporary files of names.json that a Warren
// that died while writing left in state_dir.
func removeLeftoverTemps(dir string) {
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "names.json.tmp") && !e.IsDir() {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

// sortedSocks returns the sockets in a stable order. mu held.
func (n *Node) sortedSocks() []*sock {
	out := make([]*sock, 0, len(n.socks))
	for _, s := range n.socks {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].relay != out[j].relay {
			return out[i].relay < out[j].relay
		}
		return out[i].service < out[j].service
	})
	return out
}
