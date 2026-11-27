package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/config"
	"github.com/PeterSR/warren-mesh/internal/grammar"
	"github.com/PeterSR/warren-mesh/internal/socketdir"
)

func stopContext(ch <-chan struct{}) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-ch; cancel() }()
	return ctx
}

// sock is the state of one service socket: service S at one relay.
type sock struct {
	n       *Node
	relay   string
	service string
	child   *child // nil on the node itself
	cfg     config.Service
	uid     *int
	path    string

	lease   *lease
	endedAt time.Time // when the last lease ended; entries go stale for good StaleFor later

	// entries are the last PUT's, in order. They stay in memory across leases
	// of the same boot; the view hides them once they are stale for good.
	entries []entry
	// entriesFrom is the lease whose PUT made entries. They are live only
	// while that same lease lives; a new lease starts with no published list
	// of its own, so they are stale until it PUTs (within the stale window).
	entriesFrom  *lease
	published    bool // a PUT arrived this boot
	heldNames    []string
	heldReserved []string // node services only
	lastPut      time.Time
	lastServe    time.Time
	staleTimer   *time.Timer // drops stale entries; one per socket
	putBusy      bool

	viewPolls int
	outflight int
	// readers are answer bodies still being read for calls that have ended;
	// each holds a buffer, so each counts against inflight.
	readers int
}

// entry is one published thing.
type entry struct {
	Name      string          `json:"name"`
	Namespace string          `json:"namespace"`
	Data      json.RawMessage `json:"data"`
}

func (s *sock) isNode() bool { return s.child == nil }
func (s *sock) flat() bool   { return s.child != nil && s.child.flat }

// entryNS is the namespace the socket's entries take by default.
func (s *sock) entryNS() string {
	if s.child != nil && !s.child.flat {
		return grammar.Child(s.child.label, s.n.name)
	}
	return s.n.name
}

// namespaces are the full service namespaces besides entryNS.
func (s *sock) namespaces() []string {
	var out []string
	for _, l := range s.cfg.Namespaces {
		out = append(out, grammar.Child(l, s.n.name))
	}
	return out
}

// mayPublishIn reports whether ns is this socket's entry namespace or one
// of its service namespaces.
func (s *sock) mayPublishIn(ns string) bool {
	if ns == s.entryNS() {
		return true
	}
	return slices.Contains(s.namespaces(), ns)
}

// ---- reconciling sockets with config and directories ----

// Reconcile makes the served sockets match the configuration and the child
// directories on disk now. It runs every ScanEvery and after a reload.
func (n *Node) Reconcile() {
	n.recMu.Lock()
	defer n.recMu.Unlock()

	n.mu.Lock()
	cfg := n.cfg
	n.mu.Unlock()

	// Read the disk without the lock.
	var dirs []string
	if cfg.Attach.Dir != "" {
		var err error
		dirs, err = socketdir.ListDirs(cfg.Attach.Dir)
		if err != nil {
			n.mu.Lock()
			n.warnOnce("attach-dir", "cannot read attach.dir", "dir", cfg.Attach.Dir, "err", err)
			n.mu.Unlock()
		}
		for _, w := range socketdir.UpperWarnings(cfg.Attach.Dir, anyUID(cfg)) {
			n.mu.Lock()
			n.warnOnce("attach-dir|"+w, "attach.dir: "+w, "dir", cfg.Attach.Dir)
			n.mu.Unlock()
		}
	}
	n.openServicesDir(cfg)
	if len(cfg.Services.List) > 0 {
		// A services.dir that already existed keeps the permissions it had.
		// Others entering it is no warning here: the node's sockets without a
		// uid are 0600 then (serviceSocketMode), and the others check the uid.
		for _, w := range socketdir.UpperWarnings(cfg.Services.Dir, true) {
			n.mu.Lock()
			n.warnOnce("services-dir|"+w, "services.dir: "+w, "dir", cfg.Services.Dir)
			n.mu.Unlock()
		}
	}
	dirSet := map[string]bool{}
	for _, d := range dirs {
		dirSet[d] = true
	}

	n.mu.Lock()
	// Which children are served now.
	want := map[string]*child{}
	for i := range cfg.Attach.Children {
		cc := &cfg.Attach.Children[i]
		if !dirSet[cc.Name] {
			n.warnOnce("missing|"+cc.Name, "configured child has no directory yet", "child", cc.Name, "dir", filepath.Join(cfg.Attach.Dir, cc.Name))
			continue
		}
		delete(n.warned, "missing|"+cc.Name)
		want[cc.Name] = &child{label: cc.Name, flat: cc.Flat, cfg: cc, services: cc.Services, max: cc.MaxEntries, order: i}
	}
	if d := cfg.Attach.Default; d != nil {
		var candidates []string
		for _, name := range dirs {
			if c, _ := cfg.Child(name); c != nil {
				continue
			}
			switch {
			case !grammar.Label(name) || !grammar.RelayID(grammar.Child(name, n.name)):
				n.warnOnce("dir|"+name, "directory is not a usable child label (lower case letters, digits, dash, underscore; <label>."+n.name+" at most 64 bytes); skipped", "dir", name)
			case cfg.NamespaceTaken(name):
				n.warnOnce("dir|"+name, "directory is named like a service namespace of this node; it gets nothing", "dir", name)
			default:
				candidates = append(candidates, name)
			}
		}
		sort.Strings(candidates)
		// Children already served keep their place; new ones fill what is left.
		room := cfg.Attach.MaxChildren - len(want)
		var fresh []string
		for _, name := range candidates {
			if old := n.children[name]; old != nil && old.template {
				if room > 0 {
					want[name] = &child{label: name, template: true, services: d.Services, max: d.MaxEntries, order: -1}
					room--
				}
				continue
			}
			fresh = append(fresh, name)
		}
		for _, name := range fresh {
			if room <= 0 {
				n.warnOnce("max|"+name, "attach.max_children reached; this directory gets nothing", "dir", name, "max_children", cfg.Attach.MaxChildren)
				continue
			}
			want[name] = &child{label: name, template: true, services: d.Services, max: d.MaxEntries, order: -1}
			room--
		}
	}

	// The sockets that should exist.
	type wantSock struct {
		ch  *child
		svc config.Service
	}
	desired := map[sockKey]wantSock{}
	for _, s := range cfg.Services.List {
		desired[sockKey{n.name, s.Name}] = wantSock{nil, s}
	}
	for label, ch := range want {
		for _, s := range ch.services {
			if !cfg.ChildSocketPathFits(label, s.Name) {
				n.warnOnce("sun|"+label+"|"+s.Name, "socket path is longer than a unix socket allows; skipped", "child", label, "service", s.Name)
				continue
			}
			desired[sockKey{grammar.Child(label, n.name), s.Name}] = wantSock{ch, s}
		}
	}

	changed := map[string]bool{}
	// Drop sockets that are no longer wanted, or whose child is now of
	// another kind (a changed flat flag is a removal and an addition).
	for k, s := range n.socks {
		w, ok := desired[k]
		keep := ok && (w.ch == nil) == (s.child == nil)
		if keep && w.ch != nil {
			keep = w.ch.flat == s.child.flat && w.ch.template == s.child.template
		}
		if keep {
			// Who the socket admits changed: the holder of the old lease may
			// not be who is admitted now, so the lease ends (its polls are told
			// gone) and the socket starts afresh.
			wu := w.svc.UID
			if w.ch != nil {
				wu = childUID(w.ch, w.svc)
			}
			keep = sameUID(s.uid, wu)
		}
		if keep {
			continue
		}
		n.log.Info("service socket removed", "relay", k.relay, "service", k.service)
		n.dropSock(s)
		changed[k.service] = true
	}
	for label := range n.children {
		if want[label] == nil {
			n.log.Info("child directory gone or no longer served", "child", label)
			delete(n.children, label)
		}
	}
	maxChanged := map[string]bool{}
	for label, ch := range want {
		if old := n.children[label]; old == nil || old.flat != ch.flat || old.template != ch.template {
			n.children[label] = ch
			if old == nil {
				n.log.Info("serving a child", "child", label, "flat", ch.flat, "template", ch.template)
			}
		} else {
			// Same kind: keep the identity, refresh what config may change.
			if old.max != ch.max {
				maxChanged[label] = true
			}
			old.cfg, old.services, old.max, old.order = ch.cfg, ch.services, ch.max, ch.order
		}
	}
	for k, w := range desired {
		s := n.socks[k]
		isNew := s == nil
		if s == nil {
			s = &sock{n: n, relay: k.relay, service: k.service}
			if w.ch == nil {
				s.heldNames = append([]string(nil), n.persisted[k.service].Names...)
				s.heldReserved = append([]string(nil), n.persisted[k.service].Reserved...)
			}
			n.socks[k] = s
		}
		if w.ch != nil {
			s.child = n.children[w.ch.label]
			s.path = cfg.ChildPath(w.ch.label, k.service)
			s.uid = childUID(w.ch, w.svc)
		} else {
			s.path = cfg.ServicePath(k.service)
			s.uid = w.svc.UID
		}
		if isNew || !sameLimits(s.cfg, w.svc) || (w.ch != nil && maxChanged[w.ch.label]) {
			changed[k.service] = true
		}
		s.cfg = w.svc
	}
	for _, ch := range n.children {
		for _, w := range socketdir.DirWarnings(filepath.Join(cfg.Attach.Dir, ch.label)) {
			n.warnOnce("perm|"+ch.label+"|"+w, "child directory: "+w, "child", ch.label)
		}
	}
	for svc := range changed {
		n.rebuild(n.svc(svc))
	}
	if len(changed) > 0 {
		n.saveNames()
	}
	n.scheduleServices()
	if n.closed {
		n.mu.Unlock()
		return
	}
	var wants []socketdir.Want
	for _, s := range n.sortedSocks() {
		s := s
		w := socketdir.Want{
			Key: s.key().String(), Path: s.path, UID: s.uid, Handler: s, Refuse: refuseUID,
			Mode: serviceSocketMode(cfg, s),
		}
		if s.child == nil && s.uid == nil {
			// Only Warren's own user, whatever the file mode says: a mode is
			// checked when a connection is made, so one made (or queued) before
			// a reload removed a uid would otherwise keep its access. This is
			// checked at accept and on every request.
			w.Admit = func(uid int, known bool) bool { return known && uid == socketdir.Euid() }
		}
		wants = append(wants, w)
	}
	n.mu.Unlock()

	// Listening can be slow, and its handlers take mu, so it is done unlocked.
	n.group.Sync(wants)
}

func (s *sock) key() sockKey { return sockKey{s.relay, s.service} }

// sameLimits is whether nothing that shapes the view or the limits differs.
func sameLimits(a, b config.Service) bool {
	return a.Inflight == b.Inflight && a.PerRoot == b.PerRoot && a.Outflight == b.Outflight &&
		slices.Equal(a.Allow, b.Allow) && slices.Equal(a.Namespaces, b.Namespaces)
}

func sameUID(a, b *int) bool { return (a == nil) == (b == nil) && (a == nil || *a == *b) }

// anyNodeUID is whether some service on the node itself names a uid.
func anyNodeUID(cfg *config.Config) bool {
	for _, s := range cfg.Services.List {
		if s.UID != nil {
			return true
		}
	}
	return false
}

// serviceSocketMode is the permission of a socket. A socket of a child is
// 0666: the child's directory decides who may reach it, and containers use
// other uids. A socket on the node with a uid is 0666 too, since the user it
// admits has to connect (the check is SO_PEERCRED). A node socket without a
// uid is Warren's own user's, which needs no group or other bits: 0600,
// whatever the directory's mode. Left open, any local user the directory lets
// through could take that service's lease.
func serviceSocketMode(cfg *config.Config, s *sock) os.FileMode {
	if s.child != nil || s.uid != nil {
		return 0o666
	}
	return 0o600
}

// openServicesDir keeps services.dir's mode in step with the config, when
// Warren owns the directory: 0711 when some node service names a uid (that
// user has to pass through it to reach its socket), back to 0700 when none
// does. A directory made by Warren is made right at start; this is for one
// that already existed, or a config that changed on reload. The directory is
// opened without following a link and examined and changed by descriptor, so
// it cannot be swapped for another between the check and the chmod. One that
// Warren does not own is never changed, only reported.
func (n *Node) openServicesDir(cfg *config.Config) {
	if len(cfg.Services.List) == 0 {
		return
	}
	want := os.FileMode(0o700)
	if anyNodeUID(cfg) {
		want = 0o711
	}
	st, changed, err := socketdir.AdjustDir(cfg.Services.Dir, func(st socketdir.DirState) (os.FileMode, bool) {
		if st.Uid != socketdir.Euid() {
			// Never loosened, nor tightened: not Warren's to change. It is
			// reported when it is not what a directory of Warren's would be.
			if (want == 0o711 && st.Perm&0o001 == 0) || (want == 0o700 && st.Perm&0o077 != 0) || st.Perm&0o022 != 0 {
				n.mu.Lock()
				n.warnOnce("services-dir-owner", "services.dir is not owned by Warren's user, so its mode is not managed", "dir", cfg.Services.Dir, "mode", st.Perm)
				n.mu.Unlock()
			}
			return 0, false
		}
		// Exactly the wanted mode, whatever is set now: a directory others
		// may write to would let them replace the sockets in it.
		return want, st.Perm != want
	})
	switch {
	case errors.Is(err, socketdir.ErrNotPlainDir):
		n.mu.Lock()
		n.warnOnce("services-dir-link", "services.dir is not a plain directory (a link is refused); its mode is not managed", "dir", cfg.Services.Dir)
		n.mu.Unlock()
	case err != nil:
		n.mu.Lock()
		n.warnOnce("services-dir-chmod", "cannot change the mode of services.dir", "dir", cfg.Services.Dir, "err", err)
		n.mu.Unlock()
	case changed:
		n.log.Info("services.dir mode changed to match the config (0711 when a service names a uid, else 0700)", "dir", cfg.Services.Dir, "from", st.Perm, "to", want)
	}
}

func anyUID(cfg *config.Config) bool {
	for _, c := range cfg.Attach.Children {
		if c.UID == nil {
			return false
		}
	}
	return len(cfg.Attach.Children) > 0
}

func refuseUID(w http.ResponseWriter, r *http.Request) {
	apierr.Write(w, apierr.Refused, "this socket admits another user")
}

// dropSock removes a socket's state: its lease ends and its entries go.
// mu held.
func (n *Node) dropSock(s *sock) {
	if s.lease != nil {
		n.endLease(s, "removed")
	}
	s.entries = nil
	if s.staleTimer != nil {
		s.staleTimer.Stop()
	}
	if s.child == nil {
		// The names a node service holds belong to the service name, not to
		// who holds its socket: a socket made again for it (a uid change on
		// reload) starts from these, as one made after a restart does.
		if len(s.heldNames) > 0 || len(s.heldReserved) > 0 {
			n.persisted[s.service] = persistedNames{Names: s.heldNames, Reserved: s.heldReserved}
		} else {
			delete(n.persisted, s.service)
		}
	}
	delete(n.socks, s.key())
}

// ---- children listing ----

// childLabels are the served children sorted for status. mu held.
func (n *Node) childList() []map[string]any {
	var labels []string
	for l := range n.children {
		labels = append(labels, l)
	}
	sort.Strings(labels)
	out := []map[string]any{}
	for _, l := range labels {
		out = append(out, map[string]any{"label": l, "flat": n.children[l].flat})
	}
	return out
}

// childUID is the uid a child's socket requires: the service's own, else
// the child's.
func childUID(ch *child, svc config.Service) *int {
	if svc.UID != nil {
		return svc.UID
	}
	if ch.cfg != nil {
		return ch.cfg.UID
	}
	return nil
}
