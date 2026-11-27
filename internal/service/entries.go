package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/grammar"
)

// svcState is everything the node keeps per service name: the merged,
// serialized view and its version.
type svcState struct {
	name   string
	ver    int
	notify chan struct{} // closed, then replaced, on every change
	ready  bool

	// sharedEntries and sharedReserved are the merged view (this node's
	// entries, then every peer's), serialized once per version and served to
	// every poll. localEntries and localReserved are this node's own part,
	// which is what peers are sent.
	sharedEntries  []byte
	sharedReserved []byte
	localEntries   []byte
	localReserved  []byte

	// What peers are served: a snapshot of the local part, taken when the
	// debounce timer fires, with its own version, so a peer's long poll
	// answers 1 s after a change and never for a change of another node's.
	peerVer      int
	peerEntries  []byte
	peerReserved []byte
	peerNotify   chan struct{}
	peerTimer    *time.Timer
	lostJSON     map[sockKey]string
	lost         map[sockKey][]lostRec
	addr         map[string]*sock // address -> publishing socket
	list         []listed         // the node's list, in precedence order, cut
}

// marshalRaw is json.Marshal without HTML escaping, which would turn one
// byte of entry data (<, > and &) into six: what is measured at PUT must be
// what is served, so views and peer lists are written as they were checked.
func marshalRaw(v any) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	return bytes.TrimSuffix(b.Bytes(), []byte("\n"))
}

// MaxListBytes is the most the entries of the node's list for peers may add
// up to (names, origins and data), so that the whole answer stays within
// MaxListBody; the list is cut from the tail by bytes as well as by count.
const MaxListBytes = MaxListBody - 256<<10

// listedSize is what an entry adds to a list answer, with its JSON framing.
func listedSize(l listed) int { return len(l.Address) + len(l.Origin) + len(l.Data) + 64 }

// listed is one entry of the node's list.
type listed struct {
	Address string          `json:"address"`
	Origin  string          `json:"origin"`
	Data    json.RawMessage `json:"data"`
	Stale   bool            `json:"stale"`
	sock    *sock
}

type lostRec struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Reason    string `json:"reason"`
}

// Refusal reasons.
const (
	reasonHeldByNode    = "held-by-node"
	reasonHeldByEarlier = "held-by-earlier-child"
	reasonReserved      = "reserved"
	reasonNamespace     = "namespace"
	reasonGrammar       = "grammar"
	reasonCut           = "cut"
	reasonNotAllowed    = "not-allowed"
)

// svc gets or makes the state of a service name. mu held.
func (n *Node) svc(name string) *svcState {
	if s := n.svcs[name]; s != nil {
		return s
	}
	s := &svcState{name: name, notify: make(chan struct{}), peerNotify: make(chan struct{}),
		localEntries: []byte("[]"), localReserved: []byte("{}"), peerEntries: []byte("[]"), peerReserved: []byte("{}"),
		lostJSON: map[sockKey]string{},
		lost:     map[sockKey][]lostRec{}, addr: map[string]*sock{}, sharedEntries: []byte("[]"), sharedReserved: []byte("{}")}
	n.svcs[name] = s
	return s
}

// notifyChange wakes everyone waiting on the view of svc. mu held.
func (n *Node) notify(svc *svcState) {
	close(svc.notify)
	svc.notify = make(chan struct{})
}

func (n *Node) rebuildAll() {
	for _, svc := range n.svcs {
		n.rebuild(svc)
	}
}

// rank orders sockets by precedence: the node, flat children in config
// order, other configured children in config order, template children by
// label.
func rank(s *sock) (group, order int, label string) {
	switch {
	case s.child == nil:
		return 0, 0, ""
	case s.child.flat:
		return 1, s.child.order, s.child.label
	case !s.child.template:
		return 2, s.child.order, s.child.label
	}
	return 3, 0, s.child.label
}

func (n *Node) socksOf(service string) []*sock {
	var out []*sock
	for k, s := range n.socks {
		if k.service == service {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		gi, oi, li := rank(out[i])
		gj, oj, lj := rank(out[j])
		if gi != gj {
			return gi < gj
		}
		if oi != oj {
			return oi < oj
		}
		return li < lj
	})
	return out
}

func (s *sock) maxEntries() int {
	if s.child == nil {
		return s.n.maxList
	}
	return s.child.max
}

// visible: a socket's entries are shown while its lease lives and, stale,
// for StaleFor after it ended.
func (s *sock) visible(now time.Time) bool {
	if len(s.entries) == 0 {
		return false
	}
	return s.live() || (!s.endedAt.IsZero() && now.Sub(s.endedAt) < StaleFor)
}

// live reports whether the entries were published by the lease that is live.
func (s *sock) live() bool { return s.lease != nil && s.entriesFrom == s.lease }

// childNames are a child's name patterns, nil for none.
func (s *sock) childNames() []string {
	if s.child != nil && s.child.cfg != nil {
		return s.child.cfg.Names
	}
	return nil
}

// rebuild recomputes the merged list, the lost entries and the serialized
// view of one service, and bumps the version if anything changed. It is
// called after every change that could matter. mu held.
func (n *Node) rebuild(svc *svcState) {
	now := time.Now()
	socks := n.socksOf(svc.name)

	changed := false
	nodeSock := n.socks[sockKey{n.name, svc.name}]
	if !svc.ready && (nodeSock == nil || len(nodeSock.cfg.Run) == 0 || nodeSock.published || now.Sub(n.bootAt) >= BootHold) {
		svc.ready = true
		changed = true
	}

	nodeNames, nodeReserved := map[string]bool{}, map[string]bool{}
	if nodeSock != nil {
		for _, nm := range nodeSock.heldNames {
			nodeNames[nm] = true
		}
		for _, nm := range nodeSock.heldReserved {
			nodeReserved[nm] = true
		}
	}
	claims := map[string]string{} // name in the node's namespace -> flat child relay
	var list []listed
	listBytes := 0
	listFull := false
	lost := map[sockKey][]lostRec{}
	addr := map[string]*sock{}
	for _, s := range socks {
		if !s.visible(now) {
			continue
		}
		stale := !s.live()
		count := 0
		for _, e := range s.entries {
			reason := ""
			switch {
			case !grammar.MatchNames(s.childNames(), e.Name):
				reason = reasonNotAllowed
			case !s.mayPublishIn(e.Namespace):
				// A reload may have taken the namespace away.
				reason = reasonNamespace
			case s.flat():
				switch {
				case nodeNames[e.Name]:
					reason = reasonHeldByNode
				case nodeReserved[e.Name]:
					reason = reasonReserved
				case claims[e.Name] != "" && claims[e.Name] != s.relay:
					reason = reasonHeldByEarlier
				}
				if reason == "" {
					claims[e.Name] = s.relay
				}
			}
			le := listed{Address: grammar.JoinAddress(e.Name, e.Namespace), Origin: s.relay, Data: dataOrNull(e.Data)}
			// Once the list is full by bytes, everything after is cut too: it is
			// a cut from the tail, in precedence order.
			if reason == "" && listBytes+listedSize(le) > MaxListBytes {
				listFull = true
			}
			if reason == "" && (count >= s.maxEntries() || len(list) >= n.maxList || listFull) {
				reason = reasonCut
			}
			if reason != "" {
				lost[s.key()] = append(lost[s.key()], lostRec{e.Name, e.Namespace, reason})
				continue
			}
			count++
			a := grammar.JoinAddress(e.Name, e.Namespace)
			list = append(list, listed{Address: a, Origin: s.relay, Data: dataOrNull(e.Data), Stale: stale, sock: s})
			listBytes += listedSize(list[len(list)-1])
			addr[a] = s
		}
	}

	localJSON := marshalRaw(nonNil(list))
	reserved := map[string][]string{}
	if nodeSock != nil && len(nodeSock.heldReserved) > 0 {
		reserved[n.name] = nodeSock.heldReserved
	}
	localReservedJSON := marshalRaw(reserved)
	if !bytes.Equal(localJSON, svc.localEntries) || !bytes.Equal(localReservedJSON, svc.localReserved) {
		svc.localEntries, svc.localReserved = localJSON, localReservedJSON
		n.schedulePeers(svc)
	}

	// The merged view: this node's list in precedence order, then the other
	// nodes' by name, each in its own order.
	merged := append([]listed(nil), list...)
	for _, ps := range n.sortedPeers() {
		rl := ps.lists[svc.name]
		if rl == nil || !ps.visible(now) {
			continue
		}
		stale := !ps.up || rl.stale
		for _, e := range rl.entries {
			e.Stale = e.Stale || stale
			merged = append(merged, e)
		}
		if len(rl.reserved) > 0 {
			reserved[ps.node] = rl.reserved
		}
	}
	entriesJSON := marshalRaw(nonNil(merged))
	reservedJSON := marshalRaw(reserved)
	if !bytes.Equal(entriesJSON, svc.sharedEntries) || !bytes.Equal(reservedJSON, svc.sharedReserved) {
		changed = true
	}
	lostJSON := map[sockKey]string{}
	for k, l := range lost {
		b := marshalRaw(l)
		lostJSON[k] = string(b)
	}
	if len(lostJSON) != len(svc.lostJSON) {
		changed = true
	}
	for k, v := range lostJSON {
		if svc.lostJSON[k] != v {
			changed = true
		}
	}
	svc.sharedEntries, svc.sharedReserved = entriesJSON, reservedJSON
	svc.lost, svc.lostJSON, svc.addr, svc.list = lost, lostJSON, addr, list
	if changed {
		svc.ver++
		n.notify(svc)
	}
}

func nonNil(l []listed) []listed {
	if l == nil {
		return []listed{}
	}
	return l
}

func dataOrNull(d json.RawMessage) json.RawMessage {
	if len(d) == 0 {
		return json.RawMessage("null")
	}
	return d
}

// versionString is the view version a poll passes back as since.
func (n *Node) versionString(svc *svcState) string {
	return fmt.Sprintf("%s.%d", n.boot[:8], svc.ver)
}

// NodeList is the node's list for a service, in precedence order and cut:
// what peers will be sent.
func (n *Node) NodeList(service string) []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []string
	if svc := n.svcs[service]; svc != nil {
		for _, l := range svc.list {
			out = append(out, l.Address)
		}
	}
	return out
}

// ---- PUT ----

// parsePut checks everything about a PUT that needs no state: shape,
// counts, sizes, grammar of reserved names, duplicates.
func parsePut(body []byte, entryNS string) (*putRequest, *apierr.Error) {
	// The counts and the size of the data are enforced while decoding.
	req, err := decodePut(body)
	if err != nil {
		return nil, putError(err)
	}
	for _, r := range req.Reserved {
		if !grammar.EntryName(r) {
			return nil, apierr.New(apierr.BadRequest, fmt.Sprintf("reserved name %q is not an entry name", r))
		}
	}
	seen := map[string]bool{}
	for i := range req.Entries {
		e := &req.Entries[i]
		ns := e.Namespace
		if ns == "" {
			ns = entryNS
		}
		if seen[e.Name+"@"+ns] {
			return nil, apierr.New(apierr.BadRequest, fmt.Sprintf("entry %q listed twice", e.Name))
		}
		seen[e.Name+"@"+ns] = true
	}
	return req, nil
}

// applyPut stores a PUT from the lease holder and returns what is refused.
// mu held.
func (n *Node) applyPut(s *sock, l *lease, req *putRequest) ([]lostRec, *apierr.Error) {
	if s.lease != l || l.ended {
		return nil, apierr.New(apierr.Gone, "the lease has ended")
	}
	refused := []lostRec{}
	if !s.isNode() {
		// Only node services reserve: a child's reserved names are ignored
		// and reported.
		for _, r := range req.Reserved {
			refused = append(refused, lostRec{r, "", reasonNotAllowed})
		}
		req.Reserved = nil
	}
	var accepted []entry
	for _, e := range req.Entries {
		ns := e.Namespace
		if ns == "" {
			ns = s.entryNS()
		}
		switch {
		case !grammar.EntryName(e.Name):
			refused = append(refused, lostRec{e.Name, e.Namespace, reasonGrammar})
		case !s.mayPublishIn(ns):
			refused = append(refused, lostRec{e.Name, e.Namespace, reasonNamespace})
		case !grammar.MatchNames(s.childNames(), e.Name):
			refused = append(refused, lostRec{e.Name, e.Namespace, reasonNotAllowed})
		default:
			accepted = append(accepted, entry{Name: e.Name, Namespace: ns, Data: dataOrNull(e.Data)})
		}
	}
	s.entries = accepted
	s.entriesFrom = l
	s.published = true
	if s.isNode() {
		s.heldNames = s.heldNames[:0:0]
		for _, e := range accepted {
			if e.Namespace == n.name {
				s.heldNames = append(s.heldNames, e.Name)
			}
		}
		s.heldReserved = append([]string(nil), req.Reserved...)
		n.saveNames()
	}
	svc := n.svc(s.service)
	n.rebuild(svc)
	// The precedence outcome of this PUT: what is displaced right now.
	refused = append(refused, svc.lost[s.key()]...)
	return refused, nil
}
