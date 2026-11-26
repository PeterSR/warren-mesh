package service

import (
	"encoding/json"
	"fmt"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/grammar"
)

// Vouching (docs/architecture.md, "Vouching"): what node B may say on
// behalf of the names under it. B is trusted for everything under B and for nothing else, so a
// compromised or confused peer can never take another node's names, and a
// child of B can never take a name of B's other children.

// belongsTo reports whether relay is node itself or one of its children
// (x.node), the only origins node may vouch for.
func belongsTo(node, relay string) bool {
	return grammar.RelayID(relay) && grammar.Root(relay) == node
}

// mayPublishUnder reports whether an origin of node may publish in ns (both
// already known to belong to node):
//
//   - the node itself (origin B) publishes in B and in B's service
//     namespaces (l.B); the receiver cannot tell a service namespace from a
//     child's, and B is trusted for all of them;
//   - a child (origin x.B) publishes in B (a flat child) or in x.B, never in
//     another child's namespace or a service namespace.
func mayPublishUnder(node, origin, ns string) bool {
	if origin == node {
		return true
	}
	return ns == node || ns == origin
}

// vouchCall checks the origin and the from of a call that peer node sent.
func vouchCall(node, origin, from string) *apierr.Error {
	if !belongsTo(node, origin) {
		return apierr.New(apierr.Refused, fmt.Sprintf("origin %q is not %s or a child of it", origin, node))
	}
	if from == "" {
		return nil
	}
	_, ns, ok := grammar.SplitAddress(from)
	if !ok {
		return apierr.New(apierr.BadRequest, "from must be name@namespace")
	}
	if grammar.Root(ns) != node || !mayPublishUnder(node, origin, ns) {
		return apierr.New(apierr.Refused, fmt.Sprintf("origin %q may not send as %q", origin, from))
	}
	return nil
}

// wireEntry is one entry of a peer list on the wire.
type wireEntry struct {
	Address string          `json:"address"`
	Origin  string          `json:"origin"`
	Data    json.RawMessage `json:"data"`
	Stale   bool            `json:"stale"`
}

// vouchEntry checks one entry of a list that peer node sent and returns why
// it is refused, or "".
func vouchEntry(node string, e wireEntry) string {
	_, ns, ok := grammar.SplitAddress(e.Address)
	switch {
	case !ok:
		return "address"
	case !belongsTo(node, e.Origin):
		return "origin"
	case grammar.Root(ns) != node || !mayPublishUnder(node, e.Origin, ns):
		return "namespace"
	case len(e.Data) > MaxEntryData:
		return "data"
	}
	return ""
}

// vouchList turns a peer's list into what may be used. Entries that fail a
// check, repeat an address (the earlier one wins, as the sender's order is
// its precedence) or lie beyond the cut are dropped; drop is told each with
// its reason. reserved applies only to the node's own namespace.
func vouchList(node string, in []wireEntry, reserved map[string][]string, drop func(what, reason string)) ([]listed, []string) {
	var out []listed
	seen := map[string]bool{}
	for _, e := range in {
		if len(out) >= MaxListEntries {
			drop(e.Address, "cut")
			continue
		}
		if r := vouchEntry(node, e); r != "" {
			drop(e.Address, r)
			continue
		}
		if seen[e.Address] {
			drop(e.Address, "duplicate")
			continue
		}
		seen[e.Address] = true
		out = append(out, listed{Address: e.Address, Origin: e.Origin, Data: dataOrNull(e.Data), Stale: e.Stale})
	}
	var res []string
	for ns, names := range reserved {
		if ns != node {
			drop("reserved names for "+ns, "namespace")
			continue
		}
		for _, nm := range names {
			if len(res) >= MaxPutReserved || !grammar.EntryName(nm) {
				drop("reserved name "+nm, "reserved")
				continue
			}
			res = append(res, nm)
		}
	}
	return out, res
}
