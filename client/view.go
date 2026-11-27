package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/PeterSR/warren-mesh/internal/grammar"
)

// Limits of entries, from the contract.
const (
	// MaxEntryData is the most data one entry holds (3 KiB).
	MaxEntryData = grammar.MaxEntryData
	// MaxPutEntries is the most entries one PUT holds (1000).
	MaxPutEntries = grammar.MaxPutEntries
	// MaxPutReserved is the most reserved names one PUT holds (64).
	MaxPutReserved = grammar.MaxPutReserved
	// MaxPutBody is the most a PUT of entries may carry (4 MiB).
	MaxPutBody = grammar.MaxPutBody
	// MinPutInterval is the least time between two PUTs on one socket (1 s).
	MinPutInterval = grammar.MinPutInterval
	// MaxViewPolls is the most view polls a socket holds (4).
	MaxViewPolls = grammar.MaxViewPolls
	// MaxInflight is the most inflight a socket has (and so polls per lease), 32.
	MaxInflight = grammar.MaxInflight
	// MaxWait is the longest a long poll waits (25 s).
	MaxWait = grammar.MaxWait

	viewRequestSlack = 15 * time.Second
)

// Entry is one named thing a service publishes, such as an agent.
type Entry struct {
	Name string `json:"name"`
	// Namespace is empty for the socket's entry namespace, or one of the
	// namespaces Self reports.
	Namespace string `json:"namespace,omitempty"`
	// Data is any JSON value, at most 3 KiB; nil is null.
	Data json.RawMessage `json:"data"`
}

// EntriesRequest is the list a lease holder publishes. It replaces the
// previous list. Order by importance: lists are cut from the tail.
type EntriesRequest struct {
	Entries []Entry `json:"entries"`
	// Reserved names are kept in the service's own namespace for entries to
	// come. Only services on the node itself may reserve.
	Reserved []string `json:"reserved,omitempty"`
}

// Refusal is an entry that is not published (PUT answer) or is displaced
// (view's lost), with the reason: held-by-node, held-by-earlier-child,
// reserved, namespace, grammar, cut or not-allowed.
type Refusal struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Reason    string `json:"reason"`
}

// PutEntries publishes the list under a lease (PUT /v1/service/entries).
// It returns what was refused or is displaced right now. At most one PUT per
// second per socket is accepted (busy otherwise); Server does the pacing for
// a service that uses it.
func (c *Client) PutEntries(ctx context.Context, lease string, e EntriesRequest) ([]Refusal, error) {
	// Nil and empty data are both null; the list is copied, not changed.
	list := make([]Entry, len(e.Entries))
	for i, en := range e.Entries {
		if len(en.Data) == 0 {
			en.Data = nil
		}
		list[i] = en
	}
	e.Entries = list
	b, err := json.Marshal(e)
	if err != nil {
		// Entry data that is not JSON: nothing was sent, and sending it again
		// cannot help.
		return nil, &Error{Code: CodeBadRequest, Detail: "entries cannot be encoded: " + err.Error(), Err: err}
	}
	// A flat child's PUT may be held up to 10 s while the node's own
	// services publish first.
	r, err := c.do(ctx, http.MethodPut, "/v1/service/entries", url.Values{"lease": {lease}}, b, 60*time.Second)
	if err != nil {
		return nil, err
	}
	var out struct {
		Refused []Refusal `json:"refused"`
	}
	if err := json.Unmarshal(r.body, &out); err != nil {
		return nil, fmt.Errorf("client: PUT entries answered with something that is not the expected JSON: %w", err)
	}
	return out.Refused, nil
}

// View is this service's entries across the mesh.
type View struct {
	// Version is what to pass as since to wait for the next change.
	Version string `json:"version"`
	// Boot changes when Warren restarts: a service must serve and PUT again.
	Boot string `json:"boot"`
	// Ready is false while a flat child is held back until the node's own
	// services have published (up to 10 s after Warren starts).
	Ready bool `json:"ready"`
	// Reserved are the reserved names per node namespace.
	Reserved map[string][]string `json:"reserved"`
	// Lost are this socket's entries that are displaced right now.
	Lost    []Refusal   `json:"lost"`
	Entries []ViewEntry `json:"entries"`
	// BootChanged is set by a Watcher when Boot differs from the view it
	// returned before. It is not part of the wire format.
	BootChanged bool `json:"-"`
}

// ViewEntry is one entry of the mesh.
type ViewEntry struct {
	// Address is name@namespace, what a call's to takes.
	Address string `json:"address"`
	// Origin is the relay id that published it.
	Origin string          `json:"origin"`
	Data   json.RawMessage `json:"data"`
	// Stale entries belong to a node that cannot be reached or a service
	// whose lease ended; they are dropped after 60 s.
	Stale bool `json:"stale"`
}

// View fetches the view (GET /v1/service/view). With since empty, or from
// another boot, or older than the current version, it answers at once;
// otherwise it waits up to wait (whole seconds, at most 25) for a change and
// then answers with the current view, changed or not. At most 4 view polls
// may be open per socket (busy otherwise).
func (c *Client) View(ctx context.Context, since string, wait time.Duration) (*View, error) {
	wait = min(max(wait, 0), MaxWait)
	secs := int(wait / time.Second)
	q := url.Values{"since": {since}, "wait": {strconv.Itoa(secs)}}
	r, err := c.do(ctx, http.MethodGet, "/v1/service/view", q, nil, time.Duration(secs)*time.Second+viewRequestSlack)
	if err != nil {
		return nil, err
	}
	var v View
	if err := json.Unmarshal(r.body, &v); err != nil {
		return nil, fmt.Errorf("client: view answered with something that is not the expected JSON: %w", err)
	}
	return &v, nil
}

// Watcher follows the view: each Next returns the next version.
type Watcher struct {
	c     *Client
	since string
	boot  string
	have  bool
}

// NewWatcher starts a watcher; its first Next returns the current view.
func (c *Client) NewWatcher() *Watcher { return &Watcher{c: c} }

// Next long-polls until the view changes and returns it, with BootChanged
// set when Warren has restarted since the last view (the service must then
// serve and PUT again). It backs off and retries on busy, not-ready and
// connection failures, and returns the context's error when ctx ends, or
// any other server error.
func (w *Watcher) Next(ctx context.Context) (*View, error) {
	back := newBackoff(500*time.Millisecond, 10*time.Second)
	for {
		v, err := w.c.View(ctx, w.since, MaxWait)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if _, isServer := AsError(err); isServer && !IsCode(err, CodeBusy, CodeNotReady) {
				return nil, err
			}
			if errors.Is(err, ErrAnswerTooLarge) {
				return nil, err // the next view will be as large
			}
			if !sleep(ctx, back.next()) {
				return nil, ctx.Err()
			}
			continue
		}
		if w.have && v.Version == w.since {
			continue // the wait ended without a change
		}
		v.BootChanged = w.have && v.Boot != w.boot
		w.since, w.boot, w.have = v.Version, v.Boot, true
		return v, nil
	}
}

// sleep waits d or until ctx ends; it reports whether the full time passed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// backoff doubles from min to max.
type backoff struct{ min, max, cur time.Duration }

func newBackoff(min, max time.Duration) *backoff { return &backoff{min: min, max: max} }

func (b *backoff) next() time.Duration {
	if b.cur == 0 {
		b.cur = b.min
	} else if b.cur *= 2; b.cur > b.max {
		b.cur = b.max
	}
	return b.cur
}

func (b *backoff) reset() { b.cur = 0 }
