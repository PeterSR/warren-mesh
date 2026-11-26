// Package link is how Warren reaches a transport. Inside Warren it is one
// small interface (Link); behind it are the real forms (inherited
// descriptors for a managed transport, two sockets for an external one) and
// an in-memory fake for tests. The wire is the /v1/transport API over HTTP/2
// with prior knowledge (h2c), so many requests share one connection.
package link

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/PeterSR/warren-mesh/internal/grammar"
)

// APIVersion is the /v1/transport version this Warren speaks.
const APIVersion = 1

// MinStreams is how many concurrent HTTP/2 streams both sides of each link
// allow (SETTINGS_MAX_CONCURRENT_STREAMS). Warren checks at start that its
// config's need fits.
const MinStreams = 1000

// HeaderNode is on every peer answer: the node that answered, which must
// equal the node the transport reached. The peer protocol checks it.
const HeaderNode = "X-Warren-Node"

// MinMaxBody is the smallest max_body Warren accepts from a transport: the
// largest peer list answer (4 MiB) plus headers.
const MinMaxBody = 4<<20 + 64<<10

// Header names of the transport API.
const (
	HeaderTo       = "X-Warren-Transport-To"
	HeaderFrom     = "X-Warren-Transport-From"
	HeaderError    = "X-Warren-Transport-Error"
	HeaderDeadline = "X-Warren-Deadline-Ms"
	// PrefixTransport marks headers that belong to one link hop and are
	// never carried.
	PrefixTransport = "X-Warren-Transport-"
)

// Values of HeaderError.
const (
	NotSent = "not-sent" // the request provably never left
	Lost    = "lost"     // sent, and what happened is unknown
	// Busy is a refusal for the transport's own limits, before sending: the
	// request never left, and Warren reports busy, not offline.
	Busy = "busy"
	// TooBigKind marks, on link A, a request that was sent and whose answer was
	// over the size a transport carries back (CarryError.TooBig).
	TooBigKind = "too-big"
)

// Hello is what a transport says about itself. Name and SoftwareVersion are
// optional additions that Warren shows in status.
type Hello struct {
	Version         int    `json:"version"`
	Node            string `json:"node"`
	MaxBody         int    `json:"max_body"`
	Name            string `json:"name,omitempty"`
	SoftwareVersion string `json:"software_version,omitempty"`
}

// CheckHello is Warren's startup check: refuse, loudly, a transport that
// speaks another version, is for another node, or cannot carry a full
// peer list.
func CheckHello(h Hello, node string) error {
	switch {
	case h.Version != APIVersion:
		return fmt.Errorf("transport speaks /v%d/transport, this Warren speaks /v%d", h.Version, APIVersion)
	case h.Node != node:
		return fmt.Errorf("transport says this node is %q, the config says %q", h.Node, node)
	case h.MaxBody < MinMaxBody:
		return fmt.Errorf("transport max_body is %d bytes, below the %d bytes a peer list needs", h.MaxBody, MinMaxBody)
	}
	return nil
}

// PeerList is the nodes a transport currently sees.
type PeerList struct {
	Version string   `json:"version"`
	Peers   []string `json:"peers"`
}

// Request is a peer request for the transport to carry: Method, and URI
// (path and query, starting /v<N>/peer/).
type Request struct {
	Method   string
	URI      string
	Header   http.Header
	Body     []byte
	Deadline time.Time
}

// Response is what the other node answered.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// CarryError is a failure of the transport to carry a request: the
// difference between Warren knowing a request never left (NotSent: safe to
// retry) and not knowing (Lost).
type CarryError struct {
	Kind   string
	Detail string
	// TooBig marks an answer over the size a transport carries back: the
	// request was sent, and the answer was refused for its size.
	TooBig bool
}

func (e *CarryError) Error() string { return "transport: " + e.Kind + ": " + e.Detail }

// Link is what Warren uses of a transport.
type Link interface {
	// Hello asks the transport about itself.
	Hello(ctx context.Context) (Hello, error)
	// Peers long-polls the transport's peer list; wait is at most 25 s.
	Peers(ctx context.Context, since string, wait time.Duration) (PeerList, error)
	// Carry has the transport deliver req to node `to` and returns the
	// answer. A failure of the carrying itself is a *CarryError.
	Carry(ctx context.Context, to string, req *Request) (*Response, error)
	// Handle sets the handler of requests arriving from the network (link
	// B). Each has From set to the node the transport vouches for; see From.
	Handle(h http.Handler)
	// Close ends the link.
	Close() error
}

type fromKey struct{}

// From is the node the transport vouched for on an incoming request.
func From(ctx context.Context) string {
	s, _ := ctx.Value(fromKey{}).(string)
	return s
}

// WithFrom records the vouched-for node on a context.
func WithFrom(ctx context.Context, node string) context.Context {
	return context.WithValue(ctx, fromKey{}, node)
}

// carryHeaders is the allow-list of what travels between nodes, in both
// directions: Content-Type and the X-Warren- headers, except the hop-local
// X-Warren-Transport- ones. Hop-by-hop headers and Content-Length are never
// carried; each hop sets its own.
func carryHeaders(h http.Header) http.Header {
	out := http.Header{}
	for k, v := range h {
		ck := http.CanonicalHeaderKey(k)
		if ck != "Content-Type" && !strings.HasPrefix(ck, "X-Warren-") {
			continue
		}
		if strings.HasPrefix(ck, PrefixTransport) {
			continue
		}
		out[ck] = append([]string(nil), v...)
	}
	return out
}

// CarryHeaders is what a transport carries of a set of headers in either
// direction: Content-Type and the X-Warren- headers, never the hop-local
// X-Warren-Transport- ones, never anything else.
func CarryHeaders(h http.Header) http.Header { return carryHeaders(h) }

// IsPeerPath reports whether p is a peer protocol path, /v<N>/peer/<...>.
// A transport carries every version, so a release that speaks two peer
// protocols (the one-release window) needs no change in it.
func IsPeerPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v")
	if !ok {
		return false
	}
	i := 0
	for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
		i++
	}
	return i > 0 && i <= 6 && strings.HasPrefix(rest[i:], "/peer/")
}

// h2 is the HTTP/2 setting both sides use.
func h2() *http.HTTP2Config { return &http.HTTP2Config{MaxConcurrentStreams: MinStreams} }

// validNode is the label check on a node name from the transport.
func validNode(n string) bool { return grammar.Label(n) }

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
