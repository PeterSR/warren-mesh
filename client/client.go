package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/PeterSR/warren-mesh/internal/grammar"
	"github.com/PeterSR/warren-mesh/internal/platform"
)

// Environment variables of the contract.
const (
	// EnvSocket is the socket a managed service is started with.
	EnvSocket = "WARREN_SERVICE_SOCKET"
	// EnvUID names the user Warren runs as; if set, a socket served by
	// anyone else is refused.
	EnvUID = "WARREN_UID"
)

// MaxCallBody is the most one call's request or response may carry (1 MiB).
const MaxCallBody = grammar.MaxCallBody

// MaxAnswer is the most the client reads of one answer from Warren. A view
// is the largest: up to 1000 entries of 3 KiB for each node of the mesh and
// each service, so a mesh of a few dozen nodes still fits. An answer over it
// is ErrAnswerTooLarge.
const MaxAnswer = 64 << 20

// ErrAnswerTooLarge is returned when Warren's answer is over MaxAnswer. The
// request was made and answered; the answer cannot be used. Trying again
// gives the same, so a Watcher returns it instead of retrying.
var ErrAnswerTooLarge = errors.New("client: the answer from Warren is over the size the client reads")

// Client talks to one service socket. It is safe for concurrent use.
type Client struct {
	path string
	uid  int // -1: not checked
	hc   *http.Client
	tr   *http.Transport
	// timeout bounds a request that is not a long poll.
	timeout time.Duration
	err     error // an option that was wrong, reported by Dial
}

// Option changes how Dial connects.
type Option func(*Client)

// WithServerUID makes every connection check, with SO_PEERCRED, that the
// socket is served by this user. It takes the place of WARREN_UID.
//
// A negative uid is an error from Dial, never a silent switch-off: use
// WithoutServerUIDCheck for that.
func WithServerUID(uid int) Option {
	return func(c *Client) {
		if uid < 0 {
			c.err = fmt.Errorf("client: WithServerUID(%d): a user id is not negative; use WithoutServerUIDCheck to turn the check off", uid)
			return
		}
		c.uid = uid
	}
}

// WithoutServerUIDCheck turns the check off, even if WARREN_UID is set.
func WithoutServerUIDCheck() Option { return func(c *Client) { c.uid = -1 } }

// WithRequestTimeout bounds every request that is not a long poll or a call
// (which have their own bounds). The default is 30 seconds.
func WithRequestTimeout(d time.Duration) Option { return func(c *Client) { c.timeout = d } }

// Dial makes a client of the socket at path; an empty path means the
// WARREN_SERVICE_SOCKET environment variable. When WARREN_UID is set (and
// no option says otherwise), the server on the socket must run as that
// user. Dial connects once, so a missing socket or the wrong server is
// reported here.
func Dial(path string, opts ...Option) (*Client, error) {
	if path == "" {
		path = os.Getenv(EnvSocket)
	}
	if path == "" {
		return nil, errors.New("client: no socket path given and " + EnvSocket + " is not set")
	}
	c := &Client{path: path, uid: -2, timeout: 30 * time.Second}
	for _, o := range opts {
		o(c)
	}
	if c.err != nil {
		return nil, c.err
	}
	if c.uid == -2 { // no option: the environment decides
		c.uid = -1
		if s := os.Getenv(EnvUID); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n < 0 {
				return nil, fmt.Errorf("client: %s=%q is not a user id", EnvUID, s)
			}
			c.uid = n
		}
	}
	c.tr = &http.Transport{
		DialContext: c.dial,
		// A lease keeps up to inflight polls open, and a socket takes 64
		// connections: stay below, and let requests queue instead.
		MaxConnsPerHost:     60,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     60 * time.Second,
		DisableCompression:  true,
		Proxy:               nil,
	}
	c.hc = &http.Client{Transport: c.tr}
	conn, err := c.dial(context.Background(), "unix", path)
	if err != nil {
		return nil, err
	}
	conn.Close()
	return c, nil
}

// Path is the socket the client uses.
func (c *Client) Path() string { return c.path }

// Close drops idle connections. Requests in flight are not affected. It
// always returns nil; it has the error result so that a Client is an
// io.Closer.
func (c *Client) Close() error {
	c.tr.CloseIdleConnections()
	return nil
}

// dial connects and, if a uid is required, checks who serves the socket.
func (c *Client) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", c.path)
	if err != nil {
		return nil, err
	}
	if c.uid >= 0 {
		uid, ok := platform.ConnUID(conn)
		if !ok {
			conn.Close()
			return nil, fmt.Errorf("client: cannot tell which user serves %s, and a server user is required", c.path)
		}
		if uid != c.uid {
			conn.Close()
			return nil, fmt.Errorf("client: %s is served by user %d, not the expected %d", c.path, uid, c.uid)
		}
	}
	return conn, nil
}

// response is a finished request.
type response struct {
	status int
	header http.Header
	body   []byte
}

// do sends one request and returns a 2xx answer, or an *Error. A request
// with a timeout of zero is bounded by the client's request timeout; long
// polls and calls pass their own.
func (c *Client) do(ctx context.Context, method, path string, q url.Values, body []byte, timeout time.Duration) (*response, error) {
	r, _, err := c.doSent(ctx, method, path, q, body, timeout)
	return r, err
}

// doSent is do, and also reports whether the request's headers were written
// to the connection before it failed: after that, whatever happened to the
// request, the server may have acted on it. net/http may retry a request on
// a new connection when nothing reached the old one (a dead idle
// connection); GetConn fires for each attempt, so only the last attempt
// counts.
func (c *Client) doSent(ctx context.Context, method, path string, q url.Values, body []byte, timeout time.Duration) (*response, bool, error) {
	var sent atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn:      func(string) { sent.Store(false) },
		WroteHeaders: func() { sent.Store(true) },
	})
	if timeout == 0 {
		timeout = c.timeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	u := "http://warren" + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	var rd io.Reader
	if method == http.MethodPost || method == http.MethodPut {
		// A body needs a Content-Length: bytes.Reader gives one, and an
		// empty body is sent as Content-Length: 0.
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("X-Warren", "1")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, sent.Load(), err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, MaxAnswer+1))
	if err != nil {
		return nil, true, err
	}
	if len(b) > MaxAnswer {
		return nil, true, ErrAnswerTooLarge
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, true, errorFrom(resp, b)
	}
	return &response{resp.StatusCode, resp.Header, b}, true, nil
}

func (c *Client) getJSON(ctx context.Context, path string, q url.Values, out any) error {
	r, err := c.do(ctx, http.MethodGet, path, q, nil, 0)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(r.body, out); err != nil {
		return fmt.Errorf("client: %s answered with something that is not the expected JSON: %w", path, err)
	}
	return nil
}

// Self is what the socket says about itself (GET /v1/service/self).
type Self struct {
	// Relay is the relay id this socket is at, such as "host.server".
	Relay string `json:"relay"`
	Node  string `json:"node"`
	// Service is the service this socket is.
	Service string `json:"service"`
	// Flat is true for a flat child: entries take the node's namespace.
	Flat bool `json:"flat"`
	// EntryNamespace is where this service's entries go by default.
	EntryNamespace string `json:"entry_namespace"`
	// Namespaces are the full namespaces the service may also publish under.
	Namespaces []string `json:"namespaces"`
	// Boot changes on every Warren start.
	Boot        string `json:"boot"`
	MaxCallBody int    `json:"max_call_body"`
}

// Self asks who this socket is.
func (c *Client) Self(ctx context.Context) (*Self, error) {
	var s Self
	if err := c.getJSON(ctx, "/v1/service/self", nil, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Status is Warren's status (GET /v1/service/status). Fields a newer Warren
// adds are ignored.
type Status struct {
	Node      string          `json:"node"`
	Boot      string          `json:"boot"`
	Warren    WarrenStatus    `json:"warren"`
	Transport TransportStatus `json:"transport"`
	Peers     []PeerStatus    `json:"peers"`
	// Children are the child directories being served.
	Children []ChildStatus `json:"children"`
	// Services are the sockets of the node and its children, and whether
	// each has a live lease.
	Services []ServiceStatus `json:"services"`
	// LinkUp says whether the transport link works.
	LinkUp bool `json:"link_up"`
}

// WarrenStatus is this Warren's version and what it speaks with peers.
type WarrenStatus struct {
	Version      string   `json:"version"`
	PeerProtocol []int    `json:"peer_protocol"`
	Features     []string `json:"features"`
}

// TransportStatus is the transport this Warren runs on.
type TransportStatus struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ChildStatus is a child directory being served.
type ChildStatus struct {
	Label string `json:"label"`
	Flat  bool   `json:"flat"`
}

// ServiceStatus is one socket and whether it has a live lease.
type ServiceStatus struct {
	Relay string `json:"relay"`
	Name  string `json:"name"`
	Lease bool   `json:"lease"`
}

// PeerStatus is one other node, as this Warren sees it.
type PeerStatus struct {
	Node         string   `json:"node"`
	Up           bool     `json:"up"`
	Version      string   `json:"version"`
	PeerProtocol []int    `json:"peer_protocol"`
	Features     []string `json:"features"`
	// Error says why a peer cannot be used (a protocol this Warren does not
	// share), when it is set.
	Error string `json:"error,omitempty"`
}

// Status asks for Warren's status.
func (c *Client) Status(ctx context.Context) (*Status, error) {
	var s Status
	if err := c.getJSON(ctx, "/v1/service/status", nil, &s); err != nil {
		return nil, err
	}
	return &s, nil
}
