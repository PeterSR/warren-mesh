package link

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/grammar"
)

// maxAnswer bounds what Warren reads back from one carried request.
const maxAnswer = 8 << 20

// dialError marks a failure to reach the transport at all, so the request
// is known not to have been sent.
type dialError struct{ err error }

func (e *dialError) Error() string { return e.err.Error() }
func (e *dialError) Unwrap() error { return e.err }

// Client is link A: Warren as the client of the transport.
type Client struct {
	hc   *http.Client
	tr   *http.Transport
	mu   sync.Mutex
	maxB int
}

// NewClient makes a link A client that gets its connection from dial. HTTP/2
// is spoken with prior knowledge: no Upgrade, no TLS.
func NewClient(dial func(ctx context.Context) (net.Conn, error)) *Client {
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	tr := &http.Transport{
		Protocols: &p,
		HTTP2:     h2(),
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			c, err := dial(ctx)
			if err != nil {
				return nil, &dialError{err}
			}
			return c, nil
		},
		IdleConnTimeout: 0,
	}
	return &Client{tr: tr, hc: &http.Client{Transport: tr}, maxB: MinMaxBody}
}

// NewH2CClient is an HTTP client for the other end of a link: HTTP/2 with
// prior knowledge over the connections dial makes, which it keeps open while
// idle. A failure to get a connection is reported by IsDialError, and means
// the request was not sent.
func NewH2CClient(dial func(ctx context.Context) (net.Conn, error)) *http.Client {
	return NewClient(dial).hc
}

// IsDialError reports whether err is a failure to get a connection to the
// other end of a link, so that nothing was sent.
func IsDialError(err error) bool {
	var de *dialError
	return errors.As(err, &de)
}

// Reset drops pooled connections so the next request dials again.
func (c *Client) Reset() { c.tr.CloseIdleConnections() }

// Close ends the client.
func (c *Client) Close() error { c.tr.CloseIdleConnections(); return nil }

const base = "http://transport"

func (c *Client) do(req *http.Request) (*http.Response, error) { return c.hc.Do(req) }

// Hello implements Link.
func (c *Client) Hello(ctx context.Context) (Hello, error) {
	var h Hello
	if err := c.getJSON(ctx, "/v1/transport/hello", &h); err != nil {
		return h, err
	}
	c.mu.Lock()
	if h.MaxBody > 0 {
		c.maxB = h.MaxBody
	}
	c.mu.Unlock()
	return h, nil
}

// Peers implements Link.
func (c *Client) Peers(ctx context.Context, since string, wait time.Duration) (PeerList, error) {
	wait = min(wait, grammar.MaxWait)
	ctx, cancel := context.WithTimeout(ctx, wait+10*time.Second)
	defer cancel()
	q := url.Values{"since": {since}, "wait": {strconv.Itoa(int(wait / time.Second))}}
	var p PeerList
	err := c.getJSON(ctx, "/v1/transport/peers?"+q.Encode(), &p)
	return p, err
}

func (c *Client) getJSON(ctx context.Context, uri string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+uri, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Warren", "1")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err // not an answer: the connection failed while it was read
	}
	if resp.StatusCode != http.StatusOK {
		if e := apierr.FromResponse(resp, body); e != nil {
			return &AnswerError{e}
		}
		return &AnswerError{fmt.Errorf("transport answered %d", resp.StatusCode)}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return &AnswerError{fmt.Errorf("transport answered with something that is not the expected JSON: %w", err)}
	}
	return nil
}

// AnswerError is a transport that answered, and not with what was asked: an
// error status, or a body that does not parse. Unlike a failure to reach it
// or a timeout, it is a refusal.
type AnswerError struct{ Err error }

func (e *AnswerError) Error() string { return e.Err.Error() }
func (e *AnswerError) Unwrap() error { return e.Err }

// IsAnswerError reports whether err is an AnswerError.
func IsAnswerError(err error) bool {
	var ae *AnswerError
	return errors.As(err, &ae)
}

// Carry implements Link.
func (c *Client) Carry(ctx context.Context, to string, r *Request) (*Response, error) {
	if !IsPeerPath(r.URI) {
		return nil, fmt.Errorf("link: only /v<N>/peer/ requests are carried, not %q", r.URI)
	}
	c.mu.Lock()
	maxB := c.maxB
	c.mu.Unlock()
	if len(r.Body) > maxB {
		return nil, &CarryError{Kind: NotSent, Detail: "request is larger than the transport's max_body"}
	}
	if err := ctx.Err(); err != nil {
		return nil, &CarryError{Kind: NotSent, Detail: err.Error()}
	}
	dl := r.Deadline
	if d, ok := ctx.Deadline(); ok && (dl.IsZero() || d.Before(dl)) {
		dl = d
	}
	if dl.IsZero() {
		dl = time.Now().Add(30 * time.Second)
	}
	// The deadline is enforced here, whatever the caller's context says.
	ctx, cancel := context.WithDeadline(ctx, dl)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, r.Method, base+r.URI, bytes.NewReader(r.Body))
	if err != nil {
		return nil, err
	}
	for k, v := range carryHeaders(r.Header) {
		req.Header[k] = v
	}
	req.Header.Set(HeaderTo, to)
	ms := time.Until(dl).Milliseconds()
	if ms < 1 {
		ms = 1
	}
	req.Header.Set(HeaderDeadline, strconv.FormatInt(ms, 10))
	resp, err := c.do(req)
	if err != nil {
		var de *dialError
		if errors.As(err, &de) {
			return nil, &CarryError{Kind: NotSent, Detail: de.err.Error()}
		}
		return nil, &CarryError{Kind: Lost, Detail: err.Error()}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswer+1))
	if err != nil {
		return nil, &CarryError{Kind: Lost, Detail: err.Error()}
	}
	if len(body) > maxAnswer {
		return nil, &CarryError{Kind: Lost, Detail: fmt.Sprintf("the answer is over %d bytes", maxAnswer), TooBig: true}
	}
	if kind := resp.Header.Get(HeaderError); kind != "" {
		tooBig := kind == TooBigKind
		if kind != NotSent && kind != Busy {
			kind = Lost
		}
		detail := ""
		var b struct {
			Detail string `json:"detail"`
		}
		if json.Unmarshal(body, &b) == nil {
			detail = b.Detail
		}
		return nil, &CarryError{Kind: kind, Detail: detail, TooBig: tooBig}
	}
	return &Response{Status: resp.StatusCode, Header: carryHeaders(resp.Header), Body: body}, nil
}
