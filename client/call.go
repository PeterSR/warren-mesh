package client

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Timeouts of a call, from the contract.
const (
	// DefaultCallTimeout applies when CallRequest.Timeout is zero.
	DefaultCallTimeout = 30 * time.Second
	// MaxCallTimeout is the longest a call may wait for its answer.
	MaxCallTimeout = 300 * time.Second
)

// CallRequest is one call to another service of the same name.
type CallRequest struct {
	// To is a relay id ("sandbox.server") or an entry address
	// ("alice@server").
	To string
	// Method is a label (lower case letters, digits, dash, underscore, at
	// most 63 bytes). Warren never interprets it.
	Method string
	// Version is the major version of the service to call. Required.
	Version int
	// From, if set, is the entry name (or name@namespace) the call is made
	// on behalf of. The socket must be allowed to publish it.
	From string
	// Timeout is how long to wait for the answer: DefaultCallTimeout if zero,
	// at most MaxCallTimeout.
	Timeout time.Duration
	// Body is the request, at most MaxCallBody bytes.
	Body []byte
}

// Call sends one call and returns the target's answer, which is the raw body
// the service answered with. A failure is an *Error with the code of the
// error table (see IsRetryable and IsMaybeDelivered), or a plain error when
// the socket cannot be reached. Delivery is at most once: unknown and
// timeout leave the outcome unknown, so retry those only if the method is
// idempotent. So does a failure after the request was written (the context
// ended, the connection dropped): it is returned as an *Error with the code
// unknown, and IsMaybeDelivered is true for it. A plain error means the
// request was not sent (the socket could not be reached, or the context had
// ended already) and is safe to retry.
func (c *Client) Call(ctx context.Context, r CallRequest) ([]byte, error) {
	if len(r.Body) > MaxCallBody {
		return nil, &Error{Code: CodeTooLarge, Detail: "the body is over 1 MiB"}
	}
	timeout := r.Timeout
	if timeout <= 0 {
		timeout = DefaultCallTimeout
	}
	if timeout < time.Millisecond {
		timeout = time.Millisecond // the wire has whole milliseconds, at least 1
	}
	if timeout > MaxCallTimeout {
		timeout = MaxCallTimeout
	}
	q := url.Values{
		"to":         {r.To},
		"method":     {r.Method},
		"version":    {strconv.Itoa(r.Version)},
		"timeout_ms": {strconv.FormatInt(timeout.Milliseconds(), 10)},
	}
	if r.From != "" {
		q.Set("from", r.From)
	}
	// Warren answers `timeout` itself at the deadline; the extra time is for
	// that answer to arrive.
	resp, sent, err := c.doSent(ctx, http.MethodPost, "/v1/service/call", q, r.Body, timeout+10*time.Second)
	if err != nil {
		if _, isServer := AsError(err); !isServer && sent {
			// The call was written and then the context ended or the
			// connection dropped: the service may have received it, so the
			// outcome is unknown, as for the server's own `unknown`.
			return nil, &Error{Code: CodeUnknown, Detail: "the call was sent and its outcome is lost: " + err.Error(), Err: err}
		}
		return nil, err
	}
	return resp.body, nil
}
