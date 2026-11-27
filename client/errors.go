package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// The error codes of the service API, from the X-Warren-Error header.
const (
	CodeBadRequest    = "bad-request"
	CodeTooLarge      = "too-large"
	CodeRefused       = "refused"
	CodeNoTarget      = "no-target"
	CodeNoService     = "no-service"
	CodeVersion       = "version"
	CodeAlreadyServed = "already-served"
	CodeGone          = "gone"
	CodeBusy          = "busy"
	CodeOffline       = "offline"
	CodeNotReady      = "not-ready"
	CodeUnknown       = "unknown"
	CodeTimeout       = "timeout"
	CodeUnsupported   = "unsupported"
)

// Error is a failure the server reported: a non-2xx status, the code from
// X-Warren-Error and the (cleaned, capped) detail.
type Error struct {
	// Code is one of the Code constants. A code this client does not know
	// (a newer Warren) is kept as sent.
	Code   string
	Detail string
	// Status is the HTTP status.
	Status int
	// Err is the local cause, for an error the client made itself, such as a
	// call that was sent and then lost to a cancelled context: errors.Is(err,
	// context.DeadlineExceeded) works through it. It is nil for an error the
	// server reported.
	Err error
}

func (e *Error) Error() string {
	s := "warren: " + e.Code
	if e.Code == "" {
		s = fmt.Sprintf("warren: HTTP %d", e.Status)
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// Unwrap returns Err, the local cause, if there is one.
func (e *Error) Unwrap() error { return e.Err }

// Is makes errors.Is(err, &client.Error{Code: client.CodeBusy}) match any
// error with that code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code != "" && t.Code == e.Code
}

// Advice is what the error table tells a caller to do about a code.
type Advice int

const (
	// AdviceFixCall: the call is malformed or missing something (bad-request).
	AdviceFixCall Advice = iota
	// AdviceSplit: a body is over its limit (too-large).
	AdviceSplit
	// AdviceGiveUp: do not try again (refused, version).
	AdviceGiveUp
	// AdviceGiveUpOrLater: the target is not known now; give up, or try again
	// later (no-target).
	AdviceGiveUpOrLater
	// AdviceRetryLater: try again later (no-service).
	AdviceRetryLater
	// AdviceRetryWithBackoff: try again, backing off (busy, offline).
	AdviceRetryWithBackoff
	// AdviceRetryShortly: try again soon (not-ready).
	AdviceRetryShortly
	// AdviceWaitOrTakeOver: another lease serves this socket; wait, or take over
	// (already-served).
	AdviceWaitOrTakeOver
	// AdviceDrop: the lease, poll or call has ended; drop it (gone).
	AdviceDrop
	// AdviceRetryIfIdempotent: the call was sent and its outcome is unknown; retry
	// only if repeating it is harmless (unknown, timeout).
	AdviceRetryIfIdempotent
	// AdviceDoWithout: this Warren lacks the endpoint (unsupported).
	AdviceDoWithout
)

// Advice is the "caller should" column of the error table for this code. A
// code that is not in the table (from a newer Warren) is AdviceGiveUp.
func (e *Error) Advice() Advice {
	switch e.Code {
	case CodeBadRequest:
		return AdviceFixCall
	case CodeTooLarge:
		return AdviceSplit
	case CodeRefused, CodeVersion:
		return AdviceGiveUp
	case CodeNoTarget:
		return AdviceGiveUpOrLater
	case CodeNoService:
		return AdviceRetryLater
	case CodeBusy, CodeOffline:
		return AdviceRetryWithBackoff
	case CodeNotReady:
		return AdviceRetryShortly
	case CodeAlreadyServed:
		return AdviceWaitOrTakeOver
	case CodeGone:
		return AdviceDrop
	case CodeUnknown, CodeTimeout:
		return AdviceRetryIfIdempotent
	case CodeUnsupported:
		return AdviceDoWithout
	}
	return AdviceGiveUp
}

// Retryable reports whether the same request may simply be sent again,
// later: busy, offline, not-ready and no-service all mean the call did not
// happen. no-target is left to the caller (it may be a typo), and unknown
// and timeout may have happened (see MaybeDelivered).
func (e *Error) Retryable() bool {
	switch e.Advice() {
	case AdviceRetryLater, AdviceRetryWithBackoff, AdviceRetryShortly:
		return true
	}
	return false
}

// MaybeDelivered reports whether the call may have reached its target and
// its outcome is unknown (unknown, timeout): retry only if idempotent.
func (e *Error) MaybeDelivered() bool { return e.Advice() == AdviceRetryIfIdempotent }

// AsError finds the *Error in err's chain.
func AsError(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// Code is the code of err, or "" when err is not a server error.
func Code(err error) string {
	if e, ok := AsError(err); ok {
		return e.Code
	}
	return ""
}

// IsCode reports whether err is a server error with one of the codes.
func IsCode(err error, codes ...string) bool {
	c := Code(err)
	if c == "" {
		return false
	}
	return slices.Contains(codes, c)
}

// IsRetryable reports whether err is a server error that says the request
// did not happen and may be sent again later (see Error.Retryable).
func IsRetryable(err error) bool {
	e, ok := AsError(err)
	return ok && e.Retryable()
}

// IsMaybeDelivered reports whether err leaves the outcome of a call
// unknown (see Error.MaybeDelivered).
func IsMaybeDelivered(err error) bool {
	e, ok := AsError(err)
	return ok && e.MaybeDelivered()
}

// errorFrom reads the error of a non-2xx response.
func errorFrom(resp *http.Response, body []byte) *Error {
	e := &Error{Status: resp.StatusCode, Code: resp.Header.Get("X-Warren-Error")}
	var b struct {
		Error  string `json:"error"`
		Detail string `json:"detail"`
	}
	if json.Unmarshal(body, &b) == nil {
		e.Detail = b.Detail
		if e.Code == "" {
			e.Code = b.Error
		}
	} else if e.Code == "" {
		// Not a Warren answer (something else is listening): keep a bit of it.
		e.Detail = strings.TrimSpace(string(body[:min(len(body), 200)]))
	}
	return e
}
