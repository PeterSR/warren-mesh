// Package apierr is how every Warren API (service, transport, peer) reports
// a failure: a non-2xx status, the code in X-Warren-Error and a small JSON
// body. The codes and statuses are the table in the service API.
package apierr

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/PeterSR/warren-mesh/internal/grammar"
)

// Codes.
const (
	BadRequest    = "bad-request"
	TooLarge      = "too-large"
	Refused       = "refused"
	NoTarget      = "no-target"
	NoService     = "no-service"
	Version       = "version"
	AlreadyServed = "already-served"
	Gone          = "gone"
	Busy          = "busy"
	Offline       = "offline"
	NotReady      = "not-ready"
	Unknown       = "unknown"
	Timeout       = "timeout"
	Unsupported   = "unsupported"
)

// Header carries the code.
const Header = "X-Warren-Error"

var statuses = map[string]int{
	BadRequest:    http.StatusBadRequest,
	TooLarge:      http.StatusRequestEntityTooLarge,
	Refused:       http.StatusForbidden,
	NoTarget:      http.StatusNotFound,
	NoService:     http.StatusNotFound,
	Version:       http.StatusConflict,
	AlreadyServed: http.StatusConflict,
	Gone:          http.StatusGone,
	Busy:          http.StatusTooManyRequests,
	Offline:       http.StatusServiceUnavailable,
	NotReady:      http.StatusServiceUnavailable,
	Unknown:       http.StatusBadGateway,
	Timeout:       http.StatusGatewayTimeout,
	Unsupported:   http.StatusNotFound,
}

// Status is the HTTP status of a code.
func Status(code string) int {
	if s, ok := statuses[code]; ok {
		return s
	}
	return http.StatusBadGateway
}

// Codes lists every code, for tests and docs.
func Codes() []string {
	var c []string
	for k := range statuses {
		c = append(c, k)
	}
	return c
}

// Error is an API failure.
type Error struct {
	Code   string
	Detail string
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return e.Code
	}
	return e.Code + ": " + e.Detail
}

// New makes an Error; detail is cleaned and capped.
func New(code, detail string) *Error {
	return &Error{Code: code, Detail: grammar.CleanDetail(detail)}
}

// Write answers w with the error.
func Write(w http.ResponseWriter, code, detail string) {
	body, _ := json.Marshal(map[string]string{"error": code, "detail": grammar.CleanDetail(detail)})
	h := w.Header()
	h.Set(Header, code)
	h.Set("Content-Type", "application/json")
	w.WriteHeader(Status(code))
	_, _ = w.Write(body)
}

// WriteErr answers w with err, or an `unknown` for an error that is not an
// *Error.
func WriteErr(w http.ResponseWriter, err error) {
	var e *Error
	if errors.As(err, &e) {
		Write(w, e.Code, e.Detail)
		return
	}
	Write(w, Unknown, err.Error())
}

// FromResponse reads the error a remote Warren sent, if the response is
// one. Detail is cleaned and capped again, as at every hop.
func FromResponse(resp *http.Response, body []byte) *Error {
	code := resp.Header.Get(Header)
	if code == "" {
		return nil
	}
	var b struct {
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(body, &b)
	return New(code, b.Detail)
}

// NotFound is the answer to any path an API does not have. It is frozen in
// v1, so newer parties can tell "not supported" from no-target.
func NotFound(w http.ResponseWriter, r *http.Request) {
	Write(w, Unsupported, "no such endpoint: "+r.Method+" "+r.URL.Path)
}

// Known reports whether code is one of the codes of the table.
func Known(code string) bool {
	_, ok := statuses[code]
	return ok
}
