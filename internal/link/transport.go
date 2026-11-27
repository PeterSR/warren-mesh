package link

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/grammar"
)

// Backend is what a transport does; TransportHandler turns it into the
// /v1/transport API. Real transports and test transports use it, so the
// wire is written once.
type Backend interface {
	Hello(ctx context.Context) Hello
	Peers(ctxDone <-chan struct{}, since string, wait time.Duration) PeerList
	Carry(r *http.Request, to string, req *Request) (*Response, error)
}

// TransportHandler is the transport's side of link A.
func TransportHandler(b Backend) http.Handler {
	// max_body is asked of the backend once, not per carried request.
	var mu sync.Mutex
	cached := int64(0)
	maxBody := func(ctx context.Context) int64 {
		mu.Lock()
		defer mu.Unlock()
		if cached == 0 {
			cached = int64(b.Hello(ctx).MaxBody)
		}
		return cached
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/transport/hello", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, b.Hello(r.Context()))
	})
	mux.HandleFunc("GET /v1/transport/peers", func(w http.ResponseWriter, r *http.Request) {
		wait := grammar.MaxWaitSeconds
		if s := r.URL.Query().Get("wait"); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n < 0 || n > grammar.MaxWaitSeconds {
				apierr.Write(w, apierr.BadRequest, fmt.Sprintf("wait is whole seconds, 0 to %d", grammar.MaxWaitSeconds))
				return
			}
			wait = n
		}
		writeJSON(w, b.Peers(r.Context().Done(), r.URL.Query().Get("since"), time.Duration(wait)*time.Second))
	})
	carry := func(w http.ResponseWriter, r *http.Request) {
		to := r.Header.Get(HeaderTo)
		if !validNode(to) {
			apierr.Write(w, apierr.BadRequest, "X-Warren-Transport-To must name a node")
			return
		}
		max := maxBody(r.Context())
		if r.ContentLength > max {
			notSent(w, http.StatusRequestEntityTooLarge, "request over max_body")
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, max+1))
		if err != nil || int64(len(body)) > max {
			notSent(w, http.StatusRequestEntityTooLarge, "request over max_body")
			return
		}
		req := &Request{Method: r.Method, URI: r.URL.RequestURI(), Header: carryHeaders(r.Header), Body: body}
		if ms, err := strconv.Atoi(r.Header.Get(HeaderDeadline)); err == nil && ms > 0 {
			req.Deadline = time.Now().Add(time.Duration(min(ms, 3_600_000)) * time.Millisecond) // clamped: no overflow
		}
		resp, err := b.Carry(r, to, req)
		if err != nil {
			kind, detail := Lost, err.Error()
			if ce, ok := err.(*CarryError); ok {
				kind, detail = ce.Kind, ce.Detail
			}
			if kind == NotSent {
				notSent(w, http.StatusBadGateway, detail)
				return
			}
			if kind == Busy {
				w.Header().Set(HeaderError, Busy)
				writeJSONStatus(w, http.StatusTooManyRequests, map[string]string{"error": Busy, "detail": detail})
				return
			}
			if ce, ok := err.(*CarryError); ok && ce.TooBig {
				w.Header().Set(HeaderError, TooBigKind)
				writeJSONStatus(w, http.StatusBadGateway, map[string]string{"error": TooBigKind, "detail": detail})
				return
			}
			w.Header().Set(HeaderError, Lost)
			writeJSONStatus(w, http.StatusBadGateway, map[string]string{"error": Lost, "detail": detail})
			return
		}
		for k, v := range carryHeaders(resp.Header) {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.Status)
		_, _ = w.Write(resp.Body)
	}
	// Peer paths of any protocol version are carried; the rest is unknown.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if IsPeerPath(r.URL.Path) {
			carry(w, r)
			return
		}
		apierr.NotFound(w, r)
	})
	return mux
}

func notSent(w http.ResponseWriter, status int, detail string) {
	w.Header().Set(HeaderError, NotSent)
	writeJSONStatus(w, status, map[string]string{"error": NotSent, "detail": detail})
}

func writeJSON(w http.ResponseWriter, v any) { writeJSONStatus(w, http.StatusOK, v) }

func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// carriedHeaders is for tests: reports whether h carries any hop header.
func carriedHeaders(h http.Header) bool {
	for k := range h {
		if strings.HasPrefix(http.CanonicalHeaderKey(k), PrefixTransport) {
			return true
		}
	}
	return false
}
