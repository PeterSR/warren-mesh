package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

// The updater's own control socket (docs/updates.md, "Running it"): HTTP over a unix socket in
// the state directory, mode 0600, for the CLI. It is not a public contract.
//
//	GET  /status
//	POST /stage                              {component, manifest, signature}
//	PUT  /stage/<component>/<version>/<sha>  the bytes of one file
//	POST /install                            InstallRequest
//	POST /revert                             {component}
//	POST /reload

const maxJSON = 64 << 10

// StageRequest is the body of POST /stage. The manifest and its signature are
// text.
type StageRequest struct {
	Component string `json:"component"`
	Manifest  string `json:"manifest"`
	Signature string `json:"signature"`
}

type revertRequest struct {
	Component string `json:"component"`
}

// Handler serves the updater's socket API. reload re-reads the configuration.
func (e *Engine) Handler(reload func() error) http.Handler {
	mux := http.NewServeMux()
	reply := func(w http.ResponseWriter, v any, err error) {
		if err != nil {
			var ref *Refusal
			code := http.StatusInternalServerError
			if errors.As(err, &ref) {
				code = http.StatusConflict
			}
			http.Error(w, err.Error(), code)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	decode := func(w http.ResponseWriter, r *http.Request, v any) bool {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxJSON))
		dec.DisallowUnknownFields()
		if err := dec.Decode(v); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return false
		}
		return true
	}
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		reply(w, e.Status(r.Context()), nil)
	})
	mux.HandleFunc("POST /stage", func(w http.ResponseWriter, r *http.Request) {
		var req StageRequest
		if !decode(w, r, &req) {
			return
		}
		res, err := e.Stage(req.Component, []byte(req.Manifest), []byte(req.Signature))
		reply(w, res, err)
	})
	mux.HandleFunc("PUT /stage/{component}/{version}/{sha}", func(w http.ResponseWriter, r *http.Request) {
		err := e.ReceiveFile(r.PathValue("component"), r.PathValue("version"), r.PathValue("sha"), r.Body)
		reply(w, map[string]bool{"ok": true}, err)
	})
	mux.HandleFunc("POST /install", func(w http.ResponseWriter, r *http.Request) {
		var req InstallRequest
		if !decode(w, r, &req) {
			return
		}
		res, err := e.Install(context.WithoutCancel(r.Context()), req)
		reply(w, res, err)
	})
	mux.HandleFunc("POST /revert", func(w http.ResponseWriter, r *http.Request) {
		var req revertRequest
		if !decode(w, r, &req) {
			return
		}
		res, err := e.Revert(context.WithoutCancel(r.Context()), req.Component)
		reply(w, res, err)
	})
	mux.HandleFunc("POST /reload", func(w http.ResponseWriter, r *http.Request) {
		err := reload()
		reply(w, map[string]bool{"ok": true}, err)
	})
	return mux
}

// Listen makes the socket at path (mode 0600) and serves h on it. Only the
// user running the updater, or root, may connect (SO_PEERCRED, where the
// system has it; a connection whose credentials cannot be read is refused). A socket file left by an earlier run is removed: the caller
// holds update.lock, so it is not in use.
func Listen(path string, h http.Handler) (*http.Server, error) {
	if fi, err := os.Lstat(path); err == nil && fi.Mode()&os.ModeSocket != 0 {
		_ = os.Remove(path)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(&checkedListener{Listener: ln}) }()
	return srv, nil
}

type checkedListener struct{ net.Listener }

func (l *checkedListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if !peerAllowed(c) {
			c.Close()
			continue
		}
		return c, nil
	}
}

// ---- the CLI's side ----

// Client talks to a running updater.
type Client struct{ hc *http.Client }

// NewClient makes a client for the updater socket at path. There is no total
// timeout: uploads stream.
func NewClient(path string) *Client {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", path)
		},
		DisableKeepAlives: true,
	}
	return &Client{hc: &http.Client{Transport: tr}}
}

func (c *Client) do(ctx context.Context, method, uri string, body io.Reader, size int64, in, out any) error {
	var rd io.Reader = body
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		rd = bytesReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://updater"+uri, rd)
	if err != nil {
		return err
	}
	if size > 0 {
		req.ContentLength = size
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("cannot reach the updater (is `warren-update run` running under Warren?): %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return &RemoteError{Status: resp.StatusCode, Msg: string(trimNL(b))}
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

// RemoteError is an error answer of the updater.
type RemoteError struct {
	Status int
	Msg    string
}

func (e *RemoteError) Error() string { return e.Msg }

func trimNL(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// Status reads the status.
func (c *Client) Status(ctx context.Context) (*NodeStatus, error) {
	var s NodeStatus
	if err := c.do(ctx, http.MethodGet, "/status", nil, 0, nil, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Reload asks the updater to read its config again.
func (c *Client) Reload(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/reload", nil, 0, nil, nil)
}

// Revert asks for a manual revert.
func (c *Client) Revert(ctx context.Context, component string) (*InstallResult, error) {
	var r InstallResult
	if err := c.do(ctx, http.MethodPost, "/revert", nil, 0, revertRequest{Component: component}, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Install uploads a candidate read from a source directory (the CLI reads it
// with its own rights; the daemon verifies and is the authority) and installs
// it.
func (c *Client) Install(ctx context.Context, cand *Candidate, req InstallRequest) (*InstallResult, error) {
	m := cand.Manifest
	var st StageResult
	if err := c.do(ctx, http.MethodPost, "/stage", nil, 0, StageRequest{Component: m.Component, Manifest: string(cand.Data), Signature: string(cand.Sig)}, &st); err != nil {
		return nil, err
	}
	files := m.Platforms[Platform].Files
	for _, n := range st.Need {
		f := files[n.Target]
		src, err := OpenFile(cand.Dir, f.Name, f.Size, f.SHA256)
		if err != nil {
			return nil, fmt.Errorf("source file for %s: %w", n.Target, err)
		}
		err = c.do(ctx, http.MethodPut, "/stage/"+m.Component+"/"+m.Version+"/"+n.SHA256, src, n.Size, nil, nil)
		src.Close()
		if err != nil {
			return nil, err
		}
	}
	req.Component, req.Version = m.Component, m.Version
	var res InstallResult
	if err := c.do(ctx, http.MethodPost, "/install", nil, 0, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
