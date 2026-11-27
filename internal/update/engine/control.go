package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

// The updater talks to Warren through the documented control API
// (docs/control-api.md) and nothing else of Warren's: it imports none of
// Warren's internal packages.

// ErrNotFound is a 404: the service is not managed now. A caller treats it as
// "not there at the moment", never as a failure.
var ErrNotFound = errors.New("not found")

// ErrTooOld is an older Warren that has no control API for services.
var ErrTooOld = errors.New("warren too old: needs the control API (GET /services)")

// Lease is the lease part of a service object.
type Lease struct {
	Held  bool      `json:"held"`
	Since time.Time `json:"since"`
	Epoch int       `json:"epoch"`
}

// Service is one managed service as Warren reports it.
type Service struct {
	Name              string    `json:"name"`
	Exe               string    `json:"exe"`
	ExeError          string    `json:"exe_error"`
	Updatable         *bool     `json:"updatable"` // nil: unknown
	NotUpdatable      string    `json:"not_updatable"`
	State             string    `json:"state"`
	PID               int       `json:"pid"`
	StartSeq          int       `json:"start_seq"`
	StartedAt         time.Time `json:"started_at"`
	Starts            int       `json:"starts"`
	Exits             int       `json:"exits"`
	RequestedExits    int       `json:"requested_exits"`
	StartFailures     int       `json:"start_failures"`
	LastExit          string    `json:"last_exit"`
	LastExitRequested bool      `json:"last_exit_requested"`
	LastStartError    string    `json:"last_start_error"`
	Lease             Lease     `json:"lease"`
	Boot              string    `json:"boot"`
}

// Unrequested is the failures nobody asked for: exits that were not
// requested, plus starts that failed.
func (s *Service) Unrequested() int { return s.Exits - s.RequestedExits + s.StartFailures }

// WarrenStatus is the part of Warren's /status the updater reads.
type WarrenStatus struct {
	Boot   string `json:"boot"`
	Warren struct {
		Version      string `json:"version"`
		Updatable    *bool  `json:"updatable"`
		NotUpdatable string `json:"not_updatable"`
	} `json:"warren"`
	Healthy bool   `json:"healthy"`
	Pinned  string `json:"pinned"`
}

// Control is a client of Warren's control socket.
type Control struct {
	hc *http.Client
}

// NewControl makes a client for the control socket at path.
func NewControl(path string) *Control {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		},
		DisableKeepAlives: true,
	}
	return &Control{hc: &http.Client{Transport: tr, Timeout: 30 * time.Second}}
}

func (c *Control) do(ctx context.Context, method, uri string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, "http://warren"+uri, nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("cannot reach Warren's control socket: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return ErrNotFound
	case resp.StatusCode >= 300:
		return fmt.Errorf("warren answered %d: %s", resp.StatusCode, string(body))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("warren's answer for %s is not the expected JSON: %w", uri, err)
	}
	return nil
}

// CheckAPI refuses a Warren that has no service endpoints.
func (c *Control) CheckAPI(ctx context.Context) error {
	var list []Service
	err := c.do(ctx, http.MethodGet, "/services", &list)
	if errors.Is(err, ErrNotFound) {
		return ErrTooOld
	}
	return err
}

// Service reads one managed service; ErrNotFound when it is not managed now.
func (c *Control) Service(ctx context.Context, name string) (*Service, error) {
	var s Service
	if err := c.do(ctx, http.MethodGet, "/services/"+url.PathEscape(name), &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Restart asks Warren to stop the service and start it again at once, and
// returns min_start_seq: every start with that number or higher began after
// the request.
func (c *Control) Restart(ctx context.Context, name string) (int, error) {
	var ans struct {
		Accepted    bool `json:"accepted"`
		MinStartSeq int  `json:"min_start_seq"`
	}
	if err := c.do(ctx, http.MethodPost, "/services/"+url.PathEscape(name)+"/restart", &ans); err != nil {
		return 0, err
	}
	if !ans.Accepted || ans.MinStartSeq < 1 {
		return 0, errors.New("warren did not accept the restart")
	}
	return ans.MinStartSeq, nil
}

// Status reads Warren's /status.
func (c *Control) Status(ctx context.Context) (*WarrenStatus, error) {
	var s WarrenStatus
	if err := c.do(ctx, http.MethodGet, "/status", &s); err != nil {
		return nil, err
	}
	return &s, nil
}
