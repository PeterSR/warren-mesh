package tailscale

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/config"
	"github.com/PeterSR/warren-mesh/internal/service"
)

// A whole path: two Warren nodes, each with its link to a warren-tailscale
// transport (in this process, on socket pairs), the transports on loopback
// addresses with fake tailscaled local APIs. A service on one node publishes
// an entry, a child of the other node sees it and calls it.

type e2eNode struct {
	n    *service.Node
	dir  string
	node *node
}

func e2eStart(t *testing.T, tn *node) *e2eNode {
	d, err := os.MkdirTemp("", "e2e")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	y := strings.ReplaceAll(`
node: NAME
state_dir: DIR/state
services:
  dir: DIR/s
  list:
    - name: echo
      allow: ["*"]
attach:
  dir: DIR/a
  children:
    - name: kit
      services:
        - name: echo
          allow: ["*"]
`, "DIR", d)
	y = strings.ReplaceAll(y, "NAME", tn.name)
	os.MkdirAll(filepath.Join(d, "a", "kit"), 0o755)
	cfg, err := config.Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	node, err := service.New(service.Options{Config: cfg, Link: tn.w, Log: quiet(), TransportName: "tailscale"})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.Close)
	return &e2eNode{n: node, dir: d, node: tn}
}

func sockClient(path string) *http.Client {
	return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		}}}
}

func api(t *testing.T, c *http.Client, method, pq, body string) (int, string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, "http://warren"+pq, strings.NewReader(body))
	req.Header.Set("X-Warren", "1")
	r, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return r.StatusCode, string(b), r.Header
}

func TestTwoWarrensOverTwoTransports(t *testing.T) {
	a, b := pair(t)
	// Warren's own timings, short.
	sa, sb := service.PeerDebounce, service.PollGap
	service.PeerDebounce, service.PollGap = 30*time.Millisecond, 10*time.Millisecond
	// Restored after the nodes (cleanups run last in, first out) have stopped.
	t.Cleanup(func() { service.PeerDebounce, service.PollGap = sa, sb })
	ea, eb := e2eStart(t, a), e2eStart(t, b)

	// The service on a: a lease, an entry, and an echo loop.
	echo := sockClient(filepath.Join(ea.dir, "s", "echo.sock"))
	var lease struct{ Lease string }
	eventually(t, "a's echo socket", func() bool {
		_, body, _ := apiTry(echo, "POST", "/v1/service/serve?version=1&take=1", "")
		return json.Unmarshal([]byte(body), &lease) == nil && lease.Lease != ""
	})
	st, body, _ := api(t, echo, "PUT", "/v1/service/entries?lease="+lease.Lease, `{"entries":[{"name":"alice","data":{"x":1}}]}`)
	if st != 200 {
		t.Fatalf("PUT: %d %s", st, body)
	}
	go func() {
		for {
			req, _ := http.NewRequest("GET", "http://warren/v1/service/calls?lease="+lease.Lease+"&wait=1", nil)
			req.Header.Set("X-Warren", "1")
			r, err := echo.Do(req)
			if err != nil {
				return
			}
			bd, _ := io.ReadAll(r.Body)
			r.Body.Close()
			if r.StatusCode != 200 {
				if r.StatusCode != 204 {
					return
				}
				continue
			}
			reply := fmt.Sprintf("origin=%s from=%s body=%s", r.Header.Get("X-Warren-Origin"), r.Header.Get("X-Warren-From"), bd)
			ans, _ := http.NewRequest("POST", "http://warren/v1/service/answer?lease="+lease.Lease+"&id="+r.Header.Get("X-Warren-Call"), strings.NewReader(reply))
			ans.Header.Set("X-Warren", "1")
			if ar, err := echo.Do(ans); err == nil {
				ar.Body.Close()
			}
		}
	}()

	// A child of b sees the entry, with its origin, and calls it.
	kit := sockClient(filepath.Join(eb.dir, "a", "kit", "echo.sock"))
	eventually(t, "b's child sees alice@a", func() bool {
		_, body, _ := apiTry(kit, "GET", "/v1/service/view?wait=0", "")
		return strings.Contains(body, `"address":"alice@a"`) && strings.Contains(body, `"origin":"a"`)
	})
	var kl struct{ Lease string }
	_, body, _ = api(t, kit, "POST", "/v1/service/serve?version=1", "")
	json.Unmarshal([]byte(body), &kl)
	st, body, _ = api(t, kit, "POST", "/v1/service/call?to=alice@a&method=hi&version=1&from=bob", "hello")
	if st != 200 || body != "origin=kit.b from=bob@kit.b body=hello" {
		t.Fatalf("call: %d %q", st, body)
	}
	// A call to a node nobody knows is no-target; the transport that is not
	// there for a listed peer is offline.
	st, _, h := api(t, kit, "POST", "/v1/service/call?to=alice@zzz&method=hi&version=1", "")
	if st != 404 || h.Get("X-Warren-Error") != "no-target" {
		t.Errorf("unknown node: %d %v", st, h)
	}
	// The peers see each other as up, with the version of the other.
	for _, e := range []*e2eNode{ea, eb} {
		e := e
		var peers []map[string]any
		eventually(t, e.node.name+" to see its peer up", func() bool {
			peers = e.n.Status()["peers"].([]map[string]any)
			return len(peers) == 1 && peers[0]["up"] == true && peers[0]["version"] == service.Version
		})
	}
}

func apiTry(c *http.Client, method, pq, body string) (int, string, http.Header) {
	req, _ := http.NewRequest(method, "http://warren"+pq, strings.NewReader(body))
	req.Header.Set("X-Warren", "1")
	r, err := c.Do(req)
	if err != nil {
		return 0, "", nil
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return r.StatusCode, string(b), r.Header
}
