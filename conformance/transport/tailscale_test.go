package transport_test

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/conformance/transport"
	"github.com/PeterSR/warren-mesh/internal/tailscale/fakets"
)

// tsNetwork is a tailnet of loopback addresses with a fake tailscaled for each
// node, for the warren-tailscale binary.
type tsNetwork struct {
	bin  string
	port int
	net  *fakets.Net

	mu  sync.Mutex
	ips map[string]string
}

func newTSNetwork(t *testing.T, bin string) *tsNetwork {
	dir, err := os.MkdirTemp("", "wts")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	// One port for every node: each listens on its own loopback address.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	n := &tsNetwork{bin: bin, port: port, net: fakets.New(dir), ips: map[string]string{}}
	t.Cleanup(n.net.Close)
	return n
}

// Join implements transport.Network. It may be called from several checks at once.
func (n *tsNetwork) Join(t testing.TB, names ...string) []transport.Spec {
	n.mu.Lock()
	defer n.mu.Unlock()
	var specs []transport.Spec
	for _, name := range names {
		dev, sock, err := n.net.Join(name)
		if err != nil {
			t.Fatal(err)
		}
		n.ips[name] = dev.IP
		settings, _ := json.Marshal(map[string]any{"tag": "warren-test", "port": n.port, "socket": sock})
		specs = append(specs, transport.Spec{Argv: []string{n.bin}, Env: []string{"WARREN_TRANSPORT_SETTINGS=" + string(settings)}})
	}
	return specs
}

// Leave implements transport.Leaver: the device goes offline.
func (n *tsNetwork) Leave(t testing.TB, name string) {
	n.net.Set(name, func(d *fakets.Device) { d.Online = false })
}

// Inject implements transport.Injector: a plain HTTP request to the
// transport of `to`, from the address of `from`, as warren-tailscale's own
// peers send theirs.
func (n *tsNetwork) Inject(t testing.TB, from, to string, req *http.Request) (*http.Response, error) {
	n.mu.Lock()
	fromIP, toIP := n.ips[from], n.ips[to]
	n.mu.Unlock()
	hc := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{DisableKeepAlives: true, Proxy: nil,
		DialContext: (&net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(fromIP)}}).DialContext}}
	req.URL.Scheme = "http"
	req.URL.Host = net.JoinHostPort(toIP, fmt.Sprint(n.port))
	req.Host = req.URL.Host
	return hc.Do(req)
}

// Stranger implements transport.Strangers: a device on the tailnet that
// carries no tag, so it is not vouched for.
func (n *tsNetwork) Stranger(t testing.TB, to string, req *http.Request) (*http.Response, string, error) {
	n.mu.Lock()
	if _, ok := n.ips["stranger"]; !ok {
		dev, _, err := n.net.JoinTagged("stranger", nil)
		if err != nil {
			n.mu.Unlock()
			t.Fatal(err)
		}
		n.ips["stranger"] = dev.IP
	}
	n.mu.Unlock()
	resp, err := n.Inject(t, "stranger", to, req)
	return resp, "stranger", err
}

// TestWarrenTailscale runs the harness against cmd/warren-tailscale built
// from this repository.
func TestWarrenTailscale(t *testing.T) {
	dir, err := os.MkdirTemp("", "wtsbin")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	exe := filepath.Join(dir, "warren-tailscale")
	if out, err := exec.Command("go", "build", "-o", exe, "github.com/PeterSR/warren-mesh/cmd/warren-tailscale").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	// Burst is above warren-tailscale's limit for requests from one node (128).
	transport.Run(t, newTSNetwork(t, exe), transport.Options{Name: "tailscale", Burst: 400})
}
