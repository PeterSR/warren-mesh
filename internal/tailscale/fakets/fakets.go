// Package fakets is a fake tailscaled local API for tests and the transport
// conformance harness: a tailnet of named devices on loopback addresses, each
// with a unix socket that answers the two calls warren-tailscale makes
// (status and whois) from that device's point of view. No real tailscaled is
// involved.
package fakets

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// Suffix is the MagicDNS suffix of the fake tailnet.
const Suffix = "tail1.ts.net"

// Tag is the tag the devices carry unless a test says otherwise.
const Tag = "tag:warren-test"

// Device is one machine on the tailnet.
type Device struct {
	Name   string
	IP     string
	Tags   []string
	Online bool
}

// Net is a fake tailnet.
type Net struct {
	dir string

	mu      sync.Mutex
	devices map[string]*Device
	servers map[string]*http.Server
	paths   map[string]string
	next    int
}

// New makes an empty tailnet whose sockets live in dir.
func New(dir string) *Net {
	return &Net{dir: dir, devices: map[string]*Device{}, servers: map[string]*http.Server{}, paths: map[string]string{}, next: 10}
}

// Join adds an online device with a fresh loopback address (127.0.0.N) and
// the default tag, and serves its local API. It returns the device and the
// path of its socket.
func (n *Net) Join(name string) (Device, string, error) {
	return n.JoinTagged(name, []string{Tag})
}

// JoinTagged is Join with the tags the device carries (none: an untagged
// device, which the transports must not vouch for).
func (n *Net) JoinTagged(name string, tags []string) (Device, string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, dup := n.devices[name]; dup {
		return Device{}, "", fmt.Errorf("device %s joined twice", name)
	}
	d := &Device{Name: name, IP: fmt.Sprintf("127.0.0.%d", n.next), Tags: slices.Clone(tags), Online: true}
	n.next++
	path := filepath.Join(n.dir, "ts-"+name+".sock")
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return Device{}, "", err
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { n.serve(name, w, r) })}
	go func() { _ = srv.Serve(ln) }()
	n.devices[name], n.servers[name], n.paths[name] = d, srv, path
	return *d, path, nil
}

// Set changes a device: its tags and whether it is online.
func (n *Net) Set(name string, f func(d *Device)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if d := n.devices[name]; d != nil {
		f(d)
	}
}

// Close stops every local API.
func (n *Net) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, s := range n.servers {
		s.Close()
	}
}

func dns(d *Device) string { return d.Name + "." + Suffix + "." }

// serve answers one device's local API calls.
func (n *Net) serve(self string, w http.ResponseWriter, r *http.Request) {
	// tailscaled refuses local API requests without this header.
	if r.Header.Get("Sec-Tailscale") != "localapi" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	me := n.devices[self]
	switch r.URL.Path {
	case "/localapi/v0/status":
		peers := map[string]any{}
		for name, d := range n.devices {
			if name != self {
				peers["key-"+name] = map[string]any{"DNSName": dns(d), "TailscaleIPs": []string{d.IP, "fd7a::1"}, "Online": d.Online, "Tags": d.Tags}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"BackendState": "Running", "MagicDNSSuffix": Suffix,
			"Self": map[string]any{"DNSName": dns(me), "TailscaleIPs": []string{"fd7a::2", me.IP}, "Online": true, "Tags": me.Tags}, "Peer": peers})
	case "/localapi/v0/whois":
		host, _, _ := net.SplitHostPort(r.URL.Query().Get("addr"))
		for _, d := range n.devices {
			if d.IP == host {
				_ = json.NewEncoder(w).Encode(map[string]any{"Node": map[string]any{"Name": dns(d), "Tags": slices.Clone(d.Tags)}})
				return
			}
		}
		http.Error(w, "no such device", http.StatusNotFound)
	default:
		http.NotFound(w, r)
	}
}
