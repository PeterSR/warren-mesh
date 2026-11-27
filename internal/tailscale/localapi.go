// Package tailscale is the warren-tailscale transport: it carries the peer
// protocol between Warren nodes over a tailnet and vouches for who is calling.
// It speaks /v1/transport to Warren and plain HTTP to other transports, and
// uses tailscaled's local API over its unix socket (plain HTTP, no Tailscale
// library) to learn who it is, who its peers are, and who a caller is.
package tailscale

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/PeterSR/warren-mesh/internal/grammar"
)

// DefaultSocket is where tailscaled serves its local API on Linux.
const DefaultSocket = "/var/run/tailscale/tailscaled.sock"

// device is a device on the tailnet. Name is its machine name, the first
// label of its MagicDNS name, which is unique on the tailnet and which only
// the tailnet's admins can change. It is the device's node name.
type device struct {
	IP   string
	Name string
}

// localAPI talks to tailscaled's local API over its unix socket. Only the
// read-only status and whois calls are used, which tailscaled answers for
// any local user, so the transport needs no Tailscale privileges.
type localAPI struct {
	hc *http.Client

	mu     sync.Mutex
	suffix string // this tailnet's MagicDNS suffix, from the last status
}

func newLocalAPI(socket string) *localAPI {
	if socket == "" {
		socket = DefaultSocket
	}
	return &localAPI{hc: &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		}},
	}}
}

func (l *localAPI) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, "GET", "http://local-tailscaled.sock/localapi/v0/"+path, nil)
	if err != nil {
		return err
	}
	// tailscaled refuses local API requests without this header, as
	// protection against browsers.
	req.Header.Set("Sec-Tailscale", "localapi")
	resp, err := l.hc.Do(req)
	if err != nil {
		return fmt.Errorf("tailscale local API: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("tailscale local API %s: %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type tsStatus struct {
	BackendState   string
	MagicDNSSuffix string
	Self           tsPeerStatus
	Peer           map[string]tsPeerStatus
}

type tsPeerStatus struct {
	DNSName      string
	TailscaleIPs []string
	Online       bool
	Tags         []string
}

func (l *localAPI) status(ctx context.Context) (tsStatus, error) {
	var st tsStatus
	if err := l.get(ctx, "status", &st); err != nil {
		return st, err
	}
	if st.BackendState != "Running" {
		return st, fmt.Errorf("tailscale is %s, not Running", st.BackendState)
	}
	if st.MagicDNSSuffix == "" {
		return st, fmt.Errorf("tailscale reports no MagicDNS suffix")
	}
	l.mu.Lock()
	l.suffix = st.MagicDNSSuffix
	l.mu.Unlock()
	return st, nil
}

// snapshot is this device and the online devices that carry tag.
func (l *localAPI) snapshot(ctx context.Context, tag string) (self device, peers []device, err error) {
	st, err := l.status(ctx)
	if err != nil {
		return device{}, nil, err
	}
	self = device{IP: firstIPv4(st.Self.TailscaleIPs), Name: machineName(st.Self.DNSName, st.MagicDNSSuffix)}
	if self.IP == "" || self.Name == "" {
		return device{}, nil, fmt.Errorf("this device has no tailnet IPv4 address or a machine name that is a node name")
	}
	for _, p := range st.Peer {
		if !p.Online || !slices.Contains(p.Tags, tag) {
			continue
		}
		// A device shared in from another tailnet has that tailnet's
		// suffix, and tags its own admins chose: never a peer.
		d := device{IP: firstIPv4(p.TailscaleIPs), Name: machineName(p.DNSName, st.MagicDNSSuffix)}
		if d.IP != "" && d.Name != "" {
			peers = append(peers, d)
		}
	}
	return self, peers, nil
}

// whois is the device at ip: its machine name, and whether it carries tag.
func (l *localAPI) whois(ctx context.Context, ip, tag string) (name string, tagged bool, err error) {
	var who struct {
		Node struct {
			Name string
			Tags []string
		}
	}
	// whois wants an ip:port; any port names the same device.
	if err := l.get(ctx, "whois?addr="+url.QueryEscape(net.JoinHostPort(ip, "1")), &who); err != nil {
		return "", false, err
	}
	l.mu.Lock()
	suffix := l.suffix
	l.mu.Unlock()
	if suffix == "" {
		return "", false, fmt.Errorf("tailnet suffix not known yet")
	}
	name = machineName(who.Node.Name, suffix)
	return name, name != "" && slices.Contains(who.Node.Tags, tag), nil
}

// machineName turns a MagicDNS name in our tailnet ("server.tail1234.ts.net.")
// into a node name ("server"). A name in another tailnet, or one that is not
// a Warren label, gives "".
func machineName(dns, suffix string) string {
	label, rest, ok := strings.Cut(strings.TrimSuffix(dns, "."), ".")
	if !ok || suffix == "" || !strings.EqualFold(rest, suffix) || !grammar.Label(label) {
		return ""
	}
	return label
}

// isLocalIP reports whether ip is one of this network namespace's own
// addresses (any of 127/8 counts, as Linux answers on all of them).
func isLocalIP(ip string) bool {
	want := net.ParseIP(ip)
	addrs, err := net.InterfaceAddrs()
	if want == nil || err != nil {
		return false
	}
	for _, a := range addrs {
		n, ok := a.(*net.IPNet)
		if ok && (n.IP.Equal(want) || (n.IP.IsLoopback() && n.Contains(want))) {
			return true
		}
	}
	return false
}

func firstIPv4(ips []string) string {
	for _, s := range ips {
		if a, err := netip.ParseAddr(s); err == nil && a.Is4() {
			return a.String()
		}
	}
	return ""
}

// normaliseTag gives the tag as tailscaled reports it: "tag:name".
func normaliseTag(tag string) string {
	if tag != "" && !strings.HasPrefix(tag, "tag:") {
		return "tag:" + tag
	}
	return tag
}
