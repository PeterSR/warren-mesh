package tailscale

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/link"
)

const suffix = "tail1.ts.net"
const testTag = "tag:warren-test"

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	dl := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(dl) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---- a fake tailscaled local API ----

type fakeDev struct {
	Name   string // machine name
	IP     string
	Online bool
	Tags   []string
	Suffix string // MagicDNS suffix of its name; the tailnet's if empty
}

type fakeTS struct {
	t    *testing.T
	path string
	srv  *http.Server

	mu       sync.Mutex
	backend  string
	self     fakeDev
	peers    []fakeDev
	whois    map[string]fakeDev // by ip
	badCalls atomic.Int64       // requests without Sec-Tailscale
	whoises  atomic.Int64
	delay    atomic.Int64 // nanoseconds a whois takes
	calls    atomic.Int64
}

func newFakeTS(t *testing.T, self fakeDev) *fakeTS {
	t.Helper()
	d, err := os.MkdirTemp("", "ts")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	f := &fakeTS{t: t, path: filepath.Join(d, "tailscaled.sock"), backend: "Running", self: self, whois: map[string]fakeDev{}}
	ln, err := net.Listen("unix", f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.srv = &http.Server{Handler: http.HandlerFunc(f.serve)}
	go f.srv.Serve(ln)
	t.Cleanup(func() { f.srv.Close() })
	return f
}

func dns(d fakeDev) string {
	s := d.Suffix
	if s == "" {
		s = suffix
	}
	return d.Name + "." + s + "."
}

func (f *fakeTS) set(fn func(f *fakeTS)) { f.mu.Lock(); fn(f); f.mu.Unlock() }

func (f *fakeTS) serve(w http.ResponseWriter, r *http.Request) {
	f.calls.Add(1)
	if r.Header.Get("Sec-Tailscale") != "localapi" {
		f.badCalls.Add(1)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/localapi/v0/status":
		peers := map[string]any{}
		for i, p := range f.peers {
			peers[fmt.Sprint("key", i)] = map[string]any{"DNSName": dns(p), "TailscaleIPs": []string{p.IP, "fd7a::1"}, "Online": p.Online, "Tags": p.Tags}
		}
		json.NewEncoder(w).Encode(map[string]any{"BackendState": f.backend, "MagicDNSSuffix": suffix,
			"Self": map[string]any{"DNSName": dns(f.self), "TailscaleIPs": []string{"fd7a::2", f.self.IP}, "Online": true}, "Peer": peers})
	case "/localapi/v0/whois":
		f.whoises.Add(1)
		time.Sleep(time.Duration(f.delay.Load()))
		host, _, _ := net.SplitHostPort(r.URL.Query().Get("addr"))
		d, ok := f.whois[host]
		if !ok {
			http.Error(w, "no such device", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"Node": map[string]any{"Name": dns(d), "Tags": d.Tags}})
	default:
		http.NotFound(w, r)
	}
}

// ---- a node: transport, fake tailscaled and a Warren side ----

type node struct {
	t    *testing.T
	name string
	ip   string
	ts   *fakeTS
	tr   *Transport
	w    *link.Managed
	done chan error
	stop context.CancelFunc

	mu   sync.Mutex
	seen []seenReq
	// handle answers what comes in on link B; default: 200 "ok".
	handle func(w http.ResponseWriter, r *http.Request)
}

type seenReq struct {
	Method, URI string
	Header      http.Header
	From        string
	Body        string
}

func fastTimes(t *testing.T) {
	re, wf := RefreshEvery, WhoisFor
	RefreshEvery, WhoisFor = 40*time.Millisecond, 200*time.Millisecond
	t.Cleanup(func() { RefreshEvery, WhoisFor = re, wf })
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startNode(t *testing.T, name, ip string, port int, set func(*Settings)) *node {
	t.Helper()
	n := &node{t: t, name: name, ip: ip, done: make(chan error, 1)}
	n.ts = newFakeTS(t, fakeDev{Name: name, IP: ip, Online: true, Tags: []string{testTag}})
	s := Settings{Tag: "warren-test", Port: port, Socket: n.ts.path}
	if set != nil {
		set(&s)
	}
	n.tr = New(s, quiet())
	n.w = link.NewManaged(name)
	n.w.Handle(http.HandlerFunc(n.serve))
	files, release, err := n.w.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	a, err := net.FileConn(files[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := net.FileConn(files[1])
	if err != nil {
		t.Fatal(err)
	}
	release()
	ctx, cancel := context.WithCancel(context.Background())
	n.stop = cancel
	go func() { n.done <- n.tr.RunManaged(ctx, a, b) }()
	t.Cleanup(func() { cancel(); n.w.Close() })
	return n
}

func (n *node) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	n.mu.Lock()
	n.seen = append(n.seen, seenReq{r.Method, r.URL.RequestURI(), r.Header.Clone(), link.From(r.Context()), string(body)})
	h := n.handle
	n.mu.Unlock()
	w.Header().Set("X-Warren-Node", n.name)
	if h != nil {
		h(w, r)
		return
	}
	w.Write([]byte("ok"))
}

func (n *node) last() seenReq {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.seen) == 0 {
		return seenReq{}
	}
	return n.seen[len(n.seen)-1]
}

func (n *node) count() int { n.mu.Lock(); defer n.mu.Unlock(); return len(n.seen) }

// meet makes the nodes see each other, and know each other by whois.
func meet(nodes ...*node) {
	for _, a := range nodes {
		a.ts.set(func(f *fakeTS) {
			f.peers = nil
			for _, b := range nodes {
				if b != a {
					d := fakeDev{Name: b.name, IP: b.ip, Online: true, Tags: []string{testTag}}
					f.peers = append(f.peers, d)
					f.whois[b.ip] = d
				}
			}
		})
	}
}

func (n *node) peers(t *testing.T) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pl, err := n.w.Peers(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	return pl.Peers
}

func (n *node) waitPeers(t *testing.T, want ...string) {
	t.Helper()
	eventually(t, n.name+" to see peers "+strings.Join(want, ","), func() bool { return slices.Equal(n.peers(t), want) })
}

func (n *node) carry(to, method, uri, body string, h http.Header, within time.Duration) (*link.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	return n.w.Carry(ctx, to, &link.Request{Method: method, URI: uri, Header: h, Body: []byte(body), Deadline: time.Now().Add(within)})
}

// pair starts two nodes on loopback addresses that see each other.
func pair(t *testing.T) (a, b *node) {
	fastTimes(t)
	port := freePort(t)
	a = startNode(t, "a", "127.0.0.1", port, nil)
	b = startNode(t, "b", "127.0.0.2", port, nil)
	meet(a, b)
	a.waitPeers(t, "b")
	b.waitPeers(t, "a")
	eventually(t, "both to listen", func() bool { return a.tr.Bound() != "" && b.tr.Bound() != "" })
	return a, b
}

// ---- tests ----

func TestHelloAndSettings(t *testing.T) {
	fastTimes(t)
	a := startNode(t, "a", "127.0.0.1", freePort(t), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h, err := a.w.Hello(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.Name != "tailscale" || h.Node != "a" || h.Version != 1 || h.SoftwareVersion == "" || h.MaxBody < link.MinMaxBody {
		t.Errorf("hello: %+v", h)
	}
	if err := link.CheckHello(h, "a"); err != nil {
		t.Error(err)
	}
	if link.CheckHello(h, "b") == nil {
		t.Error("a hello for another node was accepted")
	}
	if got := a.tr.tag; got != testTag {
		t.Errorf("tag %q", got)
	}
	if a.ts.badCalls.Load() != 0 || a.ts.calls.Load() == 0 {
		t.Errorf("local API: %d calls, %d without the Sec-Tailscale header", a.ts.calls.Load(), a.ts.badCalls.Load())
	}
}

func TestPeersAreTaggedOnlineOnThisTailnetAndNotSelf(t *testing.T) {
	fastTimes(t)
	a := startNode(t, "a", "127.0.0.1", freePort(t), nil)
	tagged := []string{testTag}
	a.ts.set(func(f *fakeTS) {
		f.peers = []fakeDev{
			{Name: "good", IP: "100.64.0.2", Online: true, Tags: tagged},
			{Name: "offline", IP: "100.64.0.3", Online: false, Tags: tagged},
			{Name: "untagged", IP: "100.64.0.4", Online: true, Tags: []string{"tag:other"}},
			{Name: "shared", IP: "100.64.0.5", Online: true, Tags: tagged, Suffix: "other.ts.net"},
			{Name: "Bad_Name", IP: "100.64.0.6", Online: true, Tags: tagged},
			{Name: "a", IP: "100.64.0.7", Online: true, Tags: tagged},   // this device's name
			{Name: "twin", IP: "127.0.0.1", Online: true, Tags: tagged}, // this device's address
		}
	})
	a.waitPeers(t, "good")
	// A change is long-polled: the poll returns when the list moves on.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pl, _ := a.w.Peers(ctx, "", 0)
	res := make(chan link.PeerList, 1)
	go func() { p, _ := a.w.Peers(ctx, pl.Version, 3*time.Second); res <- p }()
	time.Sleep(100 * time.Millisecond)
	a.ts.set(func(f *fakeTS) {
		f.peers = append(f.peers, fakeDev{Name: "late", IP: "100.64.0.9", Online: true, Tags: tagged})
	})
	select {
	case p := <-res:
		if !slices.Equal(p.Peers, []string{"good", "late"}) {
			t.Errorf("peers %v", p.Peers)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the long poll did not return")
	}
}

func TestCarryBothWays(t *testing.T) {
	a, b := pair(t)
	b.handle = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Warren-Peer", "1")
		w.Header().Set("X-Warren-Transport-Evil", "x") // never carried back
		w.Header().Set("Set-Cookie", "nope")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTeapot)
		w.Write([]byte(`{"hi":1}`))
	}
	h := http.Header{}
	h.Set("X-Warren-Peer", "1")
	h.Set("X-Warren-Version", "9")
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Authorization", "secret")              // not carried
	h.Set("X-Warren-Transport-From", "somebody")  // hop-local: not carried
	h.Set("X-Warren-Transport-Error", "not-sent") // likewise
	resp, err := a.carry("b", "POST", "/v1/peer/call?service=echo&to=x%40b", "payload", h, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != http.StatusTeapot || string(resp.Body) != `{"hi":1}` || resp.Header.Get("Content-Type") != "application/json" || resp.Header.Get("X-Warren-Peer") != "1" {
		t.Errorf("answer: %d %q %v", resp.Status, resp.Body, resp.Header)
	}
	if resp.Header.Get("X-Warren-Transport-Evil") != "" || resp.Header.Get("Set-Cookie") != "" {
		t.Errorf("answer headers leaked: %v", resp.Header)
	}
	got := b.last()
	if got.Method != "POST" || got.URI != "/v1/peer/call?service=echo&to=x%40b" || got.Body != "payload" {
		t.Errorf("b saw %+v", got)
	}
	if got.From != "a" {
		t.Errorf("From is %q: the transport must vouch for the caller", got.From)
	}
	if got.Header.Get("X-Warren-Version") != "9" || got.Header.Get("Content-Type") != "application/octet-stream" {
		t.Errorf("headers lost: %v", got.Header)
	}
	if got.Header.Get("Authorization") != "" || got.Header.Get("X-Warren-Transport-Error") != "" || got.Header.Get("X-Warren-Transport-To") != "" {
		t.Errorf("headers that must not travel did: %v", got.Header)
	}
	if ms := got.Header.Get("X-Warren-Deadline-Ms"); ms == "" || ms == "0" {
		t.Errorf("no deadline carried: %q", ms)
	}
	// And the other way.
	if _, err := b.carry("a", "GET", "/v1/peer/hello", "", nil, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if a.last().From != "b" {
		t.Errorf("a saw From %q", a.last().From)
	}
	// Idle links stay open: after a pause the next request still goes.
	time.Sleep(300 * time.Millisecond)
	if _, err := a.carry("b", "GET", "/v1/peer/hello", "", nil, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	// Every version of the peer protocol is carried.
	if _, err := a.carry("b", "GET", "/v2/peer/hello", "", nil, 5*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestCallersAreVouchedForByWhois(t *testing.T) {
	a, b := pair(t)
	// A raw request from this machine's address 127.0.0.1, which b knows as a.
	raw := func(from, to string, hdr map[string]string) (int, string) {
		d := &net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(from)}, Timeout: 2 * time.Second}
		c := &http.Client{Transport: &http.Transport{DialContext: d.DialContext, DisableKeepAlives: true}, Timeout: 5 * time.Second}
		req, _ := http.NewRequest("GET", "http://"+to+"/v1/peer/hello", nil)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := c.Do(req)
		if err != nil {
			return 0, err.Error()
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, resp.Header.Get("X-Warren-Transport-Error")
	}
	bAddr := b.tr.Bound()
	before := b.count()
	// The caller's own claim about who it is does not survive.
	if st, _ := raw("127.0.0.1", bAddr, map[string]string{"X-Warren-Transport-From": "evil", "X-Warren-Transport-To": "x"}); st != 200 {
		t.Fatalf("a good caller: %d", st)
	}
	if got := b.last(); b.count() != before+1 || got.From != "a" || got.Header.Get("X-Warren-Transport-To") != "" {
		t.Errorf("b saw %+v", got)
	}
	// An address whois does not know, one whose device is not tagged, and this
	// device's own address are refused, and never reach Warren.
	b.ts.set(func(f *fakeTS) {
		f.whois["127.0.0.7"] = fakeDev{Name: "untagged", IP: "127.0.0.7", Tags: []string{"tag:other"}}
		f.whois["127.0.0.8"] = fakeDev{Name: "b", IP: "127.0.0.8", Tags: []string{testTag}} // claims to be this node
		f.whois["127.0.0.9"] = fakeDev{Name: "Bad_Name", IP: "127.0.0.9", Tags: []string{testTag}}
	})
	before = b.count()
	for _, ip := range []string{"127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.2"} {
		st, kind := raw(ip, bAddr, nil)
		if st != http.StatusForbidden || kind != link.NotSent {
			t.Errorf("%s: %d %q", ip, st, kind)
		}
	}
	if b.count() != before {
		t.Error("a refused caller reached Warren")
	}
	// Tags taken away from a peer count from the next refresh.
	b.ts.set(func(f *fakeTS) {
		f.peers = nil
		delete(f.whois, "127.0.0.1")
	})
	b.waitPeers(t)
	eventually(t, "a to be refused", func() bool { st, _ := raw("127.0.0.1", bAddr, nil); return st == http.StatusForbidden })
	// Paths of other APIs are not carried.
	b.ts.set(func(f *fakeTS) { f.whois["127.0.0.1"] = fakeDev{Name: "a", IP: "127.0.0.1", Tags: []string{testTag}} })
	_ = a
	eventually(t, "a admitted again", func() bool { st, _ := raw("127.0.0.1", bAddr, nil); return st == 200 })
	st, kind := func() (int, string) {
		d := &net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}}
		c := &http.Client{Transport: &http.Transport{DialContext: d.DialContext, DisableKeepAlives: true}}
		resp, err := c.Get("http://" + bAddr + "/v1/service/self")
		if err != nil {
			return 0, err.Error()
		}
		defer resp.Body.Close()
		return resp.StatusCode, resp.Header.Get(apierr.Header)
	}()
	if st != 404 || kind != apierr.Unsupported {
		t.Errorf("another API: %d %q", st, kind)
	}
}

func TestNotSentAndLost(t *testing.T) {
	a, b := pair(t)
	_ = b
	notSent := func(what string, err error) {
		t.Helper()
		var ce *link.CarryError
		if err == nil || !asCarry(err, &ce) || ce.Kind != link.NotSent {
			t.Errorf("%s: want not-sent, got %v", what, err)
		}
	}
	lost := func(what string, err error) {
		t.Helper()
		var ce *link.CarryError
		if err == nil || !asCarry(err, &ce) || ce.Kind != link.Lost {
			t.Errorf("%s: want lost, got %v", what, err)
		}
	}
	// A node that is not on the tailnet.
	_, err := a.carry("nobody", "GET", "/v1/peer/hello", "", nil, 3*time.Second)
	notSent("unknown node", err)
	// A body over max_body is never sent.
	_, err = a.carry("b", "POST", "/v1/peer/call", strings.Repeat("x", MaxBody+1), nil, 3*time.Second)
	notSent("over max_body", err)
	// A peer on the tailnet whose port nobody listens on: no connection.
	a.ts.set(func(f *fakeTS) {
		f.peers = append(f.peers, fakeDev{Name: "dead", IP: "127.0.0.4", Online: true, Tags: []string{testTag}})
	})
	a.waitPeers(t, "b", "dead")
	_, err = a.carry("dead", "GET", "/v1/peer/hello", "", nil, 3*time.Second)
	notSent("nobody listens", err)
	// A peer that takes the request and hangs up: it may have been seen.
	port := a.tr.port
	ln, e := net.Listen("tcp", net.JoinHostPort("127.0.0.5", fmt.Sprint(port)))
	if e != nil {
		t.Fatal(e)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { buf := make([]byte, 4096); c.Read(buf); c.Close() }()
		}
	}()
	a.ts.set(func(f *fakeTS) {
		f.peers = append(f.peers, fakeDev{Name: "hangup", IP: "127.0.0.5", Online: true, Tags: []string{testTag}})
	})
	a.waitPeers(t, "b", "dead", "hangup")
	_, err = a.carry("hangup", "POST", "/v1/peer/call", "x", nil, 3*time.Second)
	lost("hang up after the request", err)
	// The other transport's Warren is gone: it says so, and it was not sent.
	b.w.Close()
	time.Sleep(50 * time.Millisecond)
	_, err = a.carry("b", "GET", "/v1/peer/hello", "", nil, 3*time.Second)
	if err == nil {
		t.Error("carried to a node whose Warren is gone")
	}
}

func asCarry(err error, out **link.CarryError) bool {
	ce, ok := err.(*link.CarryError)
	if ok {
		*out = ce
	}
	return ok
}

func TestDeadlineIsWarrensAndCancelIsPassedOn(t *testing.T) {
	a, b := pair(t)
	// No timeout of the transport's own: a slow answer within Warren's
	// deadline arrives.
	b.handle = func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(800 * time.Millisecond):
			w.Write([]byte("slow"))
		case <-r.Context().Done():
		}
	}
	resp, err := a.carry("b", "GET", "/v1/peer/x", "", nil, 10*time.Second)
	if err != nil || string(resp.Body) != "slow" {
		t.Fatalf("%v %v", resp, err)
	}
	// When Warren gives up, the request is abandoned on the far side too.
	gone := make(chan struct{})
	b.handle = func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(gone)
	}
	_, err = a.carry("b", "GET", "/v1/peer/x", "", nil, 300*time.Millisecond)
	if err == nil {
		t.Fatal("no error for a request that outlived its deadline")
	}
	select {
	case <-gone:
	case <-time.After(3 * time.Second):
		t.Fatal("the far side was not told the request is abandoned")
	}
}

func TestRebindsWhenTheTailnetAddressChanges(t *testing.T) {
	fastTimes(t)
	port := freePort(t)
	a := startNode(t, "a", "127.0.0.1", port, nil)
	b := startNode(t, "b", "127.0.0.2", port, nil)
	meet(a, b)
	eventually(t, "a to listen", func() bool { return a.tr.Bound() == fmt.Sprintf("127.0.0.1:%d", port) })
	b.waitPeers(t, "a")
	if _, err := b.carry("a", "GET", "/v1/peer/hello", "", nil, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	// a gets a new address.
	a.ts.set(func(f *fakeTS) { f.self.IP = "127.0.0.5" })
	b.ts.set(func(f *fakeTS) {
		f.peers = []fakeDev{{Name: "a", IP: "127.0.0.5", Online: true, Tags: []string{testTag}}}
		f.whois = map[string]fakeDev{"127.0.0.5": {Name: "a", IP: "127.0.0.5", Tags: []string{testTag}}}
	})
	eventually(t, "a to rebind", func() bool { return a.tr.Bound() == fmt.Sprintf("127.0.0.5:%d", port) })
	if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second); err == nil {
		c.Close()
		t.Error("the old address is still listened on")
	}
	// Requests leave from the new address, and b still knows who sent them.
	eventually(t, "a to reach b from the new address", func() bool {
		_, err := a.carry("b", "GET", "/v1/peer/hello", "", nil, 2*time.Second)
		return err == nil && a.tr.Bound() != ""
	})
	if b.last().From != "a" {
		t.Errorf("b saw From %q", b.last().From)
	}
	eventually(t, "b to reach a at the new address", func() bool {
		_, err := b.carry("a", "GET", "/v1/peer/hello", "", nil, 2*time.Second)
		return err == nil
	})
}

func TestListenSettingOverridesTheTailnetAddress(t *testing.T) {
	fastTimes(t)
	port := freePort(t)
	a := startNode(t, "a", "100.64.0.1", port, func(s *Settings) { s.Listen = fmt.Sprintf("127.0.0.1:%d", port) })
	eventually(t, "a to listen on the configured address", func() bool { return a.tr.Bound() == fmt.Sprintf("127.0.0.1:%d", port) })
	if a.tr.selfIP != "100.64.0.1" {
		t.Errorf("self %q", a.tr.selfIP)
	}
}

func TestListenFailureIsRetried(t *testing.T) {
	fastTimes(t)
	port := freePort(t)
	hold, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	a := startNode(t, "a", "127.0.0.1", port, nil)
	time.Sleep(150 * time.Millisecond)
	if a.tr.Bound() != "" {
		t.Fatal("listening on a taken port")
	}
	hold.Close()
	eventually(t, "a to listen once the port is free", func() bool { return a.tr.Bound() != "" })
}

func TestTailscaledGoneDropsPeersAfterAWhile(t *testing.T) {
	a, _ := pair(t)
	a.ts.set(func(f *fakeTS) { f.backend = "Stopped" })
	a.waitPeers(t) // after a few failed refreshes
	a.ts.set(func(f *fakeTS) { f.backend = "Running" })
	a.waitPeers(t, "b")
}

func TestTransportEndsWhenALinkCloses(t *testing.T) {
	fastTimes(t)
	a := startNode(t, "a", "127.0.0.1", freePort(t), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := a.w.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	a.w.Down() // Warren closes both links
	select {
	case err := <-a.done:
		if err == nil {
			t.Error("the transport ended cleanly though its link broke")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the transport kept running with a closed link")
	}
}

func TestManagedLinksHaveNoIdleTimeout(t *testing.T) {
	// The server side of link A and the client side of link B are built by
	// link; neither may time out an idle connection, since a managed link
	// cannot be dialled again.
	a, _ := pair(t)
	time.Sleep(500 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := a.w.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-a.done:
		t.Fatalf("the transport ended: %v", err)
	default:
	}
}

func TestExternalForm(t *testing.T) {
	fastTimes(t)
	d, err := os.MkdirTemp("", "ext")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(d)
	ts := newFakeTS(t, fakeDev{Name: "a", IP: "127.0.0.1", Online: true, Tags: []string{testTag}})
	tr := New(Settings{Tag: testTag, Port: freePort(t), Socket: ts.path}, quiet())
	linkA, linkB := filepath.Join(d, "a.sock"), filepath.Join(d, "b.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- tr.RunExternal(ctx, linkA, linkB, os.Getuid()) }()
	eventually(t, "link_a", func() bool { _, err := os.Stat(linkA); return err == nil })
	dial := func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "unix", linkA) }
	c := link.NewClient(dial)
	defer c.Close()
	hctx, hcancel := context.WithTimeout(ctx, 5*time.Second)
	defer hcancel()
	h, err := c.Hello(hctx)
	if err != nil || h.Node != "a" {
		t.Fatalf("hello: %+v %v", h, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(5 * time.Second):
		t.Error("the transport did not stop")
	}
	// Another uid than the configured one is closed at accept.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	tr2 := New(Settings{Tag: testTag, Port: freePort(t), Socket: ts.path}, quiet())
	linkA2 := filepath.Join(d, "a2.sock")
	go tr2.RunExternal(ctx2, linkA2, linkB, os.Getuid()+1)
	eventually(t, "link_a of the second", func() bool { _, err := os.Stat(linkA2); return err == nil })
	c2 := link.NewClient(func(ctx context.Context) (net.Conn, error) { return (&net.Dialer{}).DialContext(ctx, "unix", linkA2) })
	defer c2.Close()
	hctx2, hcancel2 := context.WithTimeout(ctx2, 2*time.Second)
	defer hcancel2()
	if _, err := c2.Hello(hctx2); err == nil {
		t.Error("a user other than Warren's reached link A")
	}
	if err := New(Settings{Tag: testTag}, quiet()).RunExternal(ctx, linkA, linkB, -1); err == nil {
		t.Error("an external transport without Warren's uid ran")
	}
}

func TestMachineNames(t *testing.T) {
	for in, want := range map[string]string{
		"server.tail1.ts.net.":  "server",
		"server.TAIL1.ts.net":   "server",
		"server.other.ts.net.":  "",
		"server":                "",
		"Server.tail1.ts.net.":  "", // not a label of the grammar
		"my host.tail1.ts.net.": "",
		"a.b.tail1.ts.net.":     "",
	} {
		if got := machineName(in, "tail1.ts.net"); got != want {
			t.Errorf("machineName(%q) = %q, want %q", in, got, want)
		}
	}
	if normaliseTag("x") != "tag:x" || normaliseTag("tag:x") != "tag:x" || normaliseTag("") != "" {
		t.Error("normaliseTag")
	}
}

// blockOn makes b's Warren hold every request until release is closed.
func blockOn(b *node) (started chan struct{}, release func()) {
	started = make(chan struct{}, 16)
	ch := make(chan struct{})
	b.handle = func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-ch:
		case <-r.Context().Done():
		}
	}
	var once sync.Once
	return started, func() { once.Do(func() { close(ch) }) }
}

func TestRequestsFromTheTailnetAreLimitedBeforeTheirBodiesAreRead(t *testing.T) {
	fastTimes(t)
	pc, bp := MaxPerCaller, MaxBodyPerCaller
	MaxPerCaller, MaxBodyPerCaller = 2, 1<<20
	t.Cleanup(func() { MaxPerCaller, MaxBodyPerCaller = pc, bp })
	a, b := pair(t)
	started, release := blockOn(b)
	defer release()
	errs := make(chan error, 4)
	for i := 0; i < 2; i++ {
		go func() { _, err := a.carry("b", "GET", "/v1/peer/x", "", nil, 10*time.Second); errs <- err }()
	}
	<-started
	select {
	case <-started:
	case err := <-errs:
		t.Fatalf("second request ended: %v", err)
	}
	_, err := a.carry("b", "GET", "/v1/peer/x", "", nil, 3*time.Second)
	if ce, ok := err.(*link.CarryError); !ok || ce.Kind != link.Busy {
		t.Errorf("a third request from one node: %v", err)
	}
	release()
	<-errs
	<-errs
	// Bodies: one of 600 KB is held, a second of 600 KB would pass 1 MiB.
	started, release = blockOn(b)
	defer release()
	body := strings.Repeat("x", 600<<10)
	go func() { _, err := a.carry("b", "POST", "/v1/peer/x", body, nil, 10*time.Second); errs <- err }()
	<-started
	_, err = a.carry("b", "POST", "/v1/peer/x", body, nil, 3*time.Second)
	if ce, ok := err.(*link.CarryError); !ok || ce.Kind != link.Busy {
		t.Errorf("a second body over the budget: %v", err)
	}
	release()
	<-errs
	// Everything is given back.
	b.handle = nil
	if _, err := a.carry("b", "POST", "/v1/peer/x", body, nil, 3*time.Second); err != nil {
		t.Errorf("after the others finished: %v", err)
	}
	b.tr.mu.Lock()
	defer b.tr.mu.Unlock()
	if b.tr.inflight != 0 || b.tr.bodyTotal != 0 || len(b.tr.byCaller) != 0 || len(b.tr.bodyBy) != 0 {
		t.Errorf("admission leaked: %d %d %v %v", b.tr.inflight, b.tr.bodyTotal, b.tr.byCaller, b.tr.bodyBy)
	}
}

func TestInFlightRequestsAreCancelledWhenTheAddressChanges(t *testing.T) {
	a, b := pair(t)
	started, release := blockOn(b)
	defer release()
	res := make(chan error, 1)
	go func() { _, err := a.carry("b", "GET", "/v1/peer/x", "", nil, 30*time.Second); res <- err }()
	<-started
	a.ts.set(func(f *fakeTS) { f.self.IP = "127.0.0.5" })
	select {
	case err := <-res:
		if ce, ok := err.(*link.CarryError); !ok || ce.Kind != link.Lost {
			t.Errorf("want lost, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a request on the old client was not cancelled")
	}
}

func TestClockJumpDropsConnectionsToPeers(t *testing.T) {
	fastTimes(t)
	var offset atomic.Int64
	start := time.Now()
	co, ct := clockOverride, ClockTick
	clockOverride = func() (time.Duration, time.Duration) {
		d := time.Since(start)
		return d + time.Duration(offset.Load()), d
	}
	ClockTick = 20 * time.Millisecond
	t.Cleanup(func() { clockOverride, ClockTick = co, ct })
	a, b := pair(t)
	started, release := blockOn(b)
	defer release()
	res := make(chan error, 1)
	go func() { _, err := a.carry("b", "GET", "/v1/peer/x", "", nil, 30*time.Second); res <- err }()
	<-started
	offset.Add(int64(time.Hour)) // resume from suspend
	select {
	case err := <-res:
		if ce, ok := err.(*link.CarryError); !ok || ce.Kind != link.Lost {
			t.Errorf("want lost, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a request in flight over a resume was not cancelled")
	}
	// And the transport works afterwards.
	b.handle = nil
	if _, err := a.carry("b", "GET", "/v1/peer/x", "", nil, 3*time.Second); err != nil {
		t.Errorf("after the jump: %v", err)
	}
}

func TestLongPollsAndHelloHaveRoomOfTheirOwn(t *testing.T) {
	fastTimes(t)
	pc := MaxPerCaller
	MaxPerCaller = 2
	t.Cleanup(func() { MaxPerCaller = pc })
	a, b := pair(t)
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	b.handle = func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/x") {
			started <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
	}
	defer close(release)
	for i := 0; i < 2; i++ {
		go a.carry("b", "POST", "/v1/peer/x", "y", nil, 10*time.Second)
	}
	<-started
	<-started
	// Calls have used up their places; the polls still get through.
	for _, p := range []string{"/v1/peer/hello", "/v1/peer/services?since=&wait=0", "/v1/peer/entries?service=echo&wait=0"} {
		if _, err := a.carry("b", "GET", p, "", nil, 3*time.Second); err != nil {
			t.Errorf("%s with the call places full: %v", p, err)
		}
	}
	if _, err := a.carry("b", "POST", "/v1/peer/x", "y", nil, 3*time.Second); err == nil {
		t.Error("a call got past the limit")
	}
}

func TestBusyIsCarriedBetweenTransportsAndAnswersHaveAWriteDeadline(t *testing.T) {
	a, b := pair(t)
	// A peer that sends a request and never reads the answer: the transport's
	// write ends at the request's deadline and the place is given back.
	big := strings.Repeat("z", 6<<20)
	b.handle = func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(big)) }
	d := &net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}, Control: func(_, _ string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) { syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 2048) })
	}}
	c, err := d.Dial("tcp", b.tr.Bound())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "GET /v1/peer/call HTTP/1.1\r\nHost: x\r\nX-Warren-Deadline-Ms: 400\r\n\r\n")
	inflight := func() int { b.tr.mu.Lock(); defer b.tr.mu.Unlock(); return b.tr.inflight }
	eventually(t, "the request to be in flight", func() bool { return inflight() == 1 })
	eventually(t, "the place to be given back after the deadline", func() bool { return inflight() == 0 })
	_ = a
	// A 429 busy from the other transport is busy for Warren, not a failure.
	b.tr.SetLimits(func(perCaller, inflight *int) { *perCaller = 0 }) // no package variable is written while servers run
	_, err = a.carry("b", "POST", "/v1/peer/x", "y", nil, 3*time.Second)
	if ce, ok := err.(*link.CarryError); !ok || ce.Kind != link.Busy {
		t.Errorf("want busy, got %v", err)
	}
}

func TestListenerDoesNotOutliveRun(t *testing.T) {
	fastTimes(t)
	port := freePort(t)
	a := startNode(t, "a", "127.0.0.1", port, nil)
	eventually(t, "a to listen", func() bool { return a.tr.Bound() != "" })
	a.stop()
	select {
	case <-a.done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not end")
	}
	if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second); err == nil {
		c.Close()
		t.Error("the listener outlived run")
	}
}

func TestPollsWithABodyAreRefusedUnread(t *testing.T) {
	_, b := pair(t)
	d := &net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}, Timeout: 2 * time.Second}
	for _, path := range []string{"hello", "services?wait=0", "entries?service=echo&wait=0"} {
		c, err := d.Dial("tcp", b.tr.Bound())
		if err != nil {
			t.Fatal(err)
		}
		// A body is declared and never sent: it must not be waited for.
		fmt.Fprintf(c, "GET /v1/peer/%s HTTP/1.1\r\nHost: x\r\nContent-Length: 4000000\r\n\r\n", path)
		c.SetReadDeadline(time.Now().Add(3 * time.Second))
		resp, err := http.ReadResponse(bufio.NewReader(c), nil)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if resp.StatusCode != 400 || resp.Header.Get(link.HeaderError) != link.NotSent {
			t.Errorf("%s: %d %v", path, resp.StatusCode, resp.Header)
		}
		c.Close()
	}
	b.tr.mu.Lock()
	defer b.tr.mu.Unlock()
	if b.tr.pollsTotal != 0 || b.tr.bodyTotal != 0 {
		t.Errorf("a refused poll holds a place: %d %d", b.tr.pollsTotal, b.tr.bodyTotal)
	}
}

func TestConnectionsFromOneAddressAreCappedAndIdleOnesClosed(t *testing.T) {
	fastTimes(t)
	pc, ci := MaxConnsPerIP, ConnIdle
	MaxConnsPerIP, ConnIdle = 3, 1500*time.Millisecond
	t.Cleanup(func() { MaxConnsPerIP, ConnIdle = pc, ci })
	_, b := pair(t)
	d := &net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}, Timeout: 2 * time.Second}
	var conns []net.Conn
	for i := 0; i < 5; i++ {
		c, err := d.Dial("tcp", b.tr.Bound())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		conns = append(conns, c)
	}
	closed := 0
	for _, c := range conns {
		c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
		if _, err := c.Read(make([]byte, 1)); err == io.EOF {
			closed++
		}
	}
	if closed != 2 {
		t.Errorf("%d of 5 connections closed at accept, want 2 over the cap of 3", closed)
	}
	// The ones kept are idle and closed after ConnIdle.
	eventually(t, "idle connections closed", func() bool {
		b.tr.mu.Lock()
		defer b.tr.mu.Unlock()
		return b.tr.conns == 0
	})
}

func TestLimitsFitLinkB(t *testing.T) {
	if MaxInflight+MaxPollsTotal > link.MinStreams {
		t.Errorf("%d + %d requests may be in flight on a link of %d streams", MaxInflight, MaxPollsTotal, link.MinStreams)
	}
}

func TestOneWhoisPerAddressAtATime(t *testing.T) {
	_, b := pair(t)
	b.ts.delay.Store(int64(300 * time.Millisecond))
	before := b.ts.whoises.Load()
	b.ts.set(func(f *fakeTS) { f.whois["127.0.0.11"] = fakeDev{Name: "a", IP: "127.0.0.11", Tags: []string{testTag}} })
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := &net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.11")}, Timeout: 2 * time.Second}
			c := &http.Client{Transport: &http.Transport{DialContext: d.DialContext, DisableKeepAlives: true}, Timeout: 5 * time.Second}
			resp, err := c.Get("http://" + b.tr.Bound() + "/v1/peer/hello")
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	if n := b.ts.whoises.Load() - before; n != 1 {
		t.Errorf("%d whois questions for one address", n)
	}
}

func TestConnectionCapsTieToTheRequestLimits(t *testing.T) {
	if MaxConnsPerIP < MaxPerCaller+MaxPollsPerCaller+16 {
		t.Errorf("a caller may have %d requests in flight (plus idle connections) but only %d connections", MaxPerCaller+MaxPollsPerCaller, MaxConnsPerIP)
	}
	if MaxClientConnsPerPeer > MaxConnsPerIP {
		t.Errorf("this transport may open %d connections to a peer that admits %d", MaxClientConnsPerPeer, MaxConnsPerIP)
	}
	if MaxConns < 10*MaxConnsPerIP {
		t.Errorf("MaxConns %d does not leave room for 10 peers at %d", MaxConns, MaxConnsPerIP)
	}
}

func TestWhoisIsNotCancelledWithTheRequestThatAskedIt(t *testing.T) {
	_, b := pair(t)
	b.ts.delay.Store(int64(500 * time.Millisecond))
	b.ts.set(func(f *fakeTS) { f.whois["127.0.0.12"] = fakeDev{Name: "a", IP: "127.0.0.12", Tags: []string{testTag}} })
	get := func(timeout time.Duration) int {
		d := &net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.12")}, Timeout: 2 * time.Second}
		c := &http.Client{Transport: &http.Transport{DialContext: d.DialContext, DisableKeepAlives: true}, Timeout: timeout}
		resp, err := c.Get("http://" + b.tr.Bound() + "/v1/peer/hello")
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	leader := make(chan int, 1)
	go func() { leader <- get(100 * time.Millisecond) }() // gives up while whois runs
	time.Sleep(50 * time.Millisecond)
	if st := get(5 * time.Second); st != 200 {
		t.Errorf("the follower of a cancelled leader got %d", st)
	}
	<-leader
}

func TestAskedTimeoutIsCapped(t *testing.T) {
	huge := int((24 * time.Hour).Milliseconds())
	if got := askedTimeout(huge, false); got != 310*time.Second {
		t.Errorf("a call may ask for at most 310 s, got %v", got)
	}
	if got := askedTimeout(huge, true); got != 35*time.Second {
		t.Errorf("a poll may ask for at most 35 s, got %v", got)
	}
	if got := askedTimeout(math.MaxInt, false); got != 310*time.Second {
		t.Errorf("the largest int must clamp, got %v", got)
	}
	if got := askedTimeout(1500, false); got != 1500*time.Millisecond {
		t.Errorf("a small ask is kept, got %v", got)
	}
}

// A device renamed while the transport runs ends it with an error: what
// Warren checked at start (the node name) is no longer true.
func TestRenameWhileRunningEndsTheTransport(t *testing.T) {
	fastTimes(t)
	a := startNode(t, "a", "127.0.0.1", freePort(t), nil)
	eventually(t, "a to be ready", func() bool {
		select {
		case <-a.tr.ready:
			return true
		default:
			return false
		}
	})
	a.ts.set(func(f *fakeTS) { f.self.Name = "renamed" })
	select {
	case err := <-a.done:
		if err == nil || !strings.Contains(err.Error(), "renamed") {
			t.Errorf("the transport ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the transport kept running under another name")
	}
}

// Hello with no answer from tailscaled waits for the caller to give up, and
// does not answer with an empty node name (which Warren would refuse).
func TestHelloWaitsForTailscaledOrTheCaller(t *testing.T) {
	tr := New(Settings{Tag: testTag, Socket: filepath.Join(t.TempDir(), "none.sock")}, quiet())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	h := tr.Hello(ctx)
	if d := time.Since(start); d < 250*time.Millisecond {
		t.Errorf("hello answered after %v, before the caller gave up", d)
	}
	if h.Node != "" {
		t.Errorf("node %q", h.Node)
	}
}

// A body that stalls is a bad request, not a body that is too large.
func TestStalledBodyIsABadRequest(t *testing.T) {
	old := BodyRead
	BodyRead = 300 * time.Millisecond
	t.Cleanup(func() { BodyRead = old })
	_, b := pair(t)
	d := &net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}, Timeout: 2 * time.Second}
	c, err := d.Dial("tcp", b.tr.Bound())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "POST /v1/peer/call HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\nonly a few bytes")
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 || resp.Header.Get(link.HeaderError) != link.NotSent {
		t.Errorf("a stalled body: %d %v", resp.StatusCode, resp.Header)
	}
}
