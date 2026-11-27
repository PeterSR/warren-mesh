package service

import (
	"context"
	"github.com/PeterSR/warren-mesh/internal/link"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/internal/apierr"
	"github.com/PeterSR/warren-mesh/internal/socketdir"
)

func connectable(p string) bool {
	c, err := net.Dial("unix", p)
	if err == nil {
		c.Close()
	}
	return err == nil
}

func TestTemplateChildrenArePickedUpWhileRunning(t *testing.T) {
	e := newEnv(t, "")
	e.mkChild("late")
	p := filepath.Join(e.cfg.Attach.Dir, "late", "echo.sock")
	eventually(t, "template child served", func() bool { return connectable(p) })
	c := newCl(t, p)
	var self map[string]any
	c.get("/v1/service/self").json(t, &self)
	if self["relay"] != "late.n1" || self["flat"] != false {
		t.Errorf("self %v", self)
	}
	// And dropped again when the directory goes.
	startEcho(t, c)
	os.RemoveAll(filepath.Join(e.cfg.Attach.Dir, "late"))
	eventually(t, "child dropped", func() bool { return !e.node.hasSock("late.n1", "echo") })
	st := e.nodeSock("echo").get("/v1/service/status")
	if strings.Contains(string(st.body), "late") {
		t.Errorf("status still lists the child: %s", st.body)
	}
}

func (n *Node) hasSock(relay, svc string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.socks[sockKey{relay, svc}] != nil
}

func TestWithoutDefaultNothingIsServed(t *testing.T) {
	y := strings.Replace(baseYAML, "  default:\n    services:\n      - name: echo\n        allow: [\"*\"]\n", "", 1)
	e := newEnv(t, y)
	e.mkChild("late")
	time.Sleep(5 * ScanEvery)
	if connectable(filepath.Join(e.cfg.Attach.Dir, "late", "echo.sock")) {
		t.Error("a child without a template was served")
	}
}

func TestMaxChildren(t *testing.T) {
	y := strings.Replace(baseYAML, "  dir: DIR/a\n", "  dir: DIR/a\n  max_children: 5\n", 1)
	e := newEnv(t, y) // 3 configured children
	e.mkChild("t1")
	e.mkChild("t2")
	e.mkChild("t3")
	eventually(t, "two template children", func() bool {
		return connectable(filepath.Join(e.cfg.Attach.Dir, "t1", "echo.sock")) && connectable(filepath.Join(e.cfg.Attach.Dir, "t2", "echo.sock"))
	})
	time.Sleep(5 * ScanEvery)
	if connectable(filepath.Join(e.cfg.Attach.Dir, "t3", "echo.sock")) {
		t.Error("a directory beyond max_children was served")
	}
	// Children already served keep their place when a new one appears first
	// in the sort order.
	e.mkChild("a0")
	time.Sleep(5 * ScanEvery)
	if connectable(filepath.Join(e.cfg.Attach.Dir, "a0", "echo.sock")) || !connectable(filepath.Join(e.cfg.Attach.Dir, "t1", "echo.sock")) {
		t.Error("a served child lost its place to a new one")
	}
	// Room appears when one goes.
	os.RemoveAll(filepath.Join(e.cfg.Attach.Dir, "t1"))
	eventually(t, "freed place used", func() bool { return connectable(filepath.Join(e.cfg.Attach.Dir, "a0", "echo.sock")) })
}

func TestChildNamedLikeANamespaceGetsNothing(t *testing.T) {
	e := newEnv(t, "")
	e.mkChild("chat") // chat is a service namespace of the node's echo
	time.Sleep(5 * ScanEvery)
	if connectable(filepath.Join(e.cfg.Attach.Dir, "chat", "echo.sock")) {
		t.Error("a child named like a namespace was served")
	}
}

func TestBadDirectoryNamesAreSkipped(t *testing.T) {
	e := newEnv(t, "")
	for _, n := range []string{"Bad", "has.dot", ".hidden", strings.Repeat("a", 62)} {
		os.Mkdir(filepath.Join(e.cfg.Attach.Dir, n), 0o755)
	}
	e.mkChild("good")
	eventually(t, "good served", func() bool { return connectable(filepath.Join(e.cfg.Attach.Dir, "good", "echo.sock")) })
	for _, n := range []string{"Bad", "has.dot", ".hidden", strings.Repeat("a", 62)} {
		if es, _ := os.ReadDir(filepath.Join(e.cfg.Attach.Dir, n)); len(es) != 0 {
			t.Errorf("something created in %q", n)
		}
	}
}

// ---- hostile child directories ----

func TestHostileSymlinkWhereASocketGoes(t *testing.T) {
	e := newEnv(t, "")
	victim := filepath.Join(e.dir, "victim")
	os.WriteFile(victim, []byte("precious"), 0o600)
	e.mkChild("evil")
	// The child plants a link where the socket would go, before the scan.
	sock := filepath.Join(e.cfg.Attach.Dir, "evil", "echo.sock")
	os.Symlink(victim, sock)
	time.Sleep(6 * ScanEvery)
	if b, _ := os.ReadFile(victim); string(b) != "precious" {
		t.Error("the victim file was touched")
	}
	if fi, err := os.Lstat(sock); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Error("the planted link was replaced")
	}
	// Other children are unaffected, and the node did not give up: once the
	// link is gone the socket is made.
	startEcho(t, e.childSock("kit", "echo"))
	os.Remove(sock)
	eventually(t, "served after the link is removed", func() bool { return connectable(sock) })
}

func TestHostilePrecreatedFileAndDirectory(t *testing.T) {
	e := newEnv(t, "")
	e.mkChild("evil")
	file := filepath.Join(e.cfg.Attach.Dir, "evil", "echo.sock")
	os.WriteFile(file, []byte("mine"), 0o644)
	time.Sleep(6 * ScanEvery)
	if b, _ := os.ReadFile(file); string(b) != "mine" {
		t.Error("a pre-created file was replaced")
	}
	os.Remove(file)
	os.Mkdir(file, 0o755) // a directory where the socket goes
	time.Sleep(6 * ScanEvery)
	if fi, _ := os.Lstat(file); !fi.IsDir() {
		t.Error("a pre-created directory was replaced")
	}
	// Nothing was created inside it either.
	if es, _ := os.ReadDir(file); len(es) != 0 {
		t.Error("something created in the child's own subdirectory")
	}
}

func TestHostileLinkedChildDirectory(t *testing.T) {
	e := newEnv(t, "")
	real := filepath.Join(e.dir, "elsewhere")
	os.Mkdir(real, 0o755)
	os.Symlink(real, filepath.Join(e.cfg.Attach.Dir, "linked"))
	time.Sleep(6 * ScanEvery)
	if es, _ := os.ReadDir(real); len(es) != 0 {
		t.Errorf("sockets were made through a linked directory: %v", es)
	}
}

func TestDirectoryReplacedByALinkWhileRunning(t *testing.T) {
	e := newEnv(t, "")
	e.mkChild("swap")
	dir := filepath.Join(e.cfg.Attach.Dir, "swap")
	eventually(t, "served", func() bool { return connectable(filepath.Join(dir, "echo.sock")) })
	real := filepath.Join(e.dir, "elsewhere")
	os.Mkdir(real, 0o755)
	os.RemoveAll(dir)
	os.Symlink(real, dir)
	time.Sleep(8 * ScanEvery)
	if es, _ := os.ReadDir(real); len(es) != 0 {
		t.Errorf("a socket was made in the link's target: %v", es)
	}
	if e.node.hasSock("swap.n1", "echo") {
		t.Error("the swapped directory is still a child")
	}
}

func TestRemovedSocketFileIsServedAgain(t *testing.T) {
	e := newEnv(t, "")
	e.mkChild("c")
	p := filepath.Join(e.cfg.Attach.Dir, "c", "echo.sock")
	eventually(t, "served", func() bool { return connectable(p) })
	os.Remove(p)
	eventually(t, "served again", func() bool { return connectable(p) })
}

func TestSunPathTooLongIsSkippedForTemplateChildren(t *testing.T) {
	e := newEnv(t, "")
	// Make a label so long that <attach.dir>/<label>/echo.sock exceeds 107.
	long := strings.Repeat("x", 62)
	for len(filepath.Join(e.cfg.Attach.Dir, long, "echo.sock")) <= 107 && len(long) < 62 {
		long += "x"
	}
	if len(filepath.Join(e.cfg.Attach.Dir, long, "echo.sock")) <= 107 {
		t.Skip("temp directory too short to overflow sun_path")
	}
	e.mkChild(long)
	time.Sleep(6 * ScanEvery)
	if e.node.hasSock(long+".n1", "echo") {
		t.Error("a socket with a too long path was set up")
	}
}

func TestUIDRequirement(t *testing.T) {
	me := os.Geteuid()
	y := strings.Replace(baseYAML, "    - name: kit\n", "    - name: kit\n      uid: "+itoa(me+1)+"\n", 1)
	y = strings.Replace(y, "    - name: host2\n", "    - name: host2\n      uid: "+itoa(me)+"\n", 1)
	e := newEnv(t, y)
	// kit requires another user: this process's connection is closed at accept.
	kit := e.childSock("kit", "echo")
	hc := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", kit.path)
		},
		DisableKeepAlives: true,
	}, Timeout: 5 * time.Second}
	req, _ := http.NewRequest("GET", "http://warren/v1/service/self", nil)
	req.Header.Set("X-Warren", "1")
	if r, err := hc.Do(req); err == nil {
		r.Body.Close()
		t.Errorf("a connection from another user was served: %d", r.StatusCode)
	}
	// host2 requires this user: fine.
	expectOK(t, e.childSock("host2", "echo").get("/v1/service/self"))
	// host has no uid: fine.
	expectOK(t, e.childSock("host", "echo").get("/v1/service/self"))
}

// ---- reload ----

func TestReloadAddsAndRemovesServicesAndChildren(t *testing.T) {
	e := newEnv(t, "")
	boot := e.node.Boot()
	kit := e.childSock("kit", "echo")
	lk, _ := kit.serve(1, false)
	refused(t, kit.put(lk, names("alice"), ""))
	host := e.childSock("host", "echo")
	lh, _ := host.serve(1, false)

	// Add a service to kit and a new configured child (directory first).
	e.mkChild("fresh")
	y := strings.Replace(e.yaml, "    - name: kit\n      services:\n        - name: echo\n          allow: [\"*\"]\n",
		"    - name: kit\n      services:\n        - name: echo\n          allow: [\"*\"]\n        - name: other\n          allow: [\"*\"]\n    - name: fresh\n      services:\n        - name: other\n          allow: [\"*\"]\n", 1)
	if err := e.node.Reload(parseYAML(t, e.dir, y)); err != nil {
		t.Fatal(err)
	}
	if e.node.Boot() != boot {
		t.Error("reload changed boot")
	}
	eventually(t, "new sockets", func() bool {
		return connectable(filepath.Join(e.cfg.Attach.Dir, "kit", "other.sock")) && connectable(filepath.Join(e.cfg.Attach.Dir, "fresh", "other.sock"))
	})
	// kit's lease and entries survive a reload that did not touch them.
	if v := host.view(); !v.has("alice@kit.n1") || v.Entries[0].Stale {
		t.Errorf("entries after reload: %+v", v.Entries)
	}
	expectOK(t, kit.get("/v1/service/calls?lease="+lk+"&wait=0").withStatus(t, 204))
	_ = lh

	// Remove kit's echo service: its lease ends and its entries go.
	y2 := strings.Replace(y, "    - name: kit\n      services:\n        - name: echo\n          allow: [\"*\"]\n        - name: other", "    - name: kit\n      services:\n        - name: other", 1)
	e.yaml = y
	if err := e.node.Reload(parseYAML(t, e.dir, y2)); err != nil {
		t.Fatal(err)
	}
	if v := host.view(); v.has("alice@kit.n1") {
		t.Error("removed service's entries still listed")
	}
	if connectable(filepath.Join(e.cfg.Attach.Dir, "kit", "echo.sock")) {
		eventually(t, "socket removed", func() bool { return !connectable(filepath.Join(e.cfg.Attach.Dir, "kit", "echo.sock")) })
	}
}

func (r resp) withStatus(t *testing.T, s int) resp {
	t.Helper()
	if r.status != s {
		t.Fatalf("status %d, want %d (%s)", r.status, s, r.body)
	}
	return resp{status: 200}
}

func TestReloadFlatChangeIsRemovalAndAddition(t *testing.T) {
	e := newEnv(t, "", func() { LeaseGrace = 5 * time.Second })
	host := e.childSock("host", "echo")
	l, _ := host.serve(1, false)
	refused(t, host.put(l, names("bob"), ""))
	y := strings.Replace(e.yaml, "    - name: host\n      flat: true\n", "    - name: host\n      flat: false\n", 1)
	if err := e.node.Reload(parseYAML(t, e.dir, y)); err != nil {
		t.Fatal(err)
	}
	// The lease ended and the entries are gone: the child starts over, and
	// now has a namespace of its own. The old token is not live: gone.
	expect(t, host.get("/v1/service/calls?lease="+l+"&wait=0"), apierr.Gone)
	var self map[string]any
	eventually(t, "reserved socket", func() bool {
		r := e.childSock("host", "echo").get("/v1/service/self")
		r.json(t, &self)
		return self["entry_namespace"] == "host.n1"
	})
	if self["flat"] != false {
		t.Errorf("self %v", self)
	}
}

func TestReloadRefusesRemovingAConfiguredChildWhoseDirectoryExists(t *testing.T) {
	e := newEnv(t, "")
	y := strings.Replace(e.yaml, "    - name: host\n      flat: true\n      services:\n        - name: echo\n          allow: [\"*\"]\n", "", 1)
	err := e.node.Reload(parseYAML(t, e.dir, y))
	if err == nil || !strings.Contains(err.Error(), "still exists") {
		t.Fatalf("reload not refused: %v", err)
	}
	// Nothing changed: host is still configured and flat.
	var self map[string]any
	e.childSock("host", "echo").get("/v1/service/self").json(t, &self)
	if self["flat"] != true {
		t.Errorf("self %v", self)
	}
	// Once its directory is gone, the removal is fine.
	os.RemoveAll(filepath.Join(e.cfg.Attach.Dir, "host"))
	if err := e.node.Reload(parseYAML(t, e.dir, y)); err != nil {
		t.Fatalf("reload refused after the directory was removed: %v", err)
	}
}

func TestReloadRefusesRestartSettings(t *testing.T) {
	e := newEnv(t, "")
	y := strings.Replace(e.yaml, "node: n1", "node: n2", 1)
	if err := e.node.Reload(parseYAML(t, e.dir, y)); err == nil {
		t.Error("node rename accepted")
	}
}

func TestReloadChangesAllowAndLimitsInPlace(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	startEcho(t, kit)
	host := e.childSock("host", "echo")
	expectOK(t, host.call("kit.n1", "m", "", ""))
	y := strings.Replace(e.yaml, "    - name: kit\n      services:\n        - name: echo\n          allow: [\"*\"]", "    - name: kit\n      services:\n        - name: echo\n          allow: [\"host2.n1\"]", 1)
	if err := e.node.Reload(parseYAML(t, e.dir, y)); err != nil {
		t.Fatal(err)
	}
	expect(t, host.call("kit.n1", "m", "", ""), apierr.Refused)
	expectOK(t, e.childSock("host2", "echo").call("kit.n1", "m", "", ""))
}

// ---- drain, shutdown and the full list of error codes ----

func TestDrainAnswersNotReadyAndWaitsForHandedCalls(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	node := e.nodeSock("echo")
	handed := make(chan resp, 1)
	go func() { handed <- node.call("kit.n1", "a", "", "&timeout_ms=20000") }()
	r := kit.get("/v1/service/calls?lease=" + l + "&wait=25")
	expectOK(t, r)
	held := make(chan resp, 1)
	go func() { held <- node.call("kit.n1", "b", "", "&timeout_ms=20000") }()
	eventually(t, "both calls to hold a place", func() bool { return e.node.placesTaken("kit.n1", "echo") == 2 })

	drained := make(chan struct{})
	go func() { e.node.Drain(); close(drained) }()
	// Held calls fail not-ready at once; new work is refused.
	expect(t, <-held, apierr.NotReady)
	time.Sleep(50 * time.Millisecond)
	expect(t, kit.get("/v1/service/calls?lease="+l+"&wait=25"), apierr.NotReady)
	expect(t, kit.get("/v1/service/view?wait=0"), apierr.NotReady)
	expect(t, node.call("kit.n1", "c", "", ""), apierr.NotReady)
	select {
	case <-drained:
		t.Fatal("drain did not wait for the handed-out call")
	case <-time.After(100 * time.Millisecond):
	}
	// The handed call can still be answered during the drain.
	expectOK(t, kit.post("/v1/service/answer?lease="+l+"&id="+r.hdr.Get("X-Warren-Call"), "done"))
	if a := <-handed; string(a.body) != "done" {
		t.Errorf("answer %q", a.body)
	}
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not finish after the answer")
	}
}

func TestDrainGivesUpAfterDrainWait(t *testing.T) {
	e := newEnv(t, "")
	kit := e.childSock("kit", "echo")
	l, _ := kit.serve(1, false)
	node := e.nodeSock("echo")
	go node.call("kit.n1", "a", "", "&timeout_ms=20000")
	expectOK(t, kit.get("/v1/service/calls?lease="+l+"&wait=25"))
	start := time.Now()
	e.node.Drain()
	if d := time.Since(start); d < DrainWait-50*time.Millisecond || d > DrainWait+time.Second {
		t.Errorf("drain took %v, want about %v", d, DrainWait)
	}
}

func TestEveryErrorCodeOccurs(t *testing.T) {
	// A single walk that produces every code of the table, so none is
	// forgotten: the other tests cover each in more depth.
	net := link.NewNet()
	net.Join("far")
	e := newEnv(t, "", func() { testLink = net.Join("n1") })
	seen := map[string]bool{}
	see := func(r resp, code string) {
		t.Helper()
		expect(t, r, code)
		seen[code] = true
	}
	kit := e.childSock("kit", "echo")
	host := e.childSock("host", "echo")
	node := e.nodeSock("echo")

	lh, _ := host.serve(1, false)
	see(kit.post("/v1/service/call?method=m&version=1", ""), apierr.BadRequest)
	see(kit.headerOnly("/v1/service/call?to=host.n1&method=m&version=1", MaxCallBody+1), apierr.TooLarge)
	see(node.call("kit.n1", "m", "", ""), apierr.NoService)
	see(node.call("nobody.n1", "m", "", ""), apierr.NoTarget)
	see(e.nodeSock("other").call("kit.n1", "m", "", ""), apierr.Refused)
	see(kit.post("/v1/service/call?to=host.n1&method=m&version=7", ""), apierr.Version)
	see(host.post("/v1/service/serve?version=1", ""), apierr.AlreadyServed)
	see(host.get("/v1/service/calls?lease=nope"), apierr.Gone)
	see(kit.call("host.n1", "m", "", "&timeout_ms=100"), apierr.Timeout)
	see(kit.get("/v1/service/nothing"), apierr.Unsupported)

	// unknown: handed out, then the lease ends.
	res := make(chan resp, 1)
	go func() { res <- kit.call("host.n1", "m", "", "&timeout_ms=20000") }()
	expectOK(t, host.get("/v1/service/calls?lease="+lh+"&wait=25"))
	host.post("/v1/service/release?lease="+lh, "")
	see(<-res, apierr.Unknown)
	// Any token that is not live is gone; none at all is a bad request.
	see(host.get("/v1/service/calls?lease="+lh), apierr.Gone)

	// busy: outflight of 16 is large; use the PUT rate instead.
	PutInterval = time.Hour
	lk, _ := kit.serve(1, false)
	refused(t, kit.put(lk, "[]", ""))
	see(kit.put(lk, "[]", ""), apierr.Busy)

	// offline: a node the transport sees, but it never answers hello, so nothing is sent.
	eventually(t, "peer seen", func() bool { return kit.call("x.far", "m", "", "").code() == apierr.Offline })
	see(kit.call("far", "m", "", ""), apierr.Offline)
	see(kit.call("alice@far", "m", "", ""), apierr.Offline)
	see(kit.call("x.unknown", "m", "", ""), apierr.NoTarget)
	// not-ready: during the drain.
	go e.node.Drain()
	eventually(t, "draining", func() bool { return kit.get("/v1/service/view?wait=0").code() == apierr.NotReady })
	seen[apierr.NotReady] = true

	for _, c := range apierr.Codes() {
		if !seen[c] {
			t.Errorf("error code %s was never produced", c)
		}
	}
}

const hostBlock = "    - name: host\n      flat: true\n      services:\n        - name: echo\n          allow: [\"*\"]\n"
const host2Block = "    - name: host2\n      flat: true\n      services:\n        - name: echo\n          allow: [\"*\"]\n"

func TestReloadAppliesChildOrderToPrecedence(t *testing.T) {
	e := newEnv(t, "")
	host, host2 := e.childSock("host", "echo"), e.childSock("host2", "echo")
	lh, _ := host.serve(1, false)
	l2, _ := host2.serve(1, false)
	refused(t, host.put(lh, names("x"), ""))
	refused(t, host2.put(l2, names("x"), ""))
	if r := host2.view().lostReason("x"); r != reasonHeldByEarlier {
		t.Fatalf("before the reload host2's x is %q", r)
	}
	y := strings.Replace(e.yaml, hostBlock+host2Block, host2Block+hostBlock, 1)
	if y == e.yaml {
		t.Fatal("test config not swapped")
	}
	if err := e.node.Reload(parseYAML(t, e.dir, y)); err != nil {
		t.Fatal(err)
	}
	if r := host.view().lostReason("x"); r != reasonHeldByEarlier {
		t.Errorf("after swapping the order host's x is %q, want %s", r, reasonHeldByEarlier)
	}
	if r := host2.view().lostReason("x"); r != "" {
		t.Errorf("after swapping the order host2's x is %q, want held", r)
	}
}

func TestReloadAppliesChildNamePatterns(t *testing.T) {
	e := newEnv(t, "")
	host := e.childSock("host", "echo")
	lh, _ := host.serve(1, false)
	refused(t, host.put(lh, names("x"), ""))
	if !host.view().has("x@n1") {
		t.Fatalf("x is not listed: %v", host.view().addrs())
	}
	y := strings.Replace(e.yaml, "    - name: host\n      flat: true\n", "    - name: host\n      flat: true\n      names: [\"zzz*\"]\n", 1)
	if err := e.node.Reload(parseYAML(t, e.dir, y)); err != nil {
		t.Fatal(err)
	}
	v := host.view()
	if v.has("x@n1") || v.lostReason("x") != reasonNotAllowed {
		t.Errorf("x stays listed after names were restricted: %v lost=%v", v.addrs(), v.Lost)
	}
}

// services.dir is closed to other users unless a node service names a uid,
// and a node socket without a uid is never open to others.
func TestServicesDirAndSocketModes(t *testing.T) {
	mode := func(p string) os.FileMode {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Mode().Perm()
	}
	// No uid anywhere: the directory is 0700, so its sockets are Warren's.
	e := newEnv(t, "")
	if m := mode(e.cfg.Services.Dir); m != 0o700 {
		t.Errorf("services.dir without a uid is %v, want 0700", m)
	}
	// One service with a uid: the directory is 0711 so that user can pass,
	// and the other service's socket is 0600.
	me := os.Geteuid()
	y := strings.Replace(baseYAML, "    - name: other\n", "    - name: other\n      uid: "+itoa(me)+"\n", 1)
	e = newEnv(t, y)
	if m := mode(e.cfg.Services.Dir); m != 0o711 {
		t.Errorf("services.dir with a uid is %v, want 0711", m)
	}
	if m := mode(e.cfg.ServicePath("echo")); m != 0o600 {
		t.Errorf("a socket without a uid in an enterable directory is %v, want 0600", m)
	}
	if m := mode(e.cfg.ServicePath("other")); m != 0o666 {
		t.Errorf("a socket with a uid is %v, want 0666", m)
	}
	if w := socketdir.UpperWarnings(e.cfg.Services.Dir, true); len(w) != 0 {
		t.Errorf("warnings for a 0711 services.dir: %v", w)
	}
}

// startWithDir starts a node on y after pre has made what should already
// exist in the test's directory.
func startWithDir(t *testing.T, y string, pre func(dir string)) *env {
	t.Helper()
	quick(t)
	e := &env{t: t, dir: tmpDir(t), yaml: y}
	pre(e.dir)
	e.cfg = parseYAML(t, e.dir, y)
	for _, c := range e.cfg.Attach.Children {
		os.MkdirAll(filepath.Join(e.cfg.Attach.Dir, c.Name), 0o755)
	}
	e.start(nil)
	return e
}

func permOf(t *testing.T, p string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Mode().Perm()
}

// A services.dir that exists already, open to its group or to everyone,
// does not expose the sockets of services without a uid: they are 0600
// whatever the directory allows.
func TestNodeSocketsAreClosedInAnExistingOpenDirectory(t *testing.T) {
	for _, m := range []os.FileMode{0o755, 0o750, 0o710, 0o711} {
		m := m
		e := startWithDir(t, baseYAML, func(dir string) {
			p := filepath.Join(dir, "s")
			if err := os.MkdirAll(p, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(p, m); err != nil {
				t.Fatal(err)
			}
		})
		for _, svc := range []string{"echo", "other"} {
			if got := permOf(t, e.cfg.ServicePath(svc)); got != 0o600 {
				t.Errorf("directory %v: the %s socket is %v, want 0600", m, svc, got)
			}
		}
	}
}

// A reload that adds a uid to a service opens its socket (with the uid check)
// and the directory; one that removes it closes the socket again.
func TestReloadChangesSocketModes(t *testing.T) {
	me := os.Geteuid()
	withUID := strings.Replace(baseYAML, "    - name: other\n", "    - name: other\n      uid: "+itoa(me)+"\n", 1)
	e := newEnv(t, "")
	other := e.cfg.ServicePath("other")
	if m := permOf(t, other); m != 0o600 {
		t.Fatalf("before: %v", m)
	}
	if m := permOf(t, e.cfg.Services.Dir); m != 0o700 {
		t.Fatalf("directory before: %v", m)
	}
	// Add a uid.
	if err := e.node.Reload(parseYAML(t, e.dir, withUID)); err != nil {
		t.Fatal(err)
	}
	if m := permOf(t, other); m != 0o666 {
		t.Errorf("after adding a uid the socket is %v, want 0666", m)
	}
	if m := permOf(t, e.cfg.Services.Dir); m != 0o711 {
		t.Errorf("after adding a uid the directory is %v, want 0711", m)
	}
	if m := permOf(t, e.cfg.ServicePath("echo")); m != 0o600 {
		t.Errorf("the socket without a uid is %v, want 0600", m)
	}
	// The socket still answers its own user.
	expectOK(t, e.nodeSock("other").get("/v1/service/self"))
	// Remove it again.
	if err := e.node.Reload(parseYAML(t, e.dir, baseYAML)); err != nil {
		t.Fatal(err)
	}
	if m := permOf(t, other); m != 0o600 {
		t.Errorf("after removing the uid the socket is %v, want 0600", m)
	}
	if m := permOf(t, e.cfg.Services.Dir); m != 0o700 {
		t.Errorf("after removing the uid the directory is %v, want 0700 again", m)
	}
}

// A staging directory left by a Warren that died while binding is removed,
// and a dot directory in attach.dir is never reported as a bad child label.
func TestLeftoverStagingDirectoriesAreRemovedQuietly(t *testing.T) {
	var buf lockedBuf
	quick(t)
	dir := tmpDir(t)
	cfg := parseYAML(t, dir, baseYAML)
	for _, d := range []string{cfg.Services.Dir, cfg.Attach.Dir} {
		if err := os.MkdirAll(filepath.Join(d, ".stage-deadbeef"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// A socket of ours inside the services one.
	ln, err := net.Listen("unix", filepath.Join(cfg.Services.Dir, ".stage-deadbeef", "echo.sock"))
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	ln.Close()
	n, err := New(Options{Config: cfg, Log: slog.New(slog.NewTextHandler(&buf, nil))})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if _, err := os.Stat(filepath.Join(cfg.Services.Dir, ".stage-deadbeef")); !os.IsNotExist(err) {
		t.Errorf("the leftover staging directory remains: %v", err)
	}
	time.Sleep(2 * ScanEvery) // the attach.dir scan has run
	if strings.Contains(buf.String(), "not a usable child label") || strings.Contains(buf.String(), ".stage-") {
		t.Errorf("the dot directory was reported:\n%s", buf.String())
	}
}

// A reload that changes the user a socket admits ends the old holder's lease
// and cuts the long poll it has open at once.
func TestUIDChangeEndsTheLeaseAndCutsItsPoll(t *testing.T) {
	me := os.Geteuid()
	y := func(uid int) string {
		return strings.Replace(baseYAML, "    - name: other\n", "    - name: other\n      uid: "+itoa(uid)+"\n", 1)
	}
	e := newEnv(t, y(me))
	c := e.nodeSock("other")
	lease, _ := c.serve(1, false)
	polled := make(chan error, 1)
	go func() {
		// The connection is cut, or the poll is answered gone: either ends it.
		req, _ := http.NewRequest("GET", "http://warren/v1/service/calls?lease="+lease+"&wait=25", nil)
		req.Header.Set("X-Warren", "1")
		r, err := c.hc.Do(req)
		if err == nil {
			r.Body.Close()
		}
		polled <- err
	}()
	eventually(t, "the poll to be open", func() bool {
		return e.node.sockState("n1", "other", func(s *sock) int { return b2i(s.lease != nil) }) == 1
	})
	time.Sleep(200 * time.Millisecond)
	began := time.Now()
	if err := e.node.Reload(parseYAML(t, e.dir, y(me+1))); err != nil {
		t.Fatal(err)
	}
	select {
	case <-polled:
		if d := time.Since(began); d > 3*time.Second {
			t.Errorf("the poll ended after %v", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the long poll of the old uid holder survived the reload")
	}
	if e.node.sockState("n1", "other", func(s *sock) int { return b2i(s.lease != nil) }) != 0 {
		t.Error("the old lease survived the uid change")
	}
}

// The names a node service holds belong to the service, not to the uid of its
// socket: a reload that changes the uid keeps what the service published at
// runtime, in names.json and across a restart.
func TestUIDChangeKeepsTheServiceNames(t *testing.T) {
	me := os.Geteuid()
	y := func(uid int) string {
		return strings.Replace(baseYAML, "      namespaces: [chat]\n", "      namespaces: [chat]\n      uid: "+itoa(uid)+"\n", 1)
	}
	e := newEnv(t, y(me))
	c := e.nodeSock("echo")
	l, _ := c.serve(1, false)
	refused(t, c.put(l, names("alice"), `["default"]`))
	e.node.flushNames()
	if err := e.node.Reload(parseYAML(t, e.dir, y(me+1))); err != nil {
		t.Fatal(err)
	}
	e.node.flushNames()
	data, err := os.ReadFile(filepath.Join(e.cfg.StateDir, "names.json"))
	if err != nil || !strings.Contains(string(data), "alice") || !strings.Contains(string(data), "default") {
		t.Fatalf("names.json after the uid change: %s %v", data, err)
	}
	// A restart: the names are still held, so a flat child cannot take them.
	e.restart()
	if got := e.node.persisted["echo"].Names; !slices.Equal(got, []string{"alice"}) {
		t.Errorf("persisted names after a restart: %v", got)
	}
	host := e.childSock("host", "echo")
	lh, _ := host.serve(1, false)
	rs := refused(t, host.put(lh, names("alice"), ""))
	if !hasReason(rs, "alice", "held-by-node") {
		t.Errorf("a flat child took a held name after the restart: %v", rs)
	}
}

// An existing services.dir that Warren owns is made exactly 0711 when a node
// service names a uid, even if it was open to everyone for writing.
func TestOwnedServicesDirIsNormalisedWithAUID(t *testing.T) {
	y := strings.Replace(baseYAML, "    - name: other\n", "    - name: other\n      uid: "+itoa(os.Geteuid())+"\n", 1)
	for _, m := range []os.FileMode{0o777, 0o775, 0o755, 0o711, 0o770} {
		e := startWithDir(t, y, func(dir string) {
			p := filepath.Join(dir, "s")
			if err := os.MkdirAll(p, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(p, m); err != nil {
				t.Fatal(err)
			}
		})
		if got := permOf(t, e.cfg.Services.Dir); got != 0o711 {
			t.Errorf("services.dir started as %o is %o, want 0711", m, got)
		}
	}
}
