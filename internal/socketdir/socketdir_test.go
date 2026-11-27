package socketdir

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func ok(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) })
}

func client(path string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		},
		DisableKeepAlives: true,
	}, Timeout: 3 * time.Second}
}

func get(t *testing.T, path string) string {
	t.Helper()
	resp, err := client(path).Get("http://x/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func short(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "sd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func TestServeAndStop(t *testing.T) {
	dir := short(t)
	g := NewGroup(quiet())
	p := filepath.Join(dir, "a.sock")
	if f := g.Sync([]Want{{Key: "a", Path: p, Handler: ok("hi")}}); len(f) != 0 {
		t.Fatal(f)
	}
	if get(t, p) != "hi" {
		t.Error("wrong body")
	}
	fi, _ := os.Lstat(p)
	if fi.Mode().Perm() != 0o666 {
		t.Errorf("mode %v", fi.Mode().Perm())
	}
	g.Sync(nil)
	if _, err := os.Lstat(p); err == nil {
		t.Error("socket file left behind")
	}
}

func TestReplacedSocketIsServedAgain(t *testing.T) {
	dir := short(t)
	g := NewGroup(quiet())
	defer g.Close()
	p := filepath.Join(dir, "a.sock")
	w := []Want{{Key: "a", Path: p, Handler: ok("one")}}
	g.Sync(w)
	os.Remove(p) // the child removes it
	g.Sync(w)
	if get(t, p) != "one" {
		t.Error("not served again")
	}
}

func TestHostileSymlink(t *testing.T) {
	dir := short(t)
	target := filepath.Join(dir, "victim")
	os.WriteFile(target, []byte("precious"), 0o600)
	p := filepath.Join(dir, "a.sock")
	os.Symlink(target, p)
	g := NewGroup(quiet())
	defer g.Close()
	f := g.Sync([]Want{{Key: "a", Path: p, Handler: ok("x")}})
	if f["a"] == nil {
		t.Fatal("served over a symlink")
	}
	if b, _ := os.ReadFile(target); string(b) != "precious" {
		t.Error("the link's target was touched")
	}
	if fi, _ := os.Lstat(p); fi.Mode()&os.ModeSymlink == 0 {
		t.Error("the link was removed or replaced")
	}
}

func TestHostileDanglingSymlink(t *testing.T) {
	dir := short(t)
	p := filepath.Join(dir, "a.sock")
	target := filepath.Join(dir, "planted")
	os.Symlink(target, p) // would make bind create "planted" through a follow
	g := NewGroup(quiet())
	defer g.Close()
	if g.Sync([]Want{{Key: "a", Path: p, Handler: ok("x")}})["a"] == nil {
		t.Fatal("served over a dangling symlink")
	}
	if _, err := os.Lstat(target); err == nil {
		t.Error("a file was created through the link")
	}
}

func TestHostilePrecreatedFile(t *testing.T) {
	dir := short(t)
	p := filepath.Join(dir, "a.sock")
	os.WriteFile(p, []byte("mine"), 0o644)
	g := NewGroup(quiet())
	defer g.Close()
	if g.Sync([]Want{{Key: "a", Path: p, Handler: ok("x")}})["a"] == nil {
		t.Fatal("served over a file")
	}
	if b, _ := os.ReadFile(p); string(b) != "mine" {
		t.Error("file removed")
	}
	// Same for a directory.
	os.Remove(p)
	os.Mkdir(p, 0o755)
	if g.Sync([]Want{{Key: "a", Path: p, Handler: ok("x")}})["a"] == nil {
		t.Fatal("served over a directory")
	}
}

func TestHostileSymlinkedDirectory(t *testing.T) {
	base := short(t)
	real := filepath.Join(base, "real")
	os.Mkdir(real, 0o755)
	child := filepath.Join(base, "child")
	os.Symlink(real, child)
	g := NewGroup(quiet())
	defer g.Close()
	if g.Sync([]Want{{Key: "a", Path: filepath.Join(child, "a.sock"), Handler: ok("x")}})["a"] == nil {
		t.Fatal("served through a linked directory")
	}
	if es, _ := os.ReadDir(real); len(es) != 0 {
		t.Error("something was created in the link's target")
	}
	// And a linked directory is no child.
	os.Mkdir(filepath.Join(base, "plain"), 0o755)
	dirs, _ := ListDirs(base)
	if strings.Join(dirs, ",") != "plain,real" {
		t.Errorf("ListDirs = %v", dirs)
	}
}

func TestOtherOwnersSocketIsLeftAlone(t *testing.T) {
	dir := short(t)
	p := filepath.Join(dir, "a.sock")
	g := NewGroup(quiet())
	g.Sync([]Want{{Key: "a", Path: p, Handler: ok("x")}})
	g.Close()
	// A socket file that is not ours: leave one behind by making Warren
	// believe it runs as someone else.
	l, _ := net.Listen("unix", p)
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	l.Close()
	real := Euid
	defer func() { Euid = real }()
	Euid = func() int { return real() + 1 }
	g2 := NewGroup(quiet())
	defer g2.Close()
	if g2.Sync([]Want{{Key: "a", Path: p, Handler: ok("x")}})["a"] == nil {
		t.Fatal("replaced another user's socket")
	}
	if _, err := os.Lstat(p); err != nil {
		t.Error("the other user's socket was removed")
	}
}

func TestSocketMadeByOthersIsNotOurs(t *testing.T) {
	// With a different idea of who Warren is, even a fresh socket fails the
	// owner check after binding: it is how a swapped-in socket is caught.
	dir := short(t)
	real := Euid
	defer func() { Euid = real }()
	g := NewGroup(quiet())
	defer g.Close()
	Euid = func() int { return real() + 1 }
	p := filepath.Join(dir, "a.sock")
	if g.Sync([]Want{{Key: "a", Path: p, Handler: ok("x")}})["a"] == nil {
		t.Fatal("served a socket owned by someone else")
	}
}

func TestPathTooLong(t *testing.T) {
	g := NewGroup(quiet())
	defer g.Close()
	p := filepath.Join(short(t), strings.Repeat("x", 120)+".sock")
	if g.Sync([]Want{{Key: "a", Path: p, Handler: ok("x")}})["a"] == nil {
		t.Fatal("long path accepted")
	}
}

func TestUIDCheck(t *testing.T) {
	dir := short(t)
	me := os.Geteuid()
	other := me + 1
	g := NewGroup(quiet())
	defer g.Close()
	refused := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", http.StatusForbidden) })
	pa, pb := filepath.Join(dir, "a.sock"), filepath.Join(dir, "b.sock")
	g.Sync([]Want{
		{Key: "a", Path: pa, UID: &me, Handler: ok("in"), Refuse: refused},
		{Key: "b", Path: pb, UID: &other, Handler: ok("in"), Refuse: refused},
	})
	if get(t, pa) != "in" {
		t.Error("matching uid refused")
	}
	// A wrong uid is closed at accept.
	c, err := net.Dial("unix", pb)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Errorf("wrong uid not closed at accept: %v", err)
	}
}

func TestWrongUIDDoesNotFillTheCap(t *testing.T) {
	dir := short(t)
	me := os.Geteuid()
	other := me + 1
	g := NewGroup(quiet())
	defer g.Close()
	p := filepath.Join(dir, "a.sock")
	g.Sync([]Want{{Key: "a", Path: p, UID: &other, Handler: ok("in")}})
	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for i := 0; i < 2*MaxConns; i++ {
		c, err := net.Dial("unix", p)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
	}
	time.Sleep(100 * time.Millisecond)
	// The socket now admits the real user: the strangers did not use the cap up.
	g.Sync([]Want{{Key: "a", Path: p, UID: &me, Handler: ok("in")}})
	if get(t, p) != "in" {
		t.Error("strangers filled the connection cap")
	}
}

func TestConnectionCap(t *testing.T) {
	dir := short(t)
	g := NewGroup(quiet())
	defer g.Close()
	p := filepath.Join(dir, "a.sock")
	g.Sync([]Want{{Key: "a", Path: p, Handler: ok("x")}})
	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for i := 0; i < MaxConns; i++ {
		c, err := net.Dial("unix", p)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
	}
	time.Sleep(100 * time.Millisecond)
	c, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	// The 65th is closed at once: a read ends in EOF, not a timeout.
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Errorf("65th connection not closed: %v", err)
	}
	// Closing one makes room again.
	conns[0].Close()
	time.Sleep(100 * time.Millisecond)
	if get(t, p) != "x" {
		t.Error("no room after a close")
	}
}

func TestWarnings(t *testing.T) {
	d := short(t)
	os.Chmod(d, 0o777)
	if len(DirWarnings(d)) == 0 {
		t.Error("world writable directory without sticky bit not flagged")
	}
	os.Chmod(d, os.ModeSticky|0o777)
	if len(DirWarnings(d)) != 0 {
		t.Errorf("sticky directory flagged: %v", DirWarnings(d))
	}
	os.Chmod(d, 0o755)
	if len(UpperWarnings(d, false)) == 0 || len(UpperWarnings(d, true)) != 0 {
		t.Error("UpperWarnings")
	}
}

type logBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logBuf) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *logBuf) count(s string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Count(l.b.String(), s)
}

func TestWrongUIDCloseIsLoggedOncePerUser(t *testing.T) {
	dir := short(t)
	other := os.Geteuid() + 1
	var lb logBuf
	g := NewGroup(slog.New(slog.NewTextHandler(&lb, nil)))
	defer g.Close()
	p := filepath.Join(dir, "a.sock")
	g.Sync([]Want{{Key: "a", Path: p, UID: &other, Handler: ok("in")}})
	for i := 0; i < 5; i++ {
		c, err := net.Dial("unix", p)
		if err != nil {
			t.Fatal(err)
		}
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		c.Read(make([]byte, 1))
		c.Close()
	}
	if n := lb.count("does not admit"); n != 1 {
		t.Errorf("the same user's closes were logged %d times", n)
	}
}

func TestSyncAfterCloseServesNothing(t *testing.T) {
	dir := short(t)
	g := NewGroup(quiet())
	g.Close()
	p := filepath.Join(dir, "a.sock")
	if f := g.Sync([]Want{{Key: "a", Path: p, Handler: ok("x")}}); f["a"] == nil {
		t.Error("Sync after Close was accepted")
	}
	if _, err := os.Lstat(p); err == nil {
		t.Error("a socket came back after Close")
	}
}

func TestHeaderOverTheLimitIsRefused(t *testing.T) {
	dir := short(t)
	g := NewGroup(quiet())
	defer g.Close()
	p := filepath.Join(dir, "a.sock")
	g.Sync([]Want{{Key: "a", Path: p, Handler: ok("in")}})
	c, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	req := "GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + strings.Repeat("a", 2*MaxHeaderBytes) + "\r\n\r\n"
	_, _ = c.Write([]byte(req))
	buf := make([]byte, 64)
	n, _ := c.Read(buf)
	if !strings.Contains(string(buf[:n]), "431") {
		t.Errorf("a header over MaxHeaderBytes was not refused with 431: %q", buf[:n])
	}
}

// A socket with a closed mode is made in a staging directory and moved into
// place, so it does not exist under its name with the process umask's mode.
// With umask 0 the socket would be 0777 until a chmod; here it is 0600, and
// no staging directory is left behind.
func TestClosedSocketIsStagedAndLeavesNothingBehind(t *testing.T) {
	old := syscall.Umask(0)
	defer syscall.Umask(old)
	dir := short(t)
	g := NewGroup(quiet())
	defer g.Close()
	p := filepath.Join(dir, "a.sock")
	if f := g.Sync([]Want{{Key: "a", Path: p, Mode: 0o600, Handler: ok("in")}}); f["a"] != nil {
		t.Fatal(f["a"])
	}
	fi, err := os.Stat(p)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("socket: %v %v", fi, err)
	}
	if get(t, p) != "in" {
		t.Error("the staged socket does not answer")
	}
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".stage-") {
			t.Errorf("a staging directory was left: %s", e.Name())
		}
	}
}

// A held socket takes a changed mode on the next Sync, in both directions,
// and the new handler with it.
func TestHeldSocketTakesAChangedMode(t *testing.T) {
	dir := short(t)
	g := NewGroup(quiet())
	defer g.Close()
	p := filepath.Join(dir, "a.sock")
	mode := func() os.FileMode {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Mode().Perm()
	}
	g.Sync([]Want{{Key: "a", Path: p, Mode: 0o600, Handler: ok("one")}})
	if mode() != 0o600 {
		t.Fatalf("start: %v", mode())
	}
	g.Sync([]Want{{Key: "a", Path: p, Mode: 0o666, Handler: ok("two")}})
	if mode() != 0o666 || get(t, p) != "two" {
		t.Errorf("loosened: %v", mode())
	}
	g.Sync([]Want{{Key: "a", Path: p, Mode: 0o600, Handler: ok("three")}})
	if mode() != 0o600 || get(t, p) != "three" {
		t.Errorf("tightened: %v", mode())
	}
	// No Mode is the default 0666, and is not a change from 0666.
	g.Sync([]Want{{Key: "a", Path: p, Handler: ok("four")}})
	if mode() != 0o666 || get(t, p) != "four" {
		t.Errorf("default: %v", mode())
	}
}

// A connection that was open when a Sync changed who is admitted loses its
// access: the file mode only matters at connect().
func TestOpenConnectionIsRefusedAfterTheAdmittedUserChanges(t *testing.T) {
	dir := short(t)
	g := NewGroup(quiet())
	defer g.Close()
	p := filepath.Join(dir, "a.sock")
	me := os.Geteuid()
	other := me + 1
	refuse := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", http.StatusForbidden) })
	for _, tc := range []struct {
		name  string
		first Want
		next  Want
	}{
		{"uid", Want{Key: "a", Path: p, UID: &me, Handler: ok("in"), Refuse: refuse}, Want{Key: "a", Path: p, UID: &other, Handler: ok("in"), Refuse: refuse}},
		{"admit", Want{Key: "a", Path: p, Admit: func(uid int, known bool) bool { return known && uid == me }, Handler: ok("in"), Refuse: refuse},
			Want{Key: "a", Path: p, Admit: func(uid int, known bool) bool { return known && uid == other }, Handler: ok("in"), Refuse: refuse}},
	} {
		g.Sync([]Want{tc.first})
		// One keep-alive connection, used twice.
		tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", p)
		}}
		c := &http.Client{Transport: tr, Timeout: 3 * time.Second}
		get := func() (int, error) {
			resp, err := c.Get("http://x/")
			if err != nil {
				return 0, err
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
			return resp.StatusCode, nil
		}
		if code, err := get(); err != nil || code != 200 {
			t.Fatalf("%s: before the change: %d %v", tc.name, code, err)
		}
		// The change closes the open connection (a retry on a new one is turned
		// away at accept), so the user is refused one way or the other.
		g.Sync([]Want{tc.next})
		if code, err := get(); err == nil && code == 200 {
			t.Errorf("%s: an open connection kept its access after the change", tc.name)
		}
		tr.CloseIdleConnections()
	}
}

func TestListDirsSkipsDotDirectories(t *testing.T) {
	d := short(t)
	for _, n := range []string{"a", ".stage-x", ".hidden"} {
		os.Mkdir(filepath.Join(d, n), 0o755)
	}
	got, err := ListDirs(d)
	if err != nil || len(got) != 1 || got[0] != "a" {
		t.Errorf("ListDirs: %v %v", got, err)
	}
}

func TestAdjustDirRefusesALink(t *testing.T) {
	d := short(t)
	os.Mkdir(filepath.Join(d, "real"), 0o700)
	os.Symlink(filepath.Join(d, "real"), filepath.Join(d, "link"))
	_, _, err := AdjustDir(filepath.Join(d, "link"), func(DirState) (os.FileMode, bool) { return 0o755, true })
	if !errors.Is(err, ErrNotPlainDir) {
		t.Errorf("a link: %v", err)
	}
	st, changed, err := AdjustDir(filepath.Join(d, "real"), func(DirState) (os.FileMode, bool) { return 0o711, true })
	if err != nil || !changed || st.Perm != 0o700 {
		t.Errorf("a directory: %+v %v %v", st, changed, err)
	}
	if fi, _ := os.Stat(filepath.Join(d, "real")); fi.Mode().Perm() != 0o711 {
		t.Errorf("mode %v", fi.Mode().Perm())
	}
}

// A request in progress (a long poll) from a user the new want no longer
// admits is cut at once, on plain HTTP and on h2c streams.
func TestInFlightRequestIsCutWhenTheUserIsNoLongerAdmitted(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		dir := short(t)
		g := NewGroup(quiet())
		p := filepath.Join(dir, "a.sock")
		me := os.Geteuid()
		other := me + 1
		started, cut := make(chan struct{}), make(chan struct{})
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			select {
			case <-r.Context().Done():
				close(cut)
			case <-time.After(20 * time.Second):
			}
		})
		g.Sync([]Want{{Key: "a", Path: p, UID: &me, H2C: h2, Handler: h}})
		tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", p)
		}}
		if h2 {
			var pr http.Protocols
			pr.SetUnencryptedHTTP2(true)
			tr.Protocols = &pr
		}
		go func() {
			resp, err := (&http.Client{Transport: tr, Timeout: 25 * time.Second}).Get("http://x/")
			if err == nil {
				resp.Body.Close()
			}
		}()
		<-started
		g.Sync([]Want{{Key: "a", Path: p, UID: &other, H2C: h2, Handler: ok("x")}})
		select {
		case <-cut:
		case <-time.After(3 * time.Second):
			t.Errorf("h2=%v: the long poll of a user who is no longer admitted was not cut", h2)
		}
		tr.CloseIdleConnections()
		g.Close()
	}
}

// A name that is swapped for another file between the check of a stale socket
// and its removal must not lose that file.
func TestStaleSocketSwappedBeforeRemovalIsLeftAlone(t *testing.T) {
	dir := short(t)
	p := filepath.Join(dir, "a.sock")
	ln, err := net.Listen("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	ln.Close()
	planted := filepath.Join(dir, "planted")
	if err := os.WriteFile(planted, []byte("precious"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Euid is called between the check and the removal: the swap is made
	// there, once, without falsifying any ownership.
	real := Euid
	defer func() { Euid = real }()
	swapped := false
	Euid = func() int {
		if !swapped {
			swapped = true
			if err := os.Rename(planted, p); err != nil {
				t.Error(err)
			}
		}
		return os.Geteuid()
	}
	g := NewGroup(quiet())
	defer g.Close()
	failures := g.Sync([]Want{{Key: "a", Path: p, Handler: ok("x")}})
	if len(failures) == 0 {
		t.Fatal("expected the swapped-in file to be reported and left alone")
	}
	b, err := os.ReadFile(p)
	if err != nil || string(b) != "precious" {
		t.Fatalf("the file that was swapped in was lost: %q %v", b, err)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 1 {
		t.Fatalf("a quarantine directory was left behind: %v", ents)
	}
}
