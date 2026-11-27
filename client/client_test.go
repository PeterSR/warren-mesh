package client_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PeterSR/warren-mesh/client"
)

func TestDial(t *testing.T) {
	cl := startDev(t, "")
	path := sock(t, cl, "laptop", "chat")
	if _, err := client.Dial(""); err == nil || !strings.Contains(err.Error(), client.EnvSocket) {
		t.Errorf("no path and no variable: %v", err)
	}
	if _, err := client.Dial("/nonexistent/x.sock"); err == nil {
		t.Error("a missing socket should fail at Dial")
	}
	// The path comes from the environment.
	t.Setenv(client.EnvSocket, path)
	c := dial(t, "")
	self, err := c.Self(context.Background())
	if err != nil || self.Relay != "laptop" || self.Service != "chat" || self.Node != "laptop" ||
		self.EntryNamespace != "laptop" || len(self.Namespaces) != 1 || self.Namespaces[0] != "web.laptop" ||
		self.MaxCallBody != client.MaxCallBody || self.Boot == "" || self.Flat {
		t.Errorf("self: %+v %v", self, err)
	}
	host := dial(t, sock(t, cl, "host.laptop", "chat"))
	if self, _ := host.Self(context.Background()); self == nil || !self.Flat || self.EntryNamespace != "laptop" {
		t.Errorf("host self: %+v", self)
	}
	st, err := c.Status(context.Background())
	if err != nil || st.Node != "laptop" || st.Warren.Version == "" || len(st.Children) != 2 || len(st.Services) < 4 || !st.LinkUp {
		t.Errorf("status: %+v %v", st, err)
	}
}

func TestServerUIDCheck(t *testing.T) {
	cl := startDev(t, "")
	path := sock(t, cl, "laptop", "chat")
	me := os.Geteuid()
	dial(t, path, client.WithServerUID(me))
	if _, err := client.Dial(path, client.WithServerUID(me+1)); err == nil || !strings.Contains(err.Error(), "served by user") {
		t.Errorf("wrong uid accepted: %v", err)
	}
	// WARREN_UID is read when no option is given, and an option wins.
	t.Setenv(client.EnvUID, fmt.Sprint(me+1))
	if _, err := client.Dial(path); err == nil {
		t.Error("WARREN_UID of another user accepted")
	}
	dial(t, path, client.WithServerUID(me))
	dial(t, path, client.WithoutServerUIDCheck())
	t.Setenv(client.EnvUID, "nonsense")
	if _, err := client.Dial(path); err == nil || !strings.Contains(err.Error(), "not a user id") {
		t.Errorf("bad WARREN_UID: %v", err)
	}
}

func TestServeAndCall(t *testing.T) {
	cl := startDev(t, "")
	server := dial(t, sock(t, cl, "laptop", "chat"))
	var gotCall atomic.Value
	srv := server.NewServer(client.ServeOptions{Version: 1}, func(_ context.Context, c *client.Call) ([]byte, error) {
		gotCall.Store(c)
		return append([]byte(c.Method+":"), c.Body...), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	defer func() { cancel(); <-done }()
	srv.SetEntries(client.EntriesRequest{Entries: []client.Entry{{Name: "alice", Data: []byte(`{"x":1}`)}}})

	caller := dial(t, sock(t, cl, "sandbox.laptop", "chat"))
	var out []byte
	var err error
	eventually(t, "the service to be served", func() bool {
		out, err = caller.Call(context.Background(), client.CallRequest{To: "alice@laptop", Method: "hi", Version: 1, From: "bob", Body: []byte("there")})
		return !client.IsCode(err, client.CodeNoService, client.CodeNoTarget)
	})
	if err != nil || string(out) != "hi:there" {
		t.Fatalf("call: %q %v", out, err)
	}
	c := gotCall.Load().(*client.Call)
	if c.Origin != "sandbox.laptop" || c.From != "bob@sandbox.laptop" || c.To != "alice@laptop" || c.Method != "hi" || c.ID == "" || time.Until(c.Deadline) <= 0 {
		t.Errorf("call as seen by the service: %+v", c)
	}

	// Across nodes: the other node's socket sees the entry and calls it.
	other := dial(t, sock(t, cl, "server", "chat"))
	eventually(t, "the entry to reach the other node", func() bool {
		v, err := other.View(context.Background(), "", 0)
		return err == nil && len(v.Entries) == 1 && v.Entries[0].Address == "alice@laptop" && v.Entries[0].Origin == "laptop"
	})
	out, err = other.Call(context.Background(), client.CallRequest{To: "alice@laptop", Method: "far", Version: 1})
	if err != nil || string(out) != "far:" {
		t.Errorf("remote call: %q %v", out, err)
	}
}

func TestParallelPolls(t *testing.T) {
	cl := startDev(t, "")
	server := dial(t, sock(t, cl, "laptop", "chat"))
	// Four calls are handled at the same time: each handler waits until all
	// of them are inside.
	const n = 4
	var inside atomic.Int32
	release := make(chan struct{})
	var once sync.Once
	srv := server.NewServer(client.ServeOptions{Version: 1}, func(_ context.Context, c *client.Call) ([]byte, error) {
		if c.Method == "probe" {
			return []byte("ok"), nil
		}
		if inside.Add(1) == n {
			once.Do(func() { close(release) })
		}
		<-release
		return []byte("ok"), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	defer func() { cancel(); <-done }()
	caller := dial(t, sock(t, cl, "host.laptop", "chat"))
	eventually(t, "service", func() bool {
		_, err := caller.Call(context.Background(), client.CallRequest{To: "laptop", Method: "probe", Version: 1, Timeout: time.Second})
		return err == nil
	})
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := caller.Call(context.Background(), client.CallRequest{To: "laptop", Method: "par", Version: 1, Timeout: 5 * time.Second})
			if err != nil || string(out) != "ok" {
				errs <- fmt.Errorf("%q %v", out, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestCallErrors(t *testing.T) {
	cl := startDev(t, "")
	c := dial(t, sock(t, cl, "sandbox.laptop", "chat"))
	call := func(r client.CallRequest) error {
		_, err := c.Call(context.Background(), r)
		return err
	}
	cases := []struct {
		name string
		r    client.CallRequest
		code string
		retr bool
	}{
		{"no version", client.CallRequest{To: "laptop", Method: "m"}, client.CodeBadRequest, false},
		{"bad method", client.CallRequest{To: "laptop", Method: "NOT A LABEL", Version: 1}, client.CodeBadRequest, false},
		{"no such relay", client.CallRequest{To: "nobody.laptop", Method: "m", Version: 1}, client.CodeNoTarget, false},
		{"no lease", client.CallRequest{To: "laptop", Method: "m", Version: 1}, client.CodeNoService, true},
		{"not allowed", client.CallRequest{To: "laptop", Method: "m", Version: 1, From: "x@elsewhere"}, client.CodeRefused, false},
		{"too large", client.CallRequest{To: "laptop", Method: "m", Version: 1, Body: make([]byte, client.MaxCallBody+1)}, client.CodeTooLarge, false},
	}
	for _, tc := range cases {
		err := call(tc.r)
		if !client.IsCode(err, tc.code) || client.IsRetryable(err) != tc.retr {
			t.Errorf("%s: %v (want %s, retryable %v)", tc.name, err, tc.code, tc.retr)
		}
		if !errors.Is(err, &client.Error{Code: tc.code}) {
			t.Errorf("%s: errors.Is does not match %s", tc.name, tc.code)
		}
	}
	// A service that does not answer in time.
	server := dial(t, sock(t, cl, "laptop", "chat"))
	stop := make(chan struct{})
	srv := server.NewServer(client.ServeOptions{Version: 1, Drain: 100 * time.Millisecond}, func(ctx context.Context, c *client.Call) ([]byte, error) {
		<-stop // never answers in time
		return nil, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	defer func() { close(stop); cancel(); <-done }()
	var err error
	eventually(t, "the service", func() bool {
		err = call(client.CallRequest{To: "laptop", Method: "slow", Version: 1, Timeout: 300 * time.Millisecond})
		return !client.IsCode(err, client.CodeNoService)
	})
	if !client.IsCode(err, client.CodeTimeout) || !client.IsMaybeDelivered(err) || client.IsRetryable(err) {
		t.Errorf("slow: %v", err)
	}
	err = call(client.CallRequest{To: "laptop", Method: "slow", Version: 2})
	if !client.IsCode(err, client.CodeVersion) {
		t.Errorf("version: %v", err)
	}
	e, _ := client.AsError(err)
	if e == nil || e.Advice() != client.AdviceGiveUp || e.Status != 409 {
		t.Errorf("advice: %+v", e)
	}
}

// TestLeaseLostAndTaken: another process takes the lease over; the Server's
// polls are told gone, it waits (already-served), and serves again and
// publishes again as soon as the thief lets go.
func TestLeaseLostAndTaken(t *testing.T) {
	cl := startDev(t, "")
	path := sock(t, cl, "laptop", "chat")
	srv := echoServer(t, dial(t, path), client.ServeOptions{Version: 1})
	srv.SetEntries(client.EntriesRequest{Entries: []client.Entry{{Name: "alice"}}})
	watch := dial(t, sock(t, cl, "server", "chat"))
	seen := func() bool {
		v, err := watch.View(context.Background(), "", 0)
		if err != nil {
			return false
		}
		for _, e := range v.Entries {
			if e.Address == "alice@laptop" && !e.Stale {
				return true
			}
		}
		return false
	}
	eventually(t, "alice to be published", seen)

	thief := dial(t, path)
	var l *client.Lease
	eventually(t, "the thief to take over", func() bool {
		var err error
		l, err = thief.Serve(context.Background(), 1, true)
		if err != nil && !client.IsCode(err, client.CodeBusy) { // one serve per interval per socket
			t.Fatal(err)
		}
		return err == nil
	})
	// The thief holds the lease, so the Server's entries are stale.
	eventually(t, "alice to go stale", func() bool { return !seen() })
	// Warren answers a poll of the old lease with gone, so the Server serves
	// again only when the thief's lease is released.
	if err := thief.Release(context.Background(), l.Token); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the Server to serve and publish again", seen)
}

func TestWatcherAndBootChange(t *testing.T) {
	dir := tempDir(t)
	cl := startDev(t, dir)
	path := sock(t, cl, "laptop", "chat")
	var boots []string
	var mu sync.Mutex
	srv := echoServer(t, dial(t, path), client.ServeOptions{Version: 1, OnLease: func(self *client.Self, _ *client.Lease, changed bool) {
		mu.Lock()
		boots = append(boots, fmt.Sprint(self.Boot[:4], changed))
		mu.Unlock()
	}})
	srv.SetEntries(client.EntriesRequest{Entries: []client.Entry{{Name: "alice"}}})

	w := dial(t, sock(t, cl, "laptop", "chat")).NewWatcher()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var v *client.View
	var err error
	for {
		if v, err = w.Next(ctx); err != nil {
			t.Fatal(err)
		}
		if len(v.Entries) == 1 && !v.Entries[0].Stale {
			break
		}
	}
	if v.BootChanged || v.Boot == "" {
		t.Errorf("first view: %+v", v)
	}
	boot := v.Boot

	// Restart Warren on the same directory: a new boot, the Server serves
	// again and publishes again, and the Watcher says so.
	cl.Stop()
	cl2 := startDev(t, dir)
	_ = cl2
	changed := false
	for !changed || len(v.Entries) != 1 || v.Entries[0].Stale {
		if v, err = w.Next(ctx); err != nil {
			t.Fatal(err)
		}
		if v.BootChanged {
			changed = true
			if v.Boot == boot {
				t.Error("BootChanged with the same boot")
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(boots) < 2 || !strings.HasSuffix(boots[len(boots)-1], "true") {
		t.Errorf("OnLease saw %v", boots)
	}
}

func TestPutRefusalsAndErrors(t *testing.T) {
	cl := startDev(t, "")
	host := dial(t, sock(t, cl, "host.laptop", "chat"))
	node := dial(t, sock(t, cl, "laptop", "chat"))
	ctx := context.Background()
	nl, err := node.Serve(ctx, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.PutEntries(ctx, nl.Token, client.EntriesRequest{Entries: []client.Entry{{Name: "boss"}}, Reserved: []string{"root"}}); err != nil {
		t.Fatal(err)
	}
	hl, err := host.Serve(ctx, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	refused, err := host.PutEntries(ctx, hl.Token, client.EntriesRequest{Entries: []client.Entry{{Name: "boss"}, {Name: "root"}, {Name: "fine"}, {Name: "Bad Name"}}})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range refused {
		got[r.Name] = r.Reason
	}
	if got["boss"] != "held-by-node" || got["root"] != "reserved" || got["Bad Name"] != "grammar" || len(got) != 3 {
		t.Errorf("refused: %v", got)
	}
	// Too soon: the rate limit is busy, which a caller retries.
	if _, err := host.PutEntries(ctx, hl.Token, client.EntriesRequest{}); !client.IsCode(err, client.CodeBusy) || !client.IsRetryable(err) {
		t.Errorf("second PUT: %v", err)
	}
	if _, err := host.PutEntries(ctx, "no-such-lease", client.EntriesRequest{}); !client.IsCode(err, client.CodeGone) {
		t.Errorf("gone: %v", err)
	}
}

// TestOptionsAndCopies: options that are wrong are errors, and SetEntries copies.
func TestOptionsAndCopies(t *testing.T) {
	cl := startDev(t, "")
	path := sock(t, cl, "laptop", "chat")
	if _, err := client.Dial(path, client.WithServerUID(-1)); err == nil || !strings.Contains(err.Error(), "WithoutServerUIDCheck") {
		t.Errorf("a negative uid must be an error, got %v", err)
	}
	// SetEntries shares nothing with the caller: changing the slices after
	// the call changes nothing that is published.
	c := dial(t, path)
	srv := echoServer(t, c, client.ServeOptions{Version: 1})
	entries := []client.Entry{{Name: "kept", Data: []byte(`{"a":1}`)}}
	srv.SetEntries(client.EntriesRequest{Entries: entries, Reserved: []string{"r1"}})
	entries[0].Name = "changed"
	entries[0].Data[2] = 'z'
	watch := dial(t, sock(t, cl, "server", "chat"))
	eventually(t, "the entry as it was given", func() bool {
		v, err := watch.View(context.Background(), "", 0)
		if err != nil {
			return false
		}
		for _, e := range v.Entries {
			if e.Address == "kept@laptop" && string(e.Data) == `{"a":1}` {
				return true
			}
		}
		return false
	})
}

// A Timeout under a millisecond is a millisecond, not "no timeout".
func TestSubMillisecondTimeout(t *testing.T) {
	cl := startDev(t, "")
	c := dial(t, sock(t, cl, "sandbox.laptop", "chat"))
	_, err := c.Call(context.Background(), client.CallRequest{To: "laptop", Method: "m", Version: 1, Timeout: 100 * time.Microsecond})
	if err == nil || client.IsCode(err, client.CodeBadRequest) {
		t.Errorf("a 100 microsecond timeout: %v", err)
	}
}

// TestLostSessionReleases: a session that ends while its lease is still live
// gives the lease up with /release (a small fake Warren ends it by answering
// a poll with bad-request), instead of leaving it to run out its grace.
func TestLostSessionReleases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fake.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	released := make(chan string, 4)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/service/self", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"relay":"x","node":"x","service":"s","boot":"b"}`))
	})
	mux.HandleFunc("/v1/service/serve", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"lease":"tok-1","inflight":1}`))
	})
	mux.HandleFunc("/v1/service/calls", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Warren-Error", "bad-request")
		w.WriteHeader(400)
		w.Write([]byte(`{"error":"bad-request","detail":"nope"}`))
	})
	mux.HandleFunc("/v1/service/release", func(w http.ResponseWriter, r *http.Request) {
		released <- r.URL.Query().Get("lease")
		w.WriteHeader(204)
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	defer srv.Close()

	c := dial(t, path)
	s := c.NewServer(client.ServeOptions{Version: 1}, func(context.Context, *client.Call) ([]byte, error) { return nil, nil })
	err = s.Run(context.Background())
	if !client.IsCode(err, client.CodeBadRequest) {
		t.Fatalf("Run returned %v, want the bad-request that ended the session", err)
	}
	select {
	case tok := <-released:
		if tok != "tok-1" {
			t.Errorf("released %q", tok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the lease was not released when its session ended")
	}
}

// Entry data that is not JSON is refused where it is given, and by PutEntries
// as a bad-request, never retried forever.
func TestBadEntryData(t *testing.T) {
	cl := startDev(t, "")
	c := dial(t, sock(t, cl, "laptop", "chat"))
	srv := c.NewServer(client.ServeOptions{Version: 1}, func(context.Context, *client.Call) ([]byte, error) { return nil, nil })
	err := srv.SetEntries(client.EntriesRequest{Entries: []client.Entry{{Name: "a", Data: []byte(`{bad`)}}})
	if !client.IsCode(err, client.CodeBadRequest) {
		t.Errorf("SetEntries: %v", err)
	}
	l, err := c.Serve(context.Background(), 1, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.PutEntries(context.Background(), l.Token, client.EntriesRequest{Entries: []client.Entry{{Name: "a", Data: []byte(`{bad`)}}})
	if !client.IsCode(err, client.CodeBadRequest) {
		t.Errorf("PutEntries: %v", err)
	}
	if err := srv.SetEntries(client.EntriesRequest{Entries: []client.Entry{{Name: "a", Data: []byte(`{"ok":true}`)}}}); err != nil {
		t.Errorf("a good list: %v", err)
	}
}

var _ io.Closer = (*client.Client)(nil)

// A call that was written and then cut short has an unknown outcome; one that
// could not be sent is a plain, retryable error.
func TestCallLostAfterSendingIsUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hang.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan struct{}, 1)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- struct{}{}
		<-r.Context().Done() // never answers
	})}
	go srv.Serve(ln)
	c := dial(t, path)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-got; cancel() }()
	_, err = c.Call(ctx, client.CallRequest{To: "x", Method: "m", Version: 1})
	if !client.IsCode(err, client.CodeUnknown) || !client.IsMaybeDelivered(err) || client.IsRetryable(err) {
		t.Errorf("a call cut off after sending: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("the cause of the cut is lost: %v", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a cancelled call matches a deadline: %v", err)
	}
	srv.Close()
	// Nothing listens now: the request is not sent, so the error is plain.
	_, err = c.Call(context.Background(), client.CallRequest{To: "x", Method: "m", Version: 1})
	if err == nil || client.Code(err) != "" {
		t.Errorf("a call that could not be sent should be a plain error, got %v", err)
	}
}

// A handler that panics or returns an error gives the call up: the caller
// gets unknown at once, and the Server stays up and serves the next call.
func TestHandlerPanicOrErrorLeavesTheCallUnanswered(t *testing.T) {
	cl := startDev(t, "")
	server := dial(t, sock(t, cl, "laptop", "chat"))
	srv := server.NewServer(client.ServeOptions{Version: 1}, func(_ context.Context, c *client.Call) ([]byte, error) {
		switch c.Method {
		case "boom":
			panic("handler bug")
		case "fail":
			return []byte("not an answer"), errors.New("cannot do it")
		}
		return []byte("ok"), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	defer func() { cancel(); <-done }()
	caller := dial(t, sock(t, cl, "host.laptop", "chat"))
	eventually(t, "service", func() bool {
		_, err := caller.Call(context.Background(), client.CallRequest{To: "laptop", Method: "ok", Version: 1, Timeout: time.Second})
		return err == nil
	})
	for _, m := range []string{"boom", "fail"} {
		began := time.Now()
		out, err := caller.Call(context.Background(), client.CallRequest{To: "laptop", Method: m, Version: 1, Timeout: 10 * time.Second})
		if err == nil || !client.IsCode(err, client.CodeUnknown) || time.Since(began) > 3*time.Second {
			t.Errorf("%s: got %q, %v after %v; want unknown at once", m, out, err, time.Since(began))
		}
	}
	if out, err := caller.Call(context.Background(), client.CallRequest{To: "laptop", Method: "ok", Version: 1, Timeout: time.Second}); err != nil || string(out) != "ok" {
		t.Errorf("after the failures: %q %v", out, err)
	}
}

// A handler still running when the drain time passes is cut off, and what it
// returns afterwards is not sent as an answer: the caller gets unknown, not
// an empty or late success.
func TestDrainCutsOffHandlersWithoutAnswering(t *testing.T) {
	cl := startDev(t, "")
	server := dial(t, sock(t, cl, "laptop", "chat"))
	entered := make(chan struct{}, 1)
	srv := server.NewServer(client.ServeOptions{Version: 1, Drain: 100 * time.Millisecond}, func(ctx context.Context, c *client.Call) ([]byte, error) {
		if c.Method == "ok" {
			return []byte("ok"), nil
		}
		entered <- struct{}{}
		<-ctx.Done() // runs until the Server cuts it off
		return []byte("late"), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	caller := dial(t, sock(t, cl, "host.laptop", "chat"))
	eventually(t, "service", func() bool {
		_, err := caller.Call(context.Background(), client.CallRequest{To: "laptop", Method: "ok", Version: 1, Timeout: time.Second})
		return err == nil
	})
	type result struct {
		out []byte
		err error
	}
	res := make(chan result, 1)
	go func() {
		out, err := caller.Call(context.Background(), client.CallRequest{To: "laptop", Method: "slow", Version: 1, Timeout: 10 * time.Second})
		res <- result{out, err}
	}()
	<-entered
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the Server did not stop")
	}
	select {
	case r := <-res:
		if r.err == nil || !client.IsCode(r.err, client.CodeUnknown) {
			t.Errorf("the caller got %q, %v; want unknown", r.out, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the caller got nothing")
	}
}

// A view over MaxAnswer is ErrAnswerTooLarge, and a Watcher stops with it
// instead of asking again for ever.
func TestViewOverTheLimitIsAnErrorNotARetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fake.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	var asked atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/service/view", func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		chunk := make([]byte, 1<<20)
		for i := 0; i <= client.MaxAnswer>>20; i++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	defer srv.Close()
	c := dial(t, path)
	if _, err := c.View(context.Background(), "", 0); !errors.Is(err, client.ErrAnswerTooLarge) {
		t.Fatalf("View: %v", err)
	}
	asked.Store(0)
	w := c.NewWatcher()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := w.Next(ctx); !errors.Is(err, client.ErrAnswerTooLarge) {
		t.Fatalf("Next: %v", err)
	}
	if n := asked.Load(); n != 1 {
		t.Errorf("the watcher asked %d times", n)
	}
}

// A handler that always fails does not use up the places: with inflight 2,
// many calls each end as unknown quickly, none as busy.
func TestFailingHandlerFreesItsPlaces(t *testing.T) {
	cl := startDevYAML(t, "", strings.Replace(devYAML, "        namespaces: [web]\n", "        namespaces: [web]\n        inflight: 2\n", 1))
	server := dial(t, sock(t, cl, "laptop", "chat"))
	srv := server.NewServer(client.ServeOptions{Version: 1}, func(_ context.Context, c *client.Call) ([]byte, error) {
		if c.Method == "ok" {
			return []byte("ok"), nil
		}
		return nil, errors.New("always fails")
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	defer func() { cancel(); <-done }()
	caller := dial(t, sock(t, cl, "host.laptop", "chat"))
	eventually(t, "service", func() bool {
		_, err := caller.Call(context.Background(), client.CallRequest{To: "laptop", Method: "ok", Version: 1, Timeout: time.Second})
		return err == nil
	})
	began := time.Now()
	for i := 0; i < 12; i++ {
		_, err := caller.Call(context.Background(), client.CallRequest{To: "laptop", Method: "bad", Version: 1, Timeout: 30 * time.Second})
		if !client.IsCode(err, client.CodeUnknown) {
			t.Fatalf("call %d: %v, want unknown", i, err)
		}
	}
	if d := time.Since(began); d > 10*time.Second {
		t.Errorf("12 failing calls took %v", d)
	}
}
