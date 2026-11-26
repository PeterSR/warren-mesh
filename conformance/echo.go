package conformance

import (
	"context"
	"fmt"
	"time"

	"github.com/PeterSR/warren-mesh/client"
)

// echoAnswer is what the echo service answers: everything the contract hands
// a service about a call, so a check can see it from the caller's side.
func echoAnswer(c *client.Call) string {
	return fmt.Sprintf("origin=%s from=%s to=%s method=%s body=%s", c.Origin, c.From, c.To, c.Method, c.Body)
}

// startEcho runs the tiny test-only echo service on a socket, until the test
// ends: it serves version 1, answers every call with echoAnswer, and
// publishes the entries it is given. It is the Go client in use, not part of
// what is checked.
func (e *env) startEcho(s *sc, entries ...client.Entry) *client.Server {
	e.t.Helper()
	c, err := client.Dial(s.path)
	if err != nil {
		e.t.Fatal(err)
	}
	srv := c.NewServer(client.ServeOptions{Version: 1, Drain: 200 * time.Millisecond},
		func(_ context.Context, call *client.Call) ([]byte, error) { return []byte(echoAnswer(call)), nil })
	if entries != nil {
		if err := srv.SetEntries(client.EntriesRequest{Entries: entries}); err != nil {
			e.t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Run(ctx); close(done) }()
	e.t.Cleanup(func() {
		cancel()
		<-done
		c.Close()
	})
	return srv
}
