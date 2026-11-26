package client_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/PeterSR/warren-mesh/client"
)

// These examples are compiled but not run: they need a Warren. Run `warren
// dev --config examples/dev.yaml`, point WARREN_SERVICE_SOCKET at one of the
// sockets it prints, and use the code as it is. The same flows run against a
// real Warren in client_test.go.

// Connect to the socket Warren gives a managed service (or to a path), and
// ask who it is.
func ExampleDial() {
	c, err := client.Dial("") // "" reads WARREN_SERVICE_SOCKET; a path works too
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	self, err := c.Self(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(self.Service, "at", self.Relay, "publishes under", self.EntryNamespace)
}

// A service: serve, answer calls, and publish entries. Run mends itself
// across lost leases and Warren restarts until its context ends.
func ExampleServer() {
	c, err := client.Dial("")
	if err != nil {
		log.Fatal(err)
	}
	srv := c.NewServer(client.ServeOptions{Version: 1}, func(ctx context.Context, call *client.Call) ([]byte, error) {
		// The answer reaches the caller as a body only.
		return []byte("pong from " + call.To + ", asked by " + call.Origin), nil
	})
	srv.SetEntries(client.EntriesRequest{Entries: []client.Entry{{Name: "alice", Data: []byte(`{"kind":"example"}`)}}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Run(ctx); err != nil { // returns nil once ctx ends
		log.Fatal(err)
	}
}

// Call another service of the same name, here on another node, and decide
// what to do with a failure from its code.
func ExampleClient_Call() {
	c, err := client.Dial("")
	if err != nil {
		log.Fatal(err)
	}
	_, err = c.Call(context.Background(), client.CallRequest{To: "alice@laptop", Method: "hello", Version: 1, From: "bob", Timeout: 5 * time.Second})
	switch {
	case err == nil:
		fmt.Println("answered")
	case client.IsRetryable(err):
		// busy, offline, not-ready, no-service: the call did not happen.
		fmt.Println("try again later:", client.Code(err))
	case client.IsMaybeDelivered(err):
		fmt.Println("unknown outcome:", client.Code(err))
	default:
		fmt.Println("give up:", err)
	}
}

// Errors carry the code of the error table. This one needs no Warren.
func ExampleError() {
	err := error(&client.Error{Code: client.CodeBusy, Detail: "the service's inflight places are full", Status: 429})
	var e *client.Error
	if errors.As(err, &e) {
		fmt.Println(e.Code, e.Retryable(), e.Advice() == client.RetryWithBackoff)
	}
	fmt.Println(errors.Is(err, &client.Error{Code: client.CodeBusy}))
	// Output:
	// busy true true
	// true
}

// Follow the entries of the mesh; Next returns each new version, and says
// when Warren restarted.
func ExampleWatcher_Next() {
	c, err := client.Dial("")
	if err != nil {
		log.Fatal(err)
	}
	w := c.NewWatcher()
	for {
		view, err := w.Next(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		if view.BootChanged {
			fmt.Println("Warren restarted: serve and PUT again")
		}
		for _, e := range view.Entries {
			fmt.Println(e.Address, "from", e.Origin, "stale:", e.Stale)
		}
	}
}
