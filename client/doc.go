// Package client is a Go client of Warren's service socket API
// (/v1/service), documented in docs/service-api.md.
//
// A program becomes service S at a relay by reaching S's socket. Connect with
// Dial, which takes the socket path or, when the path is empty, the
// WARREN_SERVICE_SOCKET environment variable that Warren sets for the
// services it runs. If WARREN_UID is set (or WithServerUID is used), every
// connection checks with SO_PEERCRED that the socket is served by that user,
// so a socket planted by someone else is never used.
//
//	c, err := client.Dial("")
//	self, err := c.Self(ctx)
//
// To serve calls, make a Server: it takes the lease, runs up to the lease's
// inflight polls in parallel, answers, publishes entries, and keeps going
// across Warren restarts and lease losses, as the contract asks of a
// service (re-serve on gone, back off on busy, PUT again after every serve).
//
//	srv := c.NewServer(client.ServeOptions{Version: 1}, func(ctx context.Context, call *client.Call) ([]byte, error) {
//		return call.Body, nil // echo
//	})
//	srv.SetEntries(client.EntriesRequest{Entries: []client.Entry{{Name: "alice"}}})
//	err := srv.Run(ctx)
//
// To call another service of the same name, anywhere in the mesh, use Call.
// Any process on a socket may call, with or without a lease. To follow the
// entries of the mesh, use a Watcher.
//
// The client reads at most MaxAnswer (64 MiB) of any one answer, which is
// room for the view of a large mesh. A larger one is ErrAnswerTooLarge, and a
// Watcher returns it instead of asking again.
//
// Every failure the server reports is an *Error that carries the code of the
// error table; IsRetryable and the other helpers say what a caller should do.
// A call that was written to the socket and then lost (the context ended, the
// connection dropped) is an *Error with the code unknown, since the service may
// have received it; the cause, such as context.DeadlineExceeded, is the
// error's Err, so errors.Is finds it. For that reason errors.Is and errors.As
// on a client Error may match a call that was sent and then lost: check
// IsMaybeDelivered before treating a context or network error as "not sent".
// Failures before anything was sent (the socket cannot be reached) are
// ordinary errors from the net package and safe to retry.
package client
