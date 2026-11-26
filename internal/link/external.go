package link

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/PeterSR/warren-mesh/internal/socketdir"
)

// External is the link to a transport someone else started, reached through
// two sockets: link_a, served by the transport, and link_b, served by
// Warren. It must run as another user than Warren, and Warren refuses a
// link where both sides are the same user: otherwise any process of Warren's
// user could be the transport, or talk to it.
type External struct {
	*Client
	in    *Incoming
	group *socketdir.Group
	linkB string
	uid   int
	wants []socketdir.Want
}

// NewExternal makes the link and starts serving link B. uid is the only user
// allowed to be the transport, on both links; it must not be Warren's own.
func NewExternal(node, linkA, linkB string, uid int, log *slog.Logger) (*External, error) {
	if uid < 0 {
		return nil, errors.New("the transport uid is required")
	}
	if uid == os.Geteuid() {
		return nil, errors.New("the transport uid is Warren's own; a transport must run as another user")
	}
	e := &External{in: NewIncoming(node), group: socketdir.NewGroup(log), linkB: linkB, uid: uid}
	e.Client = NewClient(func(ctx context.Context) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, "unix", linkA)
		if err != nil {
			return nil, err
		}
		if err := e.checkUser(c); err != nil {
			c.Close()
			return nil, fmt.Errorf("link_a %s: %w", linkA, err)
		}
		return c, nil
	})
	e.wants = []socketdir.Want{{
		Key:  "link_b",
		Path: linkB,
		// Mode stays 0666: the accept-time uid check closes every connection but
		// the transport's, so the transport needs no group of Warren's.
		Handler: e.in,
		H2C:     true,
		Admit:   func(uid int, known bool) bool { return known && e.userOK(uid) },
	}}
	failed := e.group.Sync(e.wants)
	if err := failed["link_b"]; err != nil {
		return nil, fmt.Errorf("link_b: %w", err)
	}
	return e, nil
}

// userOK is whether uid is the configured transport user, which is never
// Warren's own.
func (e *External) userOK(uid int) bool {
	return uid != os.Geteuid() && uid == e.uid
}

func (e *External) checkUser(c net.Conn) error {
	uid, ok := socketdir.ConnUID(c)
	switch {
	case !ok:
		return errors.New("cannot tell which user serves it")
	case uid == os.Geteuid():
		return errors.New("served by Warren's own user; a transport must run as another user")
	case !e.userOK(uid):
		return fmt.Errorf("served by uid %d, not the configured transport uid %d", uid, e.uid)
	}
	return nil
}

// Resync serves link B again if its socket file has been removed or
// replaced. The node calls it from its periodic scan.
func (e *External) Resync() { e.group.Sync(e.wants) }

// Handle implements Link.
func (e *External) Handle(h http.Handler) { e.in.Set(h) }

// Close implements Link.
func (e *External) Close() error {
	e.group.Close()
	return e.Client.Close()
}
