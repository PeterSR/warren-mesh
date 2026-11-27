package socketdir

import (
	"context"
	"net"

	"github.com/PeterSR/warren-mesh/internal/platform"
)

type peerUIDKey struct{}

// withPeerCred records the uid of the process at the other end of a unix
// socket connection, as the kernel reports it.
func withPeerCred(ctx context.Context, c net.Conn) context.Context {
	if uid, ok := ConnUID(c); ok {
		return context.WithValue(ctx, peerUIDKey{}, uid)
	}
	return ctx
}

// ConnUID is the uid of the process at the other end of a unix socket
// connection, as seen from this process's user namespace. It looks through
// the connection counting wrapper this package puts around accepted ones.
func ConnUID(c net.Conn) (int, bool) {
	if cc, ok := c.(*countedConn); ok {
		c = cc.Conn
	}
	return platform.ConnUID(c)
}

// PeerUID is the caller's uid for a request served by a Group, if known.
func PeerUID(ctx context.Context) (int, bool) {
	uid, ok := ctx.Value(peerUIDKey{}).(int)
	return uid, ok
}
