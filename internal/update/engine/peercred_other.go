//go:build !linux

package engine

import "net"

// peerAllowed cannot read credentials here; the socket's mode (0600, in a 0700
// directory) is the protection.
func peerAllowed(net.Conn) bool { return true }
