//go:build !linux && !darwin

package platform

import "net"

// MaxSunPath is a conservative limit for systems with shorter sun_path.
const MaxSunPath = 103

// ConnUID cannot tell the user on this system, so a uid requirement never
// matches and a link with a transport is refused.
func ConnUID(net.Conn) (int, bool) { return 0, false }

// SetNonDumpable does nothing here; see the darwin note.
func SetNonDumpable() error { return nil }

// Dumpable is always true here.
func Dumpable() (bool, error) { return true, nil }

// SetDumpable does nothing here.
func SetDumpable() error { return nil }

// SetSubreaper does nothing here.
func SetSubreaper() error { return nil }

// PeekExited is not available here: supported is false, and the caller
// reaps first and acts on the group after (a pid could in theory have been
// reused in between).
func PeekExited() (pid int, found, supported bool) { return 0, false, false }
