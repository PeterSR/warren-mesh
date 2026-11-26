// Package transport is the conformance harness of the transport API
// (/v1/transport, documented in docs/transport-api.md). It runs a transport
// binary the way Warren does, on inherited descriptors 3 (link A) and 4
// (link B), against a fake Warren, and checks what the contract requires of a
// transport: hello, the peers long poll, carrying (methods, paths, the
// allow-listed headers both ways, the X-Warren-Transport-* headers set and
// stripped), the transport errors not-sent, lost, busy and too-big,
// deadlines, max_body, that neither managed link is closed while idle,
// and that the process made itself non-dumpable.
//
// A transport carries requests over a network, so the harness needs a
// Network: a fixture that starts nodes of that network next to each other,
// each with its own transport command. For warren-tailscale the fixture runs
// the binary against a fake tailscaled on loopback addresses (see the test
// of this package); another transport supplies its own. Two transports of
// one Network carry peer traffic to each other, with the fake Warren of one
// node as the sender and the fake Warren of the other as the receiver.
//
// What the harness cannot see from outside, and the contract still asks for:
// that PR_SET_DUMPABLE is the very first thing the process does (it checks
// the result, not the order), and write deadlines on answers sent to the
// network.
package transport
