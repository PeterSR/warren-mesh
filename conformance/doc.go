// Package conformance checks that a Warren speaks the /v1/service contract
// of docs/service-api.md: every endpoint, every error code, the order of the
// checks, leases and their end cases, entries with their precedence, loss
// and staleness, the view's since and boot, and the limits.
//
// The suite starts a real Warren with `warren dev` (in this process, or the
// binary at a path) on a fixed topology of two nodes, and talks to its
// service sockets with plain HTTP, so it checks the wire and not a client
// library. The only service is a tiny echo service that lives here.
//
// Run it against the Warren of this repository with
//
//	go test ./conformance
//
// or against a built binary (a third party's, or a release):
//
//	go test ./conformance -args -warren /path/to/warren
//
// A binary must support `warren dev --config FILE --dir DIR` and the `fast`
// option of the dev config (shorter timers; see docs/service-api.md for the
// real values). With -real the suite runs with the contract's real timings,
// which takes a few minutes.
//
// The package is also the reference for the other conformance tools: the
// topology a dev.yaml needs to run it is Topology.
package conformance
