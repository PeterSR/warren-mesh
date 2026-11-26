package conformance

import (
	"os"
	"sort"
	"sync"
	"testing"
)

// Options say how Run runs the suite.
type Options struct {
	// RealTimers runs the Warren with the contract's real timers (a few
	// minutes) instead of the fast ones of `fast: true`.
	RealTimers bool
}

// Run starts a Warren with the launcher and checks the whole service
// contract.
func Run(t *testing.T, l Launcher, o Options) {
	real := o.RealTimers
	tm := fastTiming
	if real {
		tm = realTiming
	}
	dir, err := os.MkdirTemp("", "wconf")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	root := dir + "/w"
	sh := &shared{l: l, real: real, tm: tm, seen: map[string]bool{}, mu: &sync.Mutex{}}
	sh.in = l.Launch(t, Topology(!real), root, tm)

	checks := []struct {
		name string
		fn   func(*env)
	}{
		{"conventions", (*env).conventions},
		{"parameters", (*env).parameters},
		{"connection-limit", (*env).connectionLimit},
		{"self-and-status", (*env).selfAndStatus},
		{"serve-and-release", (*env).serveAndRelease},
		{"polls", (*env).polls},
		{"calls-and-answers", (*env).callsAndAnswers},
		{"answer-rules", (*env).answerRules},
		{"give-up", (*env).giveUp},
		{"check-order", (*env).checkOrder},
		{"call-parameters", (*env).callParameters},
		{"call-limits", (*env).callLimits},
		{"from", (*env).fromChecks},
		{"only-the-body-travels", (*env).onlyTheBody},
		{"cross-node", (*env).crossNode},
		{"cross-node-leases", (*env).crossNodeLeases},
		{"entries-put", (*env).entriesPut},
		{"entries-limits", (*env).entriesLimits},
		{"entries-precedence", (*env).entriesPrecedence},
		{"entries-reasons", (*env).entriesReasons},
		{"view", (*env).viewChecks},
		{"view-polls", (*env).viewPolls},
		{"pickup", (*env).pickup},
	}
	for _, c := range checks {
		c := c
		t.Run(c.name, func(t *testing.T) { c.fn(&env{t: t, shared: sh}) })
	}
	// The checks that wait out the real timers run side by side, each on a
	// relay of its own.
	t.Run("timers", func(t *testing.T) {
		for _, c := range []struct {
			name string
			fn   func(*env)
		}{
			{"lease-grace", (*env).leaseGrace},
			{"lease-end-cases", (*env).leaseEndCases},
			{"held-calls-and-grace", (*env).heldCallsAndGrace},
			{"stale-entries", (*env).staleEntries},
			{"node-down", (*env).nodeDown},
		} {
			c := c
			t.Run(c.name, func(t *testing.T) {
				t.Parallel()
				c.fn(&env{t: t, shared: sh})
			})
		}
	})
	// Instances of their own: they restart or shut down.
	t.Run("restart", func(t *testing.T) { (&env{t: t, shared: sh}).restart(root) })
	t.Run("shutdown", func(t *testing.T) { (&env{t: t, shared: sh}).shutdown(root) })

	t.Run("every-error-code-was-seen", func(t *testing.T) {
		var missing []string
		for code := range errorTable {
			if !sh.seen[code] {
				missing = append(missing, code)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			if sh.in.Down == nil {
				t.Logf("not seen (a launcher that cannot take a node down cannot show offline): %v", missing)
				for _, m := range missing {
					if m != "offline" {
						t.Errorf("error code %s was never seen", m)
					}
				}
				return
			}
			t.Errorf("error codes never seen: %v", missing)
		}
	})
}
