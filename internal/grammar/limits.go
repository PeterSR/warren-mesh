package grammar

import "time"

// The limits of the contract (docs/architecture.md, "Limits and fairness").
// They are defined here once: the service, the config, the transport and the
// public client all take their values from this file, so no copy can drift.
const (
	// MaxCallBody is the most one call's request or response may carry.
	MaxCallBody = 1 << 20
	// MaxPutBody is the most a PUT of entries may carry.
	MaxPutBody = 4 << 20
	// MaxEntryData is the most data one entry holds.
	MaxEntryData = 3 << 10
	// MaxPutEntries is the most entries one PUT holds.
	MaxPutEntries = 1000
	// MaxPutReserved is the most reserved names one PUT holds.
	MaxPutReserved = 64
	// MinPutInterval is the least time between two PUTs of one service.
	MinPutInterval = time.Second
	// MaxViewPolls is the most view polls a service socket holds.
	MaxViewPolls = 4
	// MaxInflight is the most inflight a socket has (and so polls per lease).
	MaxInflight = 32
	// MaxWaitSeconds is the longest a long poll may wait, in whole seconds (the
	// unit of the wait parameter).
	MaxWaitSeconds = 25
	// MaxWait is MaxWaitSeconds as a duration.
	MaxWait = MaxWaitSeconds * time.Second
)
