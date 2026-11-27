//go:build !linux

package platform

// DefaultUpdatableEnv cannot tell on this system: Check reports unknown.
func DefaultUpdatableEnv() UpdatableEnv { return UpdatableEnv{} }
