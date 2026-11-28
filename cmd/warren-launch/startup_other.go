//go:build !linux

package main

func checkDedicatedProcess(int) error { return nil }
