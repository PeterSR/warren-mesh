package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/PeterSR/warren-mesh/internal/dev"
)

// runDev is `warren dev`: the nodes of a dev.yaml in this process, on the
// in-memory transport, until SIGINT or SIGTERM.
func runDev(args []string, out, errw io.Writer) int {
	fs := flag.NewFlagSet("warren dev", flag.ContinueOnError)
	fs.SetOutput(errw)
	cfgPath := fs.String("config", "dev.yaml", "the dev configuration: nodes, children and services")
	dir := fs.String("dir", "", "keep sockets and state in this directory (default: a temporary one, removed at exit)")
	verbose := fs.Bool("v", false, "log what the nodes do")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg, err := dev.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(errw, "warren dev:", err)
		return 1
	}
	level := slog.LevelWarn
	if *verbose {
		level = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(errw, &slog.HandlerOptions{Level: level}))
	// Registered before the nodes start, so that an early signal still
	// shuts them down in order.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sig)
	cl, err := dev.Start(cfg, *dir, log)
	if err != nil {
		fmt.Fprintln(errw, "warren dev:", err)
		return 1
	}
	cl.Print(out)
	<-sig
	fmt.Fprintln(out, "\nshutting down")
	cl.Stop()
	return 0
}
