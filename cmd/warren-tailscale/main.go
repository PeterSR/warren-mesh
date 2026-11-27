// Command warren-tailscale is Warren's transport for a tailnet. It speaks
// /v1/transport to Warren and plain HTTP to the same transport on other
// nodes, and asks tailscaled's local API who is who.
//
// Started by Warren (`transport: tailscale`) it is given two connected
// sockets as inherited descriptors, 3 (link A, which it serves) and 4 (link B,
// which Warren serves), and its settings in WARREN_TRANSPORT_SETTINGS as JSON
// (tag, listen, port, socket). Started by something else it is reached through
// two socket paths:
//
//	warren-tailscale --link-a /run/x/a.sock --link-b /run/x/b.sock --warren-uid 1000
//
// as a different user than Warren, with the same settings in the environment.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/PeterSR/warren-mesh/internal/platform"
	"github.com/PeterSR/warren-mesh/internal/tailscale"
	"github.com/PeterSR/warren-mesh/internal/version"
)

func main() { os.Exit(run(os.Args[1:], os.Environ(), os.Stderr)) }

func run(args, environ []string, errw io.Writer) int {
	// First thing: other processes of this user must not be able to attach
	// and copy the descriptors that are this transport's link to Warren. The
	// setting does not survive exec, so every transport makes this call itself.
	if err := platform.SetNonDumpable(); err != nil {
		fmt.Fprintln(errw, "warren-tailscale: cannot make the process non-dumpable:", err)
		return 1
	}
	fs := flag.NewFlagSet("warren-tailscale", flag.ContinueOnError)
	fs.SetOutput(errw)
	linkA := fs.String("link-a", "", "external form: the socket this transport serves (link A)")
	linkB := fs.String("link-b", "", "external form: the socket Warren serves (link B)")
	warrenUID := fs.Int("warren-uid", -1, "external form: the user Warren runs as")
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintln(errw, version.Version)
		return 0
	}
	log := slog.New(slog.NewTextHandler(errw, nil))
	var set tailscale.Settings
	for _, kv := range environ {
		if v, ok := cutPrefix(kv, "WARREN_TRANSPORT_SETTINGS="); ok && v != "" {
			if err := json.Unmarshal([]byte(v), &set); err != nil {
				fmt.Fprintln(errw, "warren-tailscale: WARREN_TRANSPORT_SETTINGS is not the settings JSON:", err)
				return 2
			}
		}
	}
	if set.Tag == "" {
		fmt.Fprintln(errw, "warren-tailscale: the tailscale tag is not set (tailscale.tag in Warren's config)")
		return 2
	}
	t := tailscale.New(set, log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	var err error
	if *linkA != "" || *linkB != "" {
		if *linkA == "" || *linkB == "" {
			fmt.Fprintln(errw, "warren-tailscale: --link-a and --link-b go together")
			return 2
		}
		err = t.RunExternal(ctx, *linkA, *linkB, *warrenUID)
	} else {
		var a, b net.Conn
		if a, err = fdConn(3, "link-a"); err == nil {
			b, err = fdConn(4, "link-b")
		}
		if err != nil {
			fmt.Fprintln(errw, "warren-tailscale: no links: start it from Warren (transport: tailscale) or give --link-a and --link-b:", err)
			return 2
		}
		err = t.RunManaged(ctx, a, b)
	}
	if err != nil {
		fmt.Fprintln(errw, "warren-tailscale:", err)
		return 1
	}
	return 0
}

func cutPrefix(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):], true
	}
	return "", false
}

// fdConn makes a connection of an inherited socket descriptor.
func fdConn(fd uintptr, name string) (net.Conn, error) {
	f := os.NewFile(fd, name)
	if f == nil {
		return nil, fmt.Errorf("descriptor %d is not open", fd)
	}
	defer f.Close() // FileConn duplicates it
	return net.FileConn(f)
}
