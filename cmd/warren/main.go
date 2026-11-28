// Command warren is the per-machine daemon of the mesh.
//
//	warren run [--config path]      run the node
//	warren status [--config path]   show a running node (non-zero if unreachable)
//	warren status --health          exit 0 healthy, 1 unhealthy, 2 unreachable
//	  (status, reload and drain-and-exit also take --control path)
//	warren reload [--config path]   apply the config file without a restart
//	warren drain-and-exit           finish calls, stop everything and exit
//	warren dev --config dev.yaml    several nodes in one process on the in-memory
//	  transport, with real service sockets (--dir to keep them somewhere, -v to
//	  log what the nodes do)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/PeterSR/warren-mesh/internal/config"
	"github.com/PeterSR/warren-mesh/internal/daemon"
	"github.com/PeterSR/warren-mesh/internal/service"
	"github.com/PeterSR/warren-mesh/internal/supervise"
)

const defaultConfig = "/etc/warren/config.yaml"

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, out, errw io.Writer) int {
	if len(args) == 0 {
		usage(errw)
		return 2
	}
	cmd, rest := args[0], args[1:]
	if cmd == "dev" {
		return runDev(rest, out, errw) // dev has its own flags
	}
	fs := flag.NewFlagSet("warren "+cmd, flag.ContinueOnError)
	fs.SetOutput(errw)
	cfgPath := fs.String("config", defaultConfig, "path of the configuration file")
	// --control and --health belong to the commands that talk to a running
	// Warren, and are registered only for them.
	var control *string
	var health *bool
	switch cmd {
	case "status", "reload", "drain-and-exit":
		control = fs.String("control", "", "the control socket, instead of the one the config names")
	}
	if cmd == "status" {
		health = fs.Bool("health", false, "exit 0 when the config is loaded and the transport link is up, 1 if not, 2 if Warren cannot be reached")
	}
	switch cmd {
	case "run":
		if err := fs.Parse(rest); err != nil {
			return 2
		}
		// First thing: other processes of this user must not be able to
		// attach to Warren or copy the descriptors it hands its transport.
		if err := supervise.Harden(); err != nil {
			fmt.Fprintln(errw, "warren: cannot make the process non-dumpable:", err)
			return 1
		}
		log := slog.New(slog.NewTextHandler(errw, nil))
		code, err := daemon.Run(context.Background(), daemon.Options{ConfigPath: *cfgPath, Log: log})
		if err != nil {
			fmt.Fprintln(errw, "warren:", err)
		}
		return code
	case "status", "reload", "drain-and-exit":
		if err := fs.Parse(rest); err != nil {
			return 2
		}
		ctl := *control
		if ctl == "" {
			// Only what the control path needs is expanded: a variable
			// missing elsewhere in the file must not hide a running Warren.
			var err error
			if ctl, err = config.ControlPath(*cfgPath); err != nil {
				fmt.Fprintln(errw, "warren:", err)
				if health != nil && *health {
					return 2 // Warren's state cannot be read, the same as unreachable
				}
				return 1
			}
		}
		if cmd == "drain-and-exit" {
			if _, err := daemon.Control(ctl, "POST", "/drain-and-exit"); err != nil {
				fmt.Fprintln(errw, "warren:", err)
				return 1
			}
			fmt.Fprintln(out, "draining")
			return 0
		}
		if cmd == "reload" {
			if _, err := daemon.Control(ctl, "POST", "/reload"); err != nil {
				fmt.Fprintln(errw, "warren: reload:", err)
				return 1
			}
			fmt.Fprintln(out, "reloaded")
			return 0
		}
		b, err := daemon.Control(ctl, "GET", "/status")
		if err != nil {
			fmt.Fprintln(errw, "warren:", err)
			if health != nil && *health {
				return 2
			}
			return 1
		}
		if health != nil && *health {
			var st struct{ Healthy bool }
			if json.Unmarshal(b, &st) != nil || !st.Healthy {
				fmt.Fprintln(out, "unhealthy: the transport link is down")
				return 1
			}
			fmt.Fprintln(out, "healthy")
			return 0
		}
		printStatus(out, b)
		return 0
	case "version":
		fmt.Fprintln(out, service.Version)
		return 0
	}
	usage(errw)
	return 2
}

func usage(w io.Writer) {
	fmt.Fprint(w, `usage: warren <command> [flags]

  run [--config path]                     run the node
  status [--health] [--control path] [--config path]
                                          show the running node; --health prints
                                          healthy or unhealthy and sets the exit
                                          code (0 healthy, 1 not, 2 not
                                          reachable)
  reload [--control path] [--config path] apply the config without a restart
  drain-and-exit [--control path] [--config path]
                                          stop handing out calls, then exit
  dev --config dev.yaml [--dir path] [-v] several nodes in one process, for
                                          testing services (-v logs what the
                                          nodes do)
  version                                 print the version

The config defaults to `+defaultConfig+`.
`)
}

// printStatus writes a short human summary of the control socket's status.
func printStatus(w io.Writer, raw []byte) {
	var st struct {
		Node, Boot string
		Warren     struct {
			Version      string
			PeerProtocol []int `json:"peer_protocol"`
			Features     []string
			Updatable    *bool  `json:"updatable"`
			NotUpdatable string `json:"not_updatable"`
		}
		Transport struct{ Name, Version string }
		Peers     []struct {
			Node string
			Up   bool
		}
		Children []struct {
			Label string
			Flat  bool
		}
		Services []struct {
			Relay, Name string
			Lease       bool
		}
		Processes []struct {
			Name     string
			State    string
			LastExit string `json:"last_exit"`
			PID      int
			Restarts int
			// Updatable is a pointer: where the system cannot say, or an older
			// Warren does not, nothing is shown.
			Updatable    *bool  `json:"updatable"`
			NotUpdatable string `json:"not_updatable"`
		}
		Pinned      string
		MemoryBound int64 `json:"memory_bound"`
		BoundPeers  int   `json:"memory_bound_peers"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		fmt.Fprintf(w, "%s\n", raw)
		return
	}
	fmt.Fprintf(w, "node %s (boot %s)\n", st.Node, st.Boot[:min(8, len(st.Boot))])
	fmt.Fprintf(w, "warren %s, peer protocol %v, features %v\n", st.Warren.Version, st.Warren.PeerProtocol, st.Warren.Features)
	if st.Warren.Updatable != nil && !*st.Warren.Updatable {
		fmt.Fprintf(w, "warren binary: not updatable: %s\n", printable(st.Warren.NotUpdatable))
	}
	if st.Pinned != "" {
		fmt.Fprintf(w, "pinned: %s\n", printable(st.Pinned))
	}
	if st.Transport.Name != "" {
		fmt.Fprintf(w, "transport %s\n", st.Transport.Name)
	}
	var peers []string
	for _, p := range st.Peers {
		peers = append(peers, fmt.Sprintf("%s(up=%t)", p.Node, p.Up))
	}
	fmt.Fprintf(w, "peers: %s\n", orNone(strings.Join(peers, " ")))
	var kids []string
	for _, c := range st.Children {
		s := c.Label
		if c.Flat {
			s += "(flat)"
		}
		kids = append(kids, s)
	}
	fmt.Fprintf(w, "children: %s\n", orNone(strings.Join(kids, " ")))
	for _, s := range st.Services {
		fmt.Fprintf(w, "service %s at %s: lease=%t\n", s.Name, s.Relay, s.Lease)
	}
	for _, p := range st.Processes {
		fmt.Fprintf(w, "process %s: %s", p.Name, p.State)
		if p.LastExit != "" {
			fmt.Fprintf(w, " (last exit: %s, %d restarts)", p.LastExit, p.Restarts)
		}
		if p.Updatable != nil && !*p.Updatable {
			fmt.Fprintf(w, " [not updatable: %s]", printable(p.NotUpdatable))
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintf(w, "memory bound: %d MiB (assuming %d peers)\n", st.MemoryBound>>20, st.BoundPeers)
}

// printable escapes what should not reach a terminal as it is: control,
// format and line separator characters, and anything that is not valid text.
func printable(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r == utf8.RuneError || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) {
			fmt.Fprintf(&b, "\\u%04x", r)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
