// Command warren-update installs signed releases of components that Warren
// runs as services. It is an ordinary managed Warren service (`run`) plus a
// command line that talks to it.
//
//	warren-update run --config path                 the service (started by Warren)
//	warren-update install <component> --from file://DIR [--version v] [--allow-downgrade]
//	                      [--force] [--dry-run] [--wait]
//	warren-update revert [--offline] <component>
//	warren-update status [--json]
//	warren-update reload
//	warren-update version
//
// It installs service components from a directory. See docs/updates.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/PeterSR/warren-mesh/internal/update/engine"
	"github.com/PeterSR/warren-mesh/internal/update/state"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
	"github.com/PeterSR/warren-mesh/internal/version"
)

const defaultConfig = "/etc/warren/update.yaml"

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

// parse parses flags that may come before or after the positional arguments.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func run(args []string, out, errw io.Writer) int {
	if len(args) == 0 {
		usage(errw)
		return 2
	}
	cmd, rest := args[0], args[1:]
	fs := flag.NewFlagSet("warren-update "+cmd, flag.ContinueOnError)
	fs.SetOutput(errw)
	cfgPath := fs.String("config", defaultConfig, "path of the updater's configuration file")
	switch cmd {
	case "version":
		fmt.Fprintln(out, version.Version)
		return 0
	case "run":
		if err := fs.Parse(rest); err != nil {
			return 2
		}
		return cmdRun(*cfgPath, errw)
	case "install":
		var from multiFlag
		fs.Var(&from, "from", "source: file://DIR; may be given more than once, tried in order")
		ver := fs.String("version", "", "install this exact version instead of the latest")
		down := fs.Bool("allow-downgrade", false, "install an older, validly signed version")
		force := fs.Bool("force", false, "skip prudence (the bad list, \"healthy before\", a failed record); never the mutex, the version floor, or any check of signature, key or path")
		dry := fs.Bool("dry-run", false, "plan and report, change nothing")
		wait := fs.Bool("wait", false, "wait until the component is healthy, committed, reverted or failed")
		pos, err := parse(fs, rest)
		if err != nil {
			return 2
		}
		if len(pos) != 1 {
			fmt.Fprintln(errw, "warren-update install: name one component")
			return 2
		}
		return cmdInstall(*cfgPath, pos[0], from, *ver, engine.InstallRequest{AllowDowngrade: *down, Force: *force, DryRun: *dry}, *wait, out, errw)
	case "revert":
		offline := fs.Bool("offline", false, "swap the files back with no updater running; touches no process")
		pos, err := parse(fs, rest)
		if err != nil {
			return 2
		}
		if len(pos) != 1 {
			fmt.Fprintln(errw, "warren-update revert: name one component")
			return 2
		}
		return cmdRevert(*cfgPath, pos[0], *offline, out, errw)
	case "status":
		asJSON := fs.Bool("json", false, "print the status as JSON")
		if err := fs.Parse(rest); err != nil {
			return 2
		}
		return cmdStatus(*cfgPath, *asJSON, out, errw)
	case "reload":
		if err := fs.Parse(rest); err != nil {
			return 2
		}
		return cmdReload(*cfgPath, out, errw)
	}
	usage(errw)
	return 2
}

func usage(w io.Writer) {
	fmt.Fprint(w, `usage: warren-update <command> [flags]

  run [--config path]                  the service (started by Warren)
  install <component> --from file://DIR [--from ...] [--version v]
          [--allow-downgrade] [--force] [--dry-run] [--wait] [--config path]
                                       install a signed release (--from may
                                       be repeated; --force also passes a
                                       failed record)
  revert [--offline] <component> [--config path]
                                       go back to the previous version
  status [--json] [--config path]      show the components
  reload [--config path]               read the config again
  version                              print the version

The config defaults to `+defaultConfig+`.
`)
}

func cmdRun(cfgPath string, errw io.Writer) int {
	log := slog.New(slog.NewTextHandler(errw, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	err := engine.Run(ctx, engine.RunOptions{ConfigPath: cfgPath, Version: version.Version, Log: log,
		Reload: func(reload func() error) {
			go func() {
				for {
					select {
					case <-hup:
						if err := reload(); err != nil {
							log.Error("reload refused", "err", err)
						} else {
							log.Info("reloaded")
						}
					case <-ctx.Done():
						return
					}
				}
			}()
		}})
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(errw, "warren-update:", err)
		return 1
	}
	return 0
}

func loadConfig(path string, errw io.Writer) (*uconfig.Config, bool) {
	cfg, err := uconfig.LoadCLI(path)
	if err != nil {
		fmt.Fprintln(errw, "warren-update:", err)
		return nil, false
	}
	return cfg, true
}

func socketOf(cfg *uconfig.Config) string { return filepath.Join(cfg.StateDir, engine.SocketName) }

func cmdInstall(cfgPath, name string, from []string, ver string, req engine.InstallRequest, wait bool, out, errw io.Writer) int {
	cfg, ok := loadConfig(cfgPath, errw)
	if !ok {
		return 1
	}
	comp := cfg.Components[name]
	if comp == nil {
		fmt.Fprintf(errw, "warren-update: component %q is not configured\n", name)
		return 1
	}
	if comp.Pinned != "" { // before the source is read: the pin is the answer
		fmt.Fprintf(errw, "warren-update: install refused: component %s is pinned: %s\n", name, comp.Pinned)
		return 1
	}
	if len(from) == 0 {
		fmt.Fprintln(errw, "warren-update: install needs --from file://DIR (HTTP and mesh sources come in a later release)")
		return 2
	}
	var cand *engine.Candidate
	var lastErr error
	for _, f := range from {
		dir, err := fileSource(f)
		if err != nil {
			fmt.Fprintln(errw, "warren-update:", err)
			return 2
		}
		c, err := engine.LoadFromDir(dir, comp, ver)
		if err != nil {
			lastErr = err
			continue
		}
		cand = c
		break
	}
	if cand == nil {
		fmt.Fprintln(errw, "warren-update:", lastErr)
		return 1
	}
	cl := engine.NewClient(socketOf(cfg))
	res, err := cl.Install(context.Background(), cand, req)
	if err != nil {
		fmt.Fprintln(errw, "warren-update:", err)
		return 1
	}
	fmt.Fprintf(out, "%s %s: %s\n", res.Component, res.Version, res.Message)
	if req.DryRun || !wait {
		return 0
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return waitFor(ctx, cl, name, out)
}

// fileSource turns --from file://DIR into a directory; a relative path is
// resolved here, by the CLI.
func fileSource(from string) (string, error) {
	if !strings.HasPrefix(from, "file://") {
		return "", fmt.Errorf("source %q: only file:// sources are available in this release", from)
	}
	u, err := url.Parse(from)
	if err != nil || u.Host != "" && u.Host != "." {
		// file://relative/dir parses with a host; take the text after the scheme.
		return filepath.Abs(strings.TrimPrefix(from, "file://"))
	}
	p := u.Path
	if u.Host == "." {
		p = "." + p
	}
	return filepath.Abs(p)
}

// waitFor blocks until the component is healthy or committed (0), or reverted
// or failed (1). It also stops (1) when ctx ends, which is when the user
// interrupts the command: the install itself carries on in the updater.
func waitFor(ctx context.Context, cl *engine.Client, name string, out io.Writer) int {
	var last state.Phase
	for {
		st, err := cl.Status(ctx)
		if err == nil {
			for _, c := range st.Components {
				if c.Name != name {
					continue
				}
				if c.Phase != last {
					fmt.Fprintf(out, "%s: %s\n", name, c.Phase)
					last = c.Phase
				}
				switch {
				case c.Phase == state.Reverted || c.Phase == state.Failed:
					if c.LastError != "" {
						fmt.Fprintf(out, "%s: %s\n", name, c.LastError)
					}
					return 1
				case c.Phase == state.Committed, c.HealthySince != nil:
					fmt.Fprintf(out, "%s: healthy\n", name)
					return 0
				}
			}
		}
		select {
		case <-ctx.Done():
			fmt.Fprintf(out, "%s: stopped waiting; the install carries on in the updater\n", name)
			return 1
		case <-time.After(time.Second):
		}
	}
}

func cmdRevert(cfgPath, name string, offline bool, out, errw io.Writer) int {
	if offline {
		// The offline revert writes the updater's state, so it runs as the user
		// that owns it and under the daemon's own rules for the config.
		cfg, err := uconfig.Load(cfgPath)
		if err != nil {
			fmt.Fprintln(errw, "warren-update:", err)
			return 1
		}
		// What can be refused from the config alone is refused before the lock.
		e := engine.New(engine.Options{Config: cfg, Version: version.Version})
		if err := e.PreflightOffline(name); err != nil {
			fmt.Fprintln(errw, "warren-update:", err)
			return 1
		}
		if err := engine.CheckStateDir(cfg.StateDir); err != nil {
			fmt.Fprintln(errw, "warren-update:", err)
			return 1
		}
		lock, err := engine.AcquireLock(cfg.StateDir, 10*time.Second)
		if err != nil {
			fmt.Fprintln(errw, "warren-update:", err)
			return 1
		}
		defer lock.Close()
		res, err := e.RevertOffline(name)
		if err != nil {
			fmt.Fprintln(errw, "warren-update:", err)
			return 1
		}
		fmt.Fprintf(out, "%s: %s (version %s)\n", name, res.Message, res.Version)
		return 0
	}
	cfg, ok := loadConfig(cfgPath, errw)
	if !ok {
		return 1
	}
	res, err := engine.NewClient(socketOf(cfg)).Revert(context.Background(), name)
	if err != nil {
		fmt.Fprintln(errw, "warren-update:", err)
		return 1
	}
	fmt.Fprintf(out, "%s: %s (version %s)\n", name, res.Message, res.Version)
	return 0
}

func cmdReload(cfgPath string, out, errw io.Writer) int {
	cfg, ok := loadConfig(cfgPath, errw)
	if !ok {
		return 1
	}
	if err := engine.NewClient(socketOf(cfg)).Reload(context.Background()); err != nil {
		fmt.Fprintln(errw, "warren-update:", err)
		return 1
	}
	fmt.Fprintln(out, "reloaded")
	return 0
}

func cmdStatus(cfgPath string, asJSON bool, out, errw io.Writer) int {
	cfg, ok := loadConfig(cfgPath, errw)
	if !ok {
		return 1
	}
	st, err := engine.NewClient(socketOf(cfg)).Status(context.Background())
	if err != nil {
		fmt.Fprintln(errw, "warren-update:", err)
		return 1
	}
	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		_ = enc.Encode(st)
		return 0
	}
	printStatus(out, st)
	return 0
}

func printStatus(out io.Writer, st *engine.NodeStatus) {
	fmt.Fprintf(out, "warren-update %s", st.Updater)
	switch {
	case !st.WarrenReachable:
		fmt.Fprint(out, ", Warren not reachable")
	case st.WarrenVersion != "":
		fmt.Fprintf(out, ", warren %s", st.WarrenVersion)
	}
	fmt.Fprintln(out)
	if st.Pinned != "" {
		fmt.Fprintf(out, "node pinned: %s\n", st.Pinned)
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "COMPONENT\tCHANNEL\tVERSION\tPHASE\tNOTES")
	for _, c := range st.Components {
		ver := c.Version
		if ver == "" {
			ver = "-"
		}
		var notes []string
		if c.Pinned != "" {
			notes = append(notes, "pinned: "+c.Pinned)
		}
		if c.Updatable != nil && !*c.Updatable {
			notes = append(notes, "not updatable: "+c.NotUpdatable)
		}
		if c.NotManaged {
			notes = append(notes, "service not managed")
		}
		if c.HealthySince != nil {
			notes = append(notes, "healthy since "+c.HealthySince.Local().Format(time.DateTime))
		}
		if c.Prev != "" {
			notes = append(notes, "prev "+c.Prev)
		}
		if c.RestartPending {
			notes = append(notes, "restart pending")
		}
		if c.Busy {
			notes = append(notes, "installing")
		}
		if c.NeedsManual != "" {
			notes = append(notes, "needs a manual install ("+c.NeedsManual+")")
		}
		if len(c.Bad) > 0 {
			var bad []string
			for _, b := range c.Bad {
				bad = append(bad, b.Version+" ("+b.Reason+")")
			}
			notes = append(notes, "bad: "+strings.Join(bad, ", "))
		}
		if c.LastError != "" {
			notes = append(notes, "last error: "+c.LastError)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", c.Name, c.Channel, ver, c.Phase, strings.Join(notes, "; "))
	}
	tw.Flush()
}
