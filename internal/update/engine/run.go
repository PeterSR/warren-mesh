package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/PeterSR/warren-mesh/client"
	"github.com/PeterSR/warren-mesh/internal/update/uconfig"
)

// RunOptions start the updater service.
type RunOptions struct {
	ConfigPath string
	Version    string
	Log        *slog.Logger
	// PollEvery is how often the health watcher looks; default 1 s.
	PollEvery time.Duration
	// WarrenWait is how long to wait for Warren's control socket at start;
	// default 30 s. Warren starts its services before it opens the socket.
	WarrenWait time.Duration
	// Reload, if set, receives a function that re-reads the config, for SIGHUP.
	Reload func(reload func() error)
	// Ready, if set, is called once the updater serves its socket.
	Ready func(*Engine)
	// ServiceSocket is the Warren service socket to hold the lease on; default
	// $WARREN_SERVICE_SOCKET. With none the updater runs without a lease.
	ServiceSocket string
}

// Run is `warren-update run`: take update.lock, wait for Warren's control API,
// recover (docs/updates.md, "Recovery"), hold the lease, serve the CLI socket and watch health,
// until ctx ends.
func Run(ctx context.Context, o RunOptions) error {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.WarrenWait == 0 {
		o.WarrenWait = 30 * time.Second
	}
	cfg, err := uconfig.Load(o.ConfigPath)
	if err != nil {
		return err
	}
	if err := PrepareStateDir(cfg.StateDir); err != nil {
		return err
	}
	lock, err := AcquireLock(cfg.StateDir, 0)
	if err != nil {
		return err
	}
	defer lock.Close() // the daemon keeps this descriptor for its whole life
	ctl := NewControl(cfg.Control)
	if err := waitForWarren(ctx, ctl, o.WarrenWait); err != nil {
		return err
	}
	e := New(Options{Config: cfg, Control: ctl, Log: o.Log, Version: o.Version, PollEvery: o.PollEvery})
	// Recovery comes first, before the lease is taken.
	if err := e.Recover(ctx); err != nil {
		o.Log.Error("recovery had errors; the updater runs on", "err", err)
	}
	e.CleanOldStaging()
	reload := func() error {
		next, err := uconfig.Load(o.ConfigPath)
		if err != nil {
			return err
		}
		return e.Reload(next)
	}
	if o.Reload != nil {
		o.Reload(reload)
	}
	srv, err := Listen(filepath.Join(cfg.StateDir, SocketName), e.Handler(reload))
	if err != nil {
		return fmt.Errorf("updater socket: %w", err)
	}
	defer srv.Close()
	done := make(chan struct{}, 2)
	go func() { e.Watch(ctx); done <- struct{}{} }()
	sock := o.ServiceSocket
	if sock == "" {
		sock = os.Getenv(client.EnvSocket)
	}
	if sock != "" {
		go func() { serveLease(ctx, sock, o.Log); done <- struct{}{} }()
	} else {
		o.Log.Warn("no service socket: the updater runs without a lease")
		done <- struct{}{}
	}
	if o.Ready != nil {
		o.Ready(e)
	}
	<-ctx.Done()
	<-done
	<-done
	return nil
}

func waitForWarren(ctx context.Context, ctl *Control, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		err := ctl.CheckAPI(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrTooOld) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("warren's control socket does not answer: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// serveLease holds the updater's service lease, which is all the service does
// in this release (there is no mesh): it answers no calls.
func serveLease(ctx context.Context, sock string, log *slog.Logger) {
	c, err := client.Dial(sock)
	if err != nil {
		log.Error("cannot reach the service socket", "err", err)
		return
	}
	defer c.Close()
	s := c.NewServer(client.ServeOptions{Version: 1, Take: true, Log: log}, func(context.Context, *client.Call) ([]byte, error) {
		return nil, errors.New("warren-update serves no calls yet")
	})
	if err := s.Run(ctx); err != nil {
		log.Error("serving the lease", "err", err)
	}
}
