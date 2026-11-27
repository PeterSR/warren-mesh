// Package supervise runs the managed processes of a node (services and the
// transport): it starts them with a small environment, restarts them with
// backoff, logs their output, stops them in order at shutdown, and, as a
// subreaper, collects every orphan that re-parents to Warren, so Warren can
// be PID 1 in a container.
package supervise

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/PeterSR/warren-mesh/internal/platform"
)

// Timing. Variables, so tests can shorten them.
var (
	// BackoffStart is the first delay before a restart; it doubles up to
	// BackoffMax, and starts over after BackoffReset of staying up.
	BackoffStart = 1 * time.Second
	BackoffMax   = 60 * time.Second
	BackoffReset = 5 * time.Minute
	// KillAfter is how long a process has to obey SIGTERM before SIGKILL.
	KillAfter = 10 * time.Second
	// ReapEvery is the safety net next to SIGCHLD for reaping orphans.
	ReapEvery = 500 * time.Millisecond
)

// Process states.
const (
	Starting   = "starting" // before the first start
	Running    = "running"
	Restarting = "restarting"
	Stopped    = "stopped"
)

// Groups are stopped in order: services first, the transport after, so
// calls in flight can still be carried while services finish.
const (
	GroupService   = 0
	GroupTransport = 1
)

// Spec describes one managed process.
type Spec struct {
	Name  string
	Group int
	Argv  []string
	// Env is the whole environment, see MinimalEnv.
	Env []string
	// Prepare, if set, runs before every start and returns descriptors to
	// inherit (fd 3, 4, ...) and a release func, called once the process
	// has started or failed to.
	Prepare func() (files []*os.File, release func(), err error)
	// OnStart runs after each successful start.
	OnStart func()
	// OnExit runs at once when the process exits, before any restart (a
	// service's lease ends there), and when a start fails. requested is true
	// when the process was stopped because the process was removed or Warren
	// stops, or because a restart was requested. err is the failure to start,
	// or a description of an exit that was not a clean status 0; it is nil for
	// a clean exit.
	OnExit func(requested bool, err error)
}

// Status is what `warren status` shows of a process.
type Status struct {
	Name     string    `json:"name"`
	State    string    `json:"state"`
	PID      int       `json:"pid,omitempty"`
	LastExit string    `json:"last_exit,omitempty"`
	Restarts int       `json:"restarts"`
	Since    time.Time `json:"since"`

	// Exe is argv[0] made absolute as of the latest start (a bare command is
	// resolved through PATH); empty, with ExeError set, when that is not
	// possible. It is never the rest of argv, which may hold secrets.
	Exe      string `json:"exe,omitempty"`
	ExeError string `json:"exe_error,omitempty"`
	// Updatable and NotUpdatable are not known to the supervisor; the daemon
	// fills them in before the status is shown (see docs/control-api.md).
	Updatable    *bool  `json:"updatable,omitempty"`
	NotUpdatable string `json:"not_updatable,omitempty"`
	// StartSeq increments each time a start is begun, failed or not; Starts
	// counts the successful ones. The counters are cumulative per name for the
	// life of the supervisor: they survive Remove and Add.
	StartSeq          int       `json:"start_seq"`
	Starts            int       `json:"starts"`
	Exits             int       `json:"exits"`
	RequestedExits    int       `json:"requested_exits"`
	StartFailures     int       `json:"start_failures"`
	LastExitRequested bool      `json:"last_exit_requested"`
	LastStartError    string    `json:"last_start_error,omitempty"`
	StartedAt         time.Time `json:"started_at,omitzero"`
}

// stats are the counters of one process name. They are kept by the supervisor
// and outlive the proc, so a Remove followed by an Add continues them. Only
// the live proc of the name touches them, under its mu.
type stats struct {
	startSeq          int
	starts            int
	exits             int
	requestedExits    int
	startFailures     int
	lastExitRequested bool
	lastStartError    string
	startedAt         time.Time
}

// Supervisor runs a set of processes.
type Supervisor struct {
	log *slog.Logger

	mu    sync.Mutex
	procs map[string]*proc
	stats map[string]*stats // by name, for the life of the supervisor
	// removing holds the done channel of a process that Remove is still
	// stopping: Add of the same name waits for it, so two processes of one
	// name never run (or share counters) at once.
	removing map[string]chan struct{}
	stop     bool
}

type proc struct {
	spec Spec
	sup  *Supervisor

	mu       sync.Mutex
	state    string
	pid      int
	lastExit string
	restarts int
	since    time.Time
	st       *stats
	exe      string
	exeErr   string
	// pending is the lowest start_seq that satisfies a restart request: a
	// process whose start_seq is lower is stopped, and the loop skips its
	// backoff. It is recorded under mu, so a request is never lost, whatever
	// the loop is doing; wake only wakes the loop to look at it.
	pending int
	// bounce is the highest start_seq that a link break asked to stop: a
	// running process of that start or an older one is stopped, but unlike a
	// restart request this neither skips the backoff nor counts as requested.
	bounce int

	quit chan struct{} // closed to stop for good
	wake chan struct{} // a restart request was recorded
	done chan struct{} // closed when the loop has ended
}

// New makes a supervisor and starts the reaper and the subreaper setting.
func New(log *slog.Logger) *Supervisor {
	if log == nil {
		log = slog.Default()
	}
	startReaper(log)
	return &Supervisor{log: log, procs: map[string]*proc{}, stats: map[string]*stats{}, removing: map[string]chan struct{}{}}
}

// Add starts a process and keeps it running.
func (s *Supervisor) Add(spec Spec) error {
	if len(spec.Argv) == 0 {
		return fmt.Errorf("process %q has no command", spec.Name)
	}
	// Looked up outside the supervisor's lock: PATH may be slow.
	exe, exeErr := resolveExe(spec.Argv[0])
	for {
		s.mu.Lock()
		if s.stop {
			s.mu.Unlock()
			return fmt.Errorf("supervisor is stopping")
		}
		if done := s.removing[spec.Name]; done != nil {
			s.mu.Unlock()
			<-done // the old process of this name is still being stopped
			// Remove has only its bookkeeping left; do not spin on its lock.
			time.Sleep(time.Millisecond)
			continue
		}
		break
	}
	defer s.mu.Unlock()
	if _, dup := s.procs[spec.Name]; dup {
		return fmt.Errorf("process %q already exists", spec.Name)
	}
	st := s.stats[spec.Name]
	if st == nil {
		st = &stats{}
		s.stats[spec.Name] = st
	}
	p := &proc{spec: spec, sup: s, state: Starting, since: time.Now(), st: st, quit: make(chan struct{}), wake: make(chan struct{}, 1), done: make(chan struct{})}
	p.exe, p.exeErr = exe, exeErr
	s.procs[spec.Name] = p
	go p.loop()
	return nil
}

// Remove stops one process and forgets it. It returns when the process is gone.
func (s *Supervisor) Remove(name string) {
	s.mu.Lock()
	p := s.procs[name]
	delete(s.procs, name)
	if p != nil {
		s.removing[name] = p.done
	}
	s.mu.Unlock()
	if p != nil {
		p.halt()
		<-p.done
		s.mu.Lock()
		delete(s.removing, name)
		s.mu.Unlock()
	}
}

// Bounce stops the named process if it is running, as a broken link needs, and
// lets the normal loop start it again: with the doubling backoff, and without
// counting the exit as requested. (Restart is the request that skips and resets
// the backoff.) A process that is waiting in backoff is left alone.
func (s *Supervisor) Bounce(name string) bool {
	s.mu.Lock()
	p := s.procs[name]
	if p == nil {
		s.mu.Unlock()
		return false
	}
	p.mu.Lock()
	if p.st.startSeq > p.bounce {
		p.bounce = p.st.startSeq
	}
	p.mu.Unlock()
	s.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return true
}

// Restart asks for the named process to be stopped (SIGTERM, then SIGKILL
// after KillAfter) and started again with new Prepare results. It returns at
// once. The request is recorded, not signalled, so it is never lost: it also
// ends a wait in backoff, starts at once and resets the backoff. minStartSeq
// is taken under the process lock: every start of this process with a
// start_seq of minStartSeq or higher begins after the request (a start in
// progress has a lower number). ok is false when the name is not managed or
// the process is being stopped.
func (s *Supervisor) Restart(name string) (minStartSeq int, ok bool) {
	// p.mu is taken while s.mu is held (the order Status uses), so the proc
	// cannot be removed, and its counters taken over by a new one that shares
	// them, between the lookup and the request.
	s.mu.Lock()
	p := s.procs[name]
	if p == nil {
		s.mu.Unlock()
		return 0, false
	}
	select {
	case <-p.quit:
		s.mu.Unlock()
		return 0, false // being stopped: a request would never be served
	default:
	}
	p.mu.Lock()
	p.pending = p.st.startSeq + 1
	minStartSeq = p.pending
	p.mu.Unlock()
	s.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return minStartSeq, true
}

// Has reports whether a process with this name is managed.
func (s *Supervisor) Has(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.procs[name]
	return ok
}

// StopGroup stops every process of one group in parallel: SIGTERM, wait up
// to KillAfter, SIGKILL. They are not restarted.
func (s *Supervisor) StopGroup(group int) {
	s.mu.Lock()
	var ps []*proc
	for _, p := range s.procs {
		if p.spec.Group == group {
			ps = append(ps, p)
		}
	}
	s.mu.Unlock()
	for _, p := range ps {
		p.halt()
	}
	for _, p := range ps {
		<-p.done
	}
}

// StopAll stops services, then the transport, and refuses further Adds.
func (s *Supervisor) StopAll() {
	s.mu.Lock()
	s.stop = true
	s.mu.Unlock()
	s.StopGroup(GroupService)
	s.StopGroup(GroupTransport)
}

// Status lists the processes by name.
func (s *Supervisor) Status() []Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Status
	for _, p := range s.procs {
		p.mu.Lock()
		out = append(out, p.status())
		p.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// status is the process's Status. mu held, or the proc not yet started.
func (p *proc) status() Status {
	st := p.st
	return Status{Name: p.spec.Name, State: p.state, PID: p.pid, LastExit: p.lastExit, Restarts: p.restarts, Since: p.since,
		Exe: p.exe, ExeError: p.exeErr,
		StartSeq: st.startSeq, Starts: st.starts, Exits: st.exits, RequestedExits: st.requestedExits, StartFailures: st.startFailures,
		LastExitRequested: st.lastExitRequested, LastStartError: st.lastStartError, StartedAt: st.startedAt}
}

// Process returns the status of one managed process.
func (s *Supervisor) Process(name string) (Status, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.procs[name]
	if p == nil {
		return Status{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status(), true
}

// resolveExe makes argv[0] absolute: a bare command is looked up through PATH,
// a relative path is joined to the working directory.
func resolveExe(argv0 string) (exe, errText string) {
	path := argv0
	if !filepath.IsAbs(path) {
		if filepath.Base(path) == path {
			p, err := exec.LookPath(path)
			if err != nil {
				return "", err.Error()
			}
			path = p
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", err.Error()
		}
		path = abs
	}
	return filepath.Clean(path), ""
}

func (p *proc) halt() {
	select {
	case <-p.quit:
	default:
		close(p.quit)
	}
}

func (p *proc) set(state string, pid int) {
	p.mu.Lock()
	p.state, p.pid, p.since = state, pid, time.Now()
	p.mu.Unlock()
}

// stale reports whether a restart request is outstanding for a start with
// this start_seq.
func (p *proc) stale(seq int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pending > seq
}

// bounced reports whether a link break asked for a start with this start_seq
// (or an older one) to be stopped.
func (p *proc) bounced(seq int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bounce >= seq
}

// loop starts the process, waits for it, and starts it again.
func (p *proc) loop() {
	defer close(p.done)
	log := p.sup.log.With("proc", p.spec.Name)
	backoff := BackoffStart
	for {
		select {
		case <-p.quit:
			p.set(Stopped, 0)
			return
		default:
		}
		started := time.Now()
		// The exe is looked up before the lock and set in the same step as the
		// start number, so a status never shows a start with the previous exe.
		exe, exeErr := resolveExe(p.spec.Argv[0])
		p.mu.Lock()
		p.exe, p.exeErr = exe, exeErr
		p.st.startSeq++ // before the exec: a start in progress has a number
		seq := p.st.startSeq
		p.mu.Unlock()
		exited, pid, err := p.start(log)
		if err != nil {
			log.Warn("cannot start", "err", err)
			p.mu.Lock()
			p.st.startFailures++
			p.st.lastStartError = err.Error()
			p.lastExit = "cannot start: " + err.Error()
			p.mu.Unlock()
			if p.spec.OnExit != nil {
				p.spec.OnExit(false, err)
			}
		} else {
			p.mu.Lock()
			p.st.starts++
			p.st.startedAt = time.Now()
			p.st.lastStartError = ""
			p.mu.Unlock()
			p.set(Running, pid)
			if p.spec.OnStart != nil {
				p.spec.OnStart()
			}
			status, requested := p.wait(log, pid, seq, exited)
			var exitErr error
			if status != 0 {
				exitErr = errors.New(describe(status))
			}
			p.mu.Lock()
			p.st.exits++
			if requested {
				p.st.requestedExits++
			}
			p.st.lastExitRequested = requested
			p.lastExit = describe(status)
			p.mu.Unlock()
			if p.spec.OnExit != nil {
				p.spec.OnExit(requested, exitErr)
			}
			log.Info("exited", "result", describe(status))
			select {
			case <-p.quit:
				p.set(Stopped, 0)
				return
			default:
			}
			if time.Since(started) >= BackoffReset {
				backoff = BackoffStart
			}
		}
		p.mu.Lock()
		p.restarts++
		p.mu.Unlock()
		p.set(Restarting, 0)
		// A restart request skips the backoff and resets it; so does one made
		// during the wait.
		var timer *time.Timer
		var timeout <-chan time.Time
		if !p.stale(seq) {
			timer = time.NewTimer(backoff)
			timeout = timer.C
		}
	wait:
		for timeout != nil {
			select {
			case <-p.quit:
				timer.Stop()
				p.set(Stopped, 0)
				return
			case <-timeout:
				break wait
			case <-p.wake:
				if p.stale(seq) {
					timer.Stop()
					break wait
				}
			}
		}
		if p.stale(seq) {
			backoff = BackoffStart
			continue
		}
		if backoff *= 2; backoff > BackoffMax {
			backoff = BackoffMax
		}
	}
}

// wait blocks until the process has exited, stopping it first if asked:
// SIGTERM, then SIGKILL after KillAfter. It is asked to stop by quit, or by
// a restart request newer than the start with this start_seq; a wake for an
// older request goes back to waiting. requested is whether it was asked.
func (p *proc) wait(log *slog.Logger, pid, seq int, exited <-chan syscall.WaitStatus) (ws syscall.WaitStatus, requested bool) {
	requested = true
	for stop := false; !stop; {
		if p.stale(seq) {
			log.Info("restarting on request")
			break
		}
		if p.bounced(seq) {
			log.Info("stopping: the link broke")
			requested = false // not asked for by a caller: a failure that backs off
			break
		}
		select {
		case ws := <-exited:
			return ws, false
		case <-p.quit:
			stop = true
		case <-p.wake:
		}
	}
	theReaper.signal(pid, syscall.SIGTERM)
	t := time.NewTimer(KillAfter)
	defer t.Stop()
	select {
	case ws := <-exited:
		return ws, requested
	case <-t.C:
		log.Warn("did not stop after SIGTERM; killing", "after", KillAfter)
		theReaper.signal(pid, syscall.SIGKILL)
		return <-exited, requested
	}
}

func describe(ws syscall.WaitStatus) string {
	switch {
	case ws.Exited():
		return fmt.Sprintf("exit status %d", ws.ExitStatus())
	case ws.Signaled():
		return "signal: " + ws.Signal().String()
	}
	return fmt.Sprintf("wait status %d", int(ws))
}

// start runs the process once. The reaper's lock is held across Start and
// registration, so the exit of a fast process cannot be reaped unseen.
func (p *proc) start(log *slog.Logger) (<-chan syscall.WaitStatus, int, error) {
	var files []*os.File
	if p.spec.Prepare != nil {
		f, release, err := p.spec.Prepare()
		if err != nil {
			return nil, 0, err
		}
		defer release()
		files = f
	}
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, 0, err
	}
	cmd := exec.Command(p.spec.Argv[0], p.spec.Argv[1:]...)
	cmd.Env = p.spec.Env
	cmd.Stdout, cmd.Stderr = pw, pw
	cmd.ExtraFiles = files
	// Its own process group, so a signal reaches what the process started too.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	theReaper.mu.Lock()
	err = cmd.Start()
	var ch chan syscall.WaitStatus
	if err == nil {
		ch = make(chan syscall.WaitStatus, 1)
		theReaper.watch[cmd.Process.Pid] = ch
	}
	theReaper.mu.Unlock()
	pw.Close()
	if err != nil {
		pr.Close()
		return nil, 0, err
	}
	pid := cmd.Process.Pid
	// The reaper collects the exit; Go need not wait.
	_ = cmd.Process.Release()
	go logLines(log, pr)
	return ch, pid, nil
}

// maxLogLine is the longest output line logged whole; the rest of a longer
// one is read and dropped, never left in the pipe, or the process would
// block on a full pipe or die of SIGPIPE.
const maxLogLine = 1 << 20

func logLines(log *slog.Logger, r io.ReadCloser) {
	defer r.Close()
	br := bufio.NewReaderSize(r, 64<<10)
	var line []byte
	truncated := false
	emit := func() {
		if truncated {
			log.Info("output", "line", string(line)+" [line truncated]")
		} else {
			log.Info("output", "line", string(line))
		}
		line, truncated = line[:0], false
	}
	for {
		part, more, err := br.ReadLine()
		if room := maxLogLine - len(line); len(part) > room {
			part, truncated = part[:room], true
		}
		line = append(line, part...)
		if err != nil {
			if len(line) > 0 {
				emit()
			}
			return
		}
		if !more {
			emit()
		}
	}
}

// ---- reaping ----

type reaper struct {
	mu       sync.Mutex
	stopping map[int]bool // pids that were sent SIGTERM to stop them
	watch    map[int]chan syscall.WaitStatus
	orphans  int
}

var (
	theReaper = &reaper{watch: map[int]chan syscall.WaitStatus{}, stopping: map[int]bool{}}
	reapOnce  sync.Once
)

// Orphans is how many unmanaged children (re-parented grandchildren) have
// been reaped.
func Orphans() int {
	theReaper.mu.Lock()
	defer theReaper.mu.Unlock()
	return theReaper.orphans
}

// startReaper makes this process a subreaper and starts the loop that waits
// for every child, managed or orphaned.
func startReaper(log *slog.Logger) {
	reapOnce.Do(func() {
		if err := platform.SetSubreaper(); err != nil {
			log.Warn("cannot become a subreaper; orphans will not be collected", "err", err)
		}
		ch := make(chan os.Signal, 8)
		signal.Notify(ch, syscall.SIGCHLD)
		go func() {
			t := time.NewTicker(ReapEvery)
			defer t.Stop()
			for {
				select {
				case <-ch:
				case <-t.C:
				}
				theReaper.reap(log)
			}
		}()
	})
}

// signal sends sig to the process group led by pid, but only while the
// process has not been reaped. The reaper reaps under the same lock, and a
// process that has exited stays a zombie until then, so its pid cannot have
// been given to another process: no signal can reach a stranger. (A pidfd
// would give the same guarantee on Linux only; this holds everywhere.)
func (r *reaper) signal(pid int, sig syscall.Signal) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, live := r.watch[pid]; live {
		_ = syscall.Kill(-pid, sig)
		if sig == syscall.SIGTERM {
			r.stopping[pid] = true
		}
	}
}

func (r *reaper) reap(log *slog.Logger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		// On Linux the exited child is peeked at first, still a zombie, and
		// the group of a managed leader is killed before it is reaped,
		// whether it was asked to stop or exited on its own: what it left
		// running must not outlive it. Until it is reaped its pid cannot be
		// reused, so the kill can never reach a stranger. Elsewhere the child
		// is reaped first.
		peeked, found, supported := platform.PeekExited()
		if supported && !found {
			return
		}
		if _, managed := r.watch[peeked]; found && managed {
			_ = syscall.Kill(-peeked, syscall.SIGKILL)
			delete(r.stopping, peeked)
		}
		wpid := -1
		if found {
			wpid = peeked
		}
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(wpid, &ws, syscall.WNOHANG, nil)
		if pid <= 0 || err != nil {
			return
		}
		if ch, ok := r.watch[pid]; ok {
			delete(r.watch, pid)
			if !supported && r.stopping[pid] {
				// The leader was asked to stop and is gone: members of its
				// group that ignored SIGTERM must not survive it. This is
				// in the same locked step as the reaping, and a group with
				// a live member keeps its number, so the pgid is still ours.
				// A leader that exited by itself is not treated so here: with
				// no way to look at it before it is reaped, a group with no
				// live member has no claim on its number, which another
				// process may already hold. (Linux kills that group too.)
				_ = syscall.Kill(-pid, syscall.SIGKILL)
			}
			delete(r.stopping, pid)
			ch <- ws
			continue
		}
		r.orphans++
		log.Debug("reaped an orphan", "pid", pid, "result", describe(ws))
	}
}

// Harden makes Warren's own process non-dumpable, so other processes of the
// same user cannot attach with ptrace or copy its descriptors with
// pidfd_getfd (Linux; see platform for other systems). Call it first thing;
// the setting does not survive exec.
func Harden() error { return platform.SetNonDumpable() }

// minimalNames are the variables every managed process gets.
var minimalNames = []string{"PATH", "HOME", "USER", "LANG", "TZ", "XDG_RUNTIME_DIR"}

// MinimalEnv builds a managed process's environment: the minimal set from
// Warren's own, then env_pass names that are set, then env values, then
// extra (WARREN_SERVICE_SOCKET). Later entries win.
func MinimalEnv(environ func(string) (string, bool), pass []string, env, extra map[string]string) []string {
	m := map[string]string{}
	for _, n := range minimalNames {
		if v, ok := environ(n); ok {
			m[n] = v
		}
	}
	for _, n := range pass {
		if v, ok := environ(n); ok {
			m[n] = v
		}
	}
	for k, v := range env {
		m[k] = v
	}
	for k, v := range extra {
		m[k] = v
	}
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}
