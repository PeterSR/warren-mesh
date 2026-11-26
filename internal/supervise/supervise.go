// Package supervise runs the managed processes of a node (services and the
// transport): it starts them with a small environment, restarts them with
// backoff, logs their output, stops them in order at shutdown, and, as a
// subreaper, collects every orphan that re-parents to Warren, so Warren can
// be PID 1 in a container.
package supervise

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
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
	// OnExit runs at once when the process exits, before any restart: a
	// service's lease ends there.
	OnExit func()
}

// Status is what `warren status` shows of a process.
type Status struct {
	Name     string    `json:"name"`
	State    string    `json:"state"`
	PID      int       `json:"pid,omitempty"`
	LastExit string    `json:"last_exit,omitempty"`
	Restarts int       `json:"restarts"`
	Since    time.Time `json:"since"`
}

// Supervisor runs a set of processes.
type Supervisor struct {
	log *slog.Logger

	mu    sync.Mutex
	procs map[string]*proc
	stop  bool
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

	quit    chan struct{} // closed to stop for good
	restart chan struct{} // a request to stop the running process and start it again
	done    chan struct{} // closed when the loop has ended
}

// New makes a supervisor and starts the reaper and the subreaper setting.
func New(log *slog.Logger) *Supervisor {
	if log == nil {
		log = slog.Default()
	}
	startReaper(log)
	return &Supervisor{log: log, procs: map[string]*proc{}}
}

// Add starts a process and keeps it running.
func (s *Supervisor) Add(spec Spec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stop {
		return fmt.Errorf("supervisor is stopping")
	}
	if _, dup := s.procs[spec.Name]; dup {
		return fmt.Errorf("process %q already exists", spec.Name)
	}
	if len(spec.Argv) == 0 {
		return fmt.Errorf("process %q has no command", spec.Name)
	}
	p := &proc{spec: spec, sup: s, state: Starting, since: time.Now(), quit: make(chan struct{}), restart: make(chan struct{}, 1), done: make(chan struct{})}
	s.procs[spec.Name] = p
	go p.loop()
	return nil
}

// Remove stops one process and forgets it.
func (s *Supervisor) Remove(name string) {
	s.mu.Lock()
	p := s.procs[name]
	delete(s.procs, name)
	s.mu.Unlock()
	if p != nil {
		p.halt()
		<-p.done
	}
}

// Restart asks for the named process to be stopped (SIGTERM, then SIGKILL
// after KillAfter) and started again with new Prepare results. It returns at
// once; a request for a process that is not running is forgotten.
func (s *Supervisor) Restart(name string) {
	s.mu.Lock()
	p := s.procs[name]
	s.mu.Unlock()
	if p != nil {
		select {
		case p.restart <- struct{}{}:
		default:
		}
	}
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
		out = append(out, Status{Name: p.spec.Name, State: p.state, PID: p.pid, LastExit: p.lastExit, Restarts: p.restarts, Since: p.since})
		p.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
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
		select { // a request made while nothing ran is old
		case <-p.restart:
		default:
		}
		exited, pid, err := p.start(log)
		if err != nil {
			log.Warn("cannot start", "err", err)
			p.mu.Lock()
			p.lastExit = "cannot start: " + err.Error()
			p.mu.Unlock()
		} else {
			p.set(Running, pid)
			if p.spec.OnStart != nil {
				p.spec.OnStart()
			}
			status := p.wait(log, pid, exited)
			if p.spec.OnExit != nil {
				p.spec.OnExit()
			}
			p.mu.Lock()
			p.lastExit = describe(status)
			p.mu.Unlock()
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
		select {
		case <-p.quit:
			p.set(Stopped, 0)
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > BackoffMax {
			backoff = BackoffMax
		}
	}
}

// wait blocks until the process has exited, stopping it first if asked:
// SIGTERM, then SIGKILL after KillAfter.
func (p *proc) wait(log *slog.Logger, pid int, exited <-chan syscall.WaitStatus) syscall.WaitStatus {
	select {
	case ws := <-exited:
		return ws
	case <-p.quit:
	case <-p.restart:
		log.Info("restarting on request")
	}
	theReaper.signal(pid, syscall.SIGTERM)
	t := time.NewTimer(KillAfter)
	defer t.Stop()
	select {
	case ws := <-exited:
		return ws
	case <-t.C:
		log.Warn("did not stop after SIGTERM; killing", "after", KillAfter)
		theReaper.signal(pid, syscall.SIGKILL)
		return <-exited
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
		// the group of a leader that was asked to stop is killed before it
		// is reaped: until then its pid cannot be reused, so the kill can
		// never reach a stranger. Elsewhere the child is reaped first.
		peeked, found, supported := platform.PeekExited()
		if supported && !found {
			return
		}
		if found && r.stopping[peeked] {
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
