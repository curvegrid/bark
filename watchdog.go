package bark

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type WatchdogLogFunc func(format string, a ...interface{})

type Watchdog struct {
	RestartChild             chan bool
	ReqStopWatchdog          chan bool
	TermChildAndStopWatchdog chan bool
	Done                     chan bool
	CurrentPid               chan int
	curPid                   int

	Logger             WatchdogLogFunc
	EnableDebugLogging bool

	MaxRetries             int64
	startCount             int64
	retryCount             int64
	RetryInterval          time.Duration
	DeclareSuccessInterval time.Duration // time after which process is declared successfully (re)started

	mut      sync.Mutex
	shutdown bool

	PathToChildExecutable string
	Argv                  []string
	Attr                  os.ProcAttr
	err                   error
	needRestart           bool
	startingProcess       bool
	proc                  *os.Process
	exitAfterReaping      bool
}

// NewWatchdog creates a Watchdog structure but
// does not launch it or its child process until
// Start() is called on it.
// The attr argument sets function attributes such
// as environment and open files; see os.ProcAttr for details.
// Also, attr can be nil here, in which case and
// empty os.ProcAttr will be supplied to the
// os.StartProcess() call.
func NewWatchdog(
	attr *os.ProcAttr,
	cmd string,
	args ...string) *Watchdog {

	// add the program name the argv (argv[0] represents the program name)
	argv := append([]string{cmd}, args...)

	// if needed, look for the binary in the path
	if filepath.Base(cmd) == cmd {
		if lp, err := exec.LookPath(cmd); err == nil {
			cmd = lp
		}
	}

	w := &Watchdog{
		PathToChildExecutable:    cmd,
		Argv:                     argv,
		RestartChild:             make(chan bool),
		ReqStopWatchdog:          make(chan bool),
		TermChildAndStopWatchdog: make(chan bool),
		CurrentPid:               make(chan int),
		MaxRetries:               10,
		RetryInterval:            5 * time.Second,
		DeclareSuccessInterval:   10 * time.Second,
	}

	if attr != nil {
		w.Attr = *attr
	}
	return w
}

// StartAndWatch() is the convenience/main entry API.
// pathToProcess should give the path to the executable within
// the filesystem. If it dies it will be restarted by
// the Watchdog.
func StartAndWatch(cmd string, args ...string) (*Watchdog, error) {

	// start our child; restart it if it dies.
	watcher := NewWatchdog(nil, cmd, args...)
	watcher.Start()

	return watcher, nil
}

func (w *Watchdog) AlreadyDone() bool {
	select {
	case <-w.Done:
		return true
	default:
		return false
	}
}
func (w *Watchdog) Stop() error {
	if w.AlreadyDone() {
		// once Done, w.err is immutable, so we don't need to lock.
		return w.err
	}
	w.mut.Lock()
	if w.shutdown {
		defer w.mut.Unlock()
		return w.err
	}
	w.mut.Unlock()

	close(w.ReqStopWatchdog)
	<-w.Done
	// don't wait for Done while holding the lock,
	// as that is deadlock prone.

	w.mut.Lock()
	defer w.mut.Unlock()
	w.shutdown = true
	return w.err
}

func (w *Watchdog) SetErr(err error) {
	w.mut.Lock()
	defer w.mut.Unlock()
	w.err = err
}

func (w *Watchdog) GetErr() error {
	w.mut.Lock()
	defer w.mut.Unlock()
	return w.err
}

func (w *Watchdog) logEvent(isDebug bool, format string, a ...interface{}) {
	if !w.EnableDebugLogging && isDebug {
		return
	}

	format = "Watchdog: " + format

	if w.Logger != nil {
		w.Logger(format, a...)
	} else {
		log.Printf(format, a...)
	}
}

// see w.err for any error after w.Done
func (w *Watchdog) Start() {
	w.Done = make(chan bool)
	w.startingProcess = true
	w.needRestart = true
	atomic.StoreInt64(&w.retryCount, 0)

	signalChild := make(chan os.Signal, 1)
	signal.Notify(signalChild, syscall.SIGCHLD)

	var ws syscall.WaitStatus
	go func() {
		defer func() {
			if w.proc != nil {
				w.proc.Release()
			}
			close(w.Done)
			// can deadlock if we don't close(w.Done) before grabbing the mutex:
			w.mut.Lock()
			w.shutdown = true
			w.mut.Unlock()
			signal.Stop(signalChild) // reverse the effect of the above Notify()
		}()
		var err error

	reaploop:
		for {
			retryCount := atomic.LoadInt64(&w.retryCount)
			if retryCount > w.MaxRetries {
				w.logEvent(true, "unable to start after %v retries, giving up", retryCount)
				err = fmt.Errorf("unable to start process after %v retries, giving up", retryCount)
				w.SetErr(err)
				return
			}

			if w.needRestart {
				if w.proc != nil {
					w.proc.Release()
				}
				w.logEvent(true, "about to start '%s'", w.PathToChildExecutable)

				if retryCount > 0 {
					w.logEvent(true, "sleeping for %v before attempting restart; retryCount = %v (max = %v)", w.RetryInterval, retryCount, w.MaxRetries)
					time.Sleep(w.RetryInterval)
				}

				w.proc, err = os.StartProcess(w.PathToChildExecutable, w.Argv, &w.Attr)
				if err != nil {
					w.SetErr(err)
					w.logEvent(true, "unable to start: '%v'", err)
					return
				}
				w.curPid = w.proc.Pid
				w.needRestart = false
				w.startCount++
				if w.startingProcess {
					w.startingProcess = false
				}

				// increment the counter
				retryCount = atomic.AddInt64(&w.retryCount, 1)

				// reset retry count after an interval of process stability
				go func(currRetryCount int64) {
					time.Sleep(w.DeclareSuccessInterval)
					if atomic.LoadInt64(&w.retryCount) == currRetryCount {
						atomic.StoreInt64(&w.retryCount, 0)
					}
				}(retryCount)

				w.logEvent(true, "started pid %d '%s'; total start count %d", w.proc.Pid, w.PathToChildExecutable, w.startCount)
			}

			select {
			case w.CurrentPid <- w.curPid:
			case <-w.TermChildAndStopWatchdog:
				w.logEvent(true, "TermChildAndStopWatchdog noted, exiting watchdog.Start() loop")

				err := w.proc.Signal(syscall.SIGTERM)
				if err != nil {
					err = fmt.Errorf("warning: watchdog tried to SIGTERM pid %d but got error: '%s'", w.proc.Pid, err)
					w.SetErr(err)
					w.logEvent(false, "%s", err)
					return
				}
				w.exitAfterReaping = true
				continue reaploop
			case <-w.ReqStopWatchdog:
				w.logEvent(true, "ReqStopWatchdog noted, exiting watchdog.Start() loop")
				return
			case <-w.RestartChild:
				w.logEvent(true, "got <-w.RestartChild")
				err := w.proc.Signal(syscall.SIGKILL)
				if err != nil {
					err = fmt.Errorf("warning: watchdog tried to SIGKILL pid %d but got error: '%s'", w.proc.Pid, err)
					w.SetErr(err)
					w.logEvent(false, "%s", err)
					return
				}
				w.curPid = 0
				continue reaploop
			case <-signalChild:
				w.logEvent(true, "got <-signalChild")

				for i := 0; i < 1000; i++ {
					pid, err := syscall.Wait4(w.proc.Pid, &ws, syscall.WNOHANG, nil)
					// pid > 0 => pid is the ID of the child that died, but
					//  there could be other children that are signalling us
					//  and not the one we in particular are waiting for.
					// pid -1 && errno == ECHILD => no new status children
					// pid -1 && errno != ECHILD => syscall interupped by signal
					// pid == 0 => no more children to wait for.
					w.logEvent(true, "pid=%v  ws=%v and err == %v", pid, ws, err)
					switch {
					case err != nil:
						err = fmt.Errorf("wait4() got error back: '%s' and ws:%v", err, ws)
						w.logEvent(false, "warning in reaploop, wait4(WNOHANG) returned error: '%s'. ws=%v", err, ws)
						w.SetErr(err)
						continue reaploop
					case pid == w.proc.Pid:
						w.logEvent(true, "saw OUR current w.proc.Pid %d/process '%s' finish with waitstatus: %v.", pid, w.PathToChildExecutable, ws)
						if w.exitAfterReaping {
							w.logEvent(true, "sees exitAfterReaping. exiting now.")
							return
						}
						w.needRestart = true
						w.curPid = 0
						continue reaploop
					case pid == 0:
						// this is what we get when SIGSTOP is sent on OSX. ws == 0 in this case.
						// Note that on OSX we never get a SIGCONT signal.
						// Under WNOHANG, pid == 0 means there is nobody left to wait for,
						// so just go back to waiting for another SIGCHLD.
						w.logEvent(true, "pid == 0 on wait4, (perhaps SIGSTOP?): nobody left to wait for, keep looping. ws = %v", ws)
						continue reaploop
					default:
						w.logEvent(true, "warning in reaploop: wait4() negative or not our pid, sleep and try again")
						time.Sleep(time.Millisecond)
					}
				} // end for i
				w.SetErr(fmt.Errorf("could not reap child PID %d or obtain wait4(WNOHANG)==0 even after 1000 attempts", w.proc.Pid))
				w.logEvent(false, "%s", w.GetErr())
				return
			} // end select
		} // end for reaploop
	}()
}
