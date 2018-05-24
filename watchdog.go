package bark

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type WatchdogLogFunc func(format string, a ...interface{})

type Watchdog struct {
	Starting                 chan bool
	Ready                    chan bool
	Restarting               chan bool
	RestartChild             chan bool
	ReqStopWatchdog          chan bool
	TermChildAndStopWatchdog chan bool
	Done                     chan bool
	CurrentPid               chan int
	curPid                   int

	Logger             WatchdogLogFunc
	EnableDebugLogging bool

	startCount int64

	MaxRetries             int
	retryCount             int
	RetryInterval          time.Duration
	DeclareSuccessInterval time.Duration // time after which process is declared successfully (re)started

	mut      sync.Mutex
	shutdown bool

	PathToChildExecutable string
	Cwd                   string
	Args                  []string
	// Attr                  os.ProcAttr
	err              error
	needRestart      bool
	startingProcess  bool
	cmd              *exec.Cmd
	exitAfterReaping bool
	sout             bytes.Buffer
	serr             bytes.Buffer
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
	//attr *os.ProcAttr,
	cwd string,
	pathToChildExecutable string,
	args ...string) *Watchdog {

	cpOfArgs := make([]string, 0)
	for i := range args {
		cpOfArgs = append(cpOfArgs, args[i])
	}
	w := &Watchdog{
		PathToChildExecutable: pathToChildExecutable,
		Args:                     cpOfArgs,
		Starting:                 make(chan bool, 1),
		Ready:                    make(chan bool),
		Restarting:               make(chan bool, 1),
		RestartChild:             make(chan bool),
		ReqStopWatchdog:          make(chan bool),
		TermChildAndStopWatchdog: make(chan bool),
		Done:                   make(chan bool),
		CurrentPid:             make(chan int),
		Cwd:                    cwd,
		MaxRetries:             10,
		RetryInterval:          5 * time.Second,
		DeclareSuccessInterval: 10 * time.Second,
	}

	// if attr != nil {
	// 	w.Attr = *attr
	// }
	return w
}

// StartAndWatch() is the convenience/main entry API.
// pathToProcess should give the path to the executable within
// the filesystem. If it dies it will be restarted by
// the Watchdog.
func StartAndWatch(pathToProcess string, args ...string) (*Watchdog, error) {

	// start our child; restart it if it dies.
	watcher := NewWatchdog("", pathToProcess, args...)
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

	signalChild := make(chan os.Signal, 1)

	signal.Notify(signalChild, syscall.SIGCHLD)

	w.startingProcess = true
	w.needRestart = true
	var ws syscall.WaitStatus
	go func() {
		defer func() {
			if w.cmd != nil && w.cmd.Process != nil {
				w.cmd.Process.Release()
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
			w.mut.Lock()
			if w.retryCount > w.MaxRetries {
				w.logEvent(true, "unable to start after %v retries, giving up", w.retryCount)
				w.err = fmt.Errorf("unable to start process after %v retries, giving up", w.retryCount)
				return
			}
			w.mut.Unlock()

			if w.needRestart {
				if w.cmd != nil && w.cmd.Process != nil {
					w.cmd.Process.Release()
				}
				w.logEvent(true, "about to start '%s'", w.PathToChildExecutable)
				//w.cmd.SysProcAttr = &w.Attr;

				w.mut.Lock()
				if w.retryCount > 0 {
					w.logEvent(true, "sleeping for %v before attempting restart; retryCount = %v (max = %v)", w.RetryInterval, w.retryCount, w.MaxRetries)
					time.Sleep(w.RetryInterval)
				}
				w.mut.Unlock()

				w.cmd = exec.Command(w.PathToChildExecutable, w.Args...)
				w.cmd.Dir = w.Cwd

				w.cmd.Stdout = &w.sout
				w.cmd.Stderr = &w.serr

				if w.startingProcess {
					w.Starting <- true
				} else {
					w.Restarting <- true
				}
				err = w.cmd.Start()
				if err != nil {
					w.err = err
					w.logEvent(true, "unable to start: '%v' '%v' '%v'", w.err, w.sout.String(), w.serr.String())
					return
				}
				w.curPid = w.cmd.Process.Pid
				w.needRestart = false
				w.startCount++
				if w.startingProcess {
					w.startingProcess = false
				}

				w.mut.Lock()
				w.retryCount++

				// reset retry count after an interval of process stability
				go func(currRetryCount int) {
					time.Sleep(w.DeclareSuccessInterval)
					w.mut.Lock()
					if w.retryCount == currRetryCount {
						w.retryCount = 0
					}
					w.mut.Unlock()
				}(w.retryCount)
				w.mut.Unlock()

				w.logEvent(true, "started pid %d '%s'; total start count %d", w.cmd.Process.Pid, w.PathToChildExecutable, w.startCount)
			}

			select {
			case w.CurrentPid <- w.curPid:
			case <-w.TermChildAndStopWatchdog:
				w.logEvent(true, "TermChildAndStopWatchdog noted, exiting watchdog.Start() loop")

				err := w.cmd.Process.Signal(syscall.SIGKILL)
				if err != nil {
					err = fmt.Errorf("warning: watchdog tried to SIGKILL pid %d but got error: '%s'", w.cmd.Process.Pid, err)
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
				err := w.cmd.Process.Signal(syscall.SIGKILL)
				if err != nil {
					err = fmt.Errorf("warning: watchdog tried to SIGKILL pid %d but got error: '%s'", w.cmd.Process.Pid, err)
					w.SetErr(err)
					w.logEvent(false, "%s", err)
					return
				}
				w.curPid = 0
				continue reaploop
			case <-signalChild:
				w.logEvent(true, "got <-signalChild")

				for i := 0; i < 1000; i++ {
					pid, err := syscall.Wait4(w.cmd.Process.Pid, &ws, syscall.WNOHANG, nil)
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
					case pid == w.cmd.Process.Pid:
						w.logEvent(true, "saw OUR current w.cmd.Process.Pid %d/process '%s' finish with waitstatus: %v.", pid, w.PathToChildExecutable, ws)
						w.logEvent(true, "\nstdout: '%v'\nstderr: '%v'\n", w.sout.String(), w.serr.String())
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
				w.SetErr(fmt.Errorf("could not reap child PID %d or obtain wait4(WNOHANG)==0 even after 1000 attempts", w.cmd.Process.Pid))
				w.logEvent(false, "%s", w.err)
				return
			} // end select
		} // end for reaploop
	}()
}
