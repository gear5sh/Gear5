package safego

import (
	"runtime/debug"
	"time"

	"github.com/piyushsingariya/syndicate/logger"
)

const defaultRestartTimeout = 2 * time.Second

type RecoverHandler func(value interface{})

var GlobalRecoverHandler RecoverHandler

var (
	startTime time.Time
)

type Execution struct {
	f              func()
	recoverHandler RecoverHandler
	restartTimeout time.Duration
}

// Run runs a new goroutine and add panic handler (without restart)
func Run(f func()) *Execution {
	exec := Execution{
		f:              f,
		recoverHandler: GlobalRecoverHandler,
		restartTimeout: 0,
	}
	return exec.run()
}

// RunWithRestart run a new goroutine and add panic handler:
// write logs, wait 2 seconds and restart the goroutine
func RunWithRestart(f func()) *Execution {
	exec := Execution{
		f:              f,
		recoverHandler: GlobalRecoverHandler,
		restartTimeout: defaultRestartTimeout,
	}
	return exec.run()
}

func (exec *Execution) run() *Execution {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				exec.recoverHandler(r)

				if exec.restartTimeout > 0 {
					time.Sleep(exec.restartTimeout)
					exec.run()
				}
			}
		}()
		exec.f()
	}()
	return exec
}

func (exec *Execution) WithRestartTimeout(timeout time.Duration) *Execution {
	exec.restartTimeout = timeout
	return exec
}

func init() {
	GlobalRecoverHandler = func(value interface{}) {
		logger.Error("panic")
		logger.Error(value)
		logger.Error(string(debug.Stack()))
	}
}

func Recovery() {
	err := recover()
	if err != nil {
		logger.Error(err)
		// capture stacks trace
		logger.Error(string(debug.Stack()))
	}
	logger.Infof("Time of execution %v", time.Now().Sub(startTime).String())
}

func Insert[T any](ch chan<- T, value T) bool {
	select {
	case ch <- value:
		return true
	default:
		return false
	}
}

func ChannelClosed[T any](ch chan T) bool {
	// Check if the channel is not closed
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func init() {
	startTime = time.Now()
}
