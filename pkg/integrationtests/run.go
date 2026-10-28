// Package integrationtests runs package-wide container environments from TestMain.
package integrationtests

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests/runtime"
)

// Public aliases allow consumers to use one import for environment configuration.
type (
	Environment   = runtime.Environment
	Config        = runtime.Config
	Timeouts      = runtime.Timeouts
	ContainerSpec = runtime.ContainerSpec
	ServerSpec    = runtime.ServerSpec
	Server        = runtime.Server
	ServerLogger  = runtime.ServerLogger
	LogPolicy     = runtime.LogPolicy
	JobResult     = runtime.JobResult
	JobOption     = runtime.JobOption
)

const (
	OnFailure = runtime.OnFailure
	Disabled  = runtime.Disabled
)

// New creates an environment without accessing Docker.
func New(config Config) *Environment { return runtime.New(config) }

// WithCommand overrides the command for one job invocation.
func WithCommand(command ...string) JobOption { return runtime.WithCommand(command...) }

// WithEnv adds environment variables to one job invocation.
func WithEnv(values map[string]string) JobOption { return runtime.WithEnv(values) }

// Run handles signals, phase timeouts and cleanup. Use os.Exit(Run(m, env)) in
// TestMain. On an unrecoverable timeout it exits after bounded cleanup, because
// testing.M.Run and arbitrary test goroutines cannot be forcibly canceled.
func Run(m *testing.M, env *Environment) int { return RunContext(context.Background(), m, env) }

// RunContext also aborts when the supplied context is canceled.
func RunContext(ctx context.Context, m *testing.M, env *Environment) int {
	if !flag.Parsed() {
		flag.Parse()
	}
	if err := validateTestingTimeout(env.Timeouts()); err != nil {
		env.Logger().Error("invalid test timeout", "error", err)
		return 1
	}
	env.CaptureFrameworkLogs()
	ctx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	stopAbort := context.AfterFunc(ctx, func() { env.Abort(context.Cause(ctx)) })
	defer stopAbort()
	code, aborted := execute(env, m.Run)
	if ctx.Err() != nil {
		code = 1
		aborted = true
	}
	if aborted {
		stopAbort()
		stopSignals()
		os.Exit(code) //nolint:gocritic // Cleanup has completed; hung test goroutines must not survive.
	}
	return code
}

func validateTestingTimeout(limits Timeouts) error {
	value := flag.Lookup("test.timeout")
	if value == nil {
		return nil
	}
	duration, err := time.ParseDuration(value.Value.String())
	if err != nil {
		return err
	}
	if duration != 0 && duration <= limits.Tests+limits.Down {
		return fmt.Errorf("-test.timeout=%s must exceed TestsTimeout + DownTimeout (%s), or be 0", duration, limits.Tests+limits.Down)
	}
	return nil
}

func execute(env *Environment, tests func() int) (int, bool) {
	var runErr error
	code := 1
	if runErr = env.Prepare(env.Context()); runErr == nil {
		runErr = env.Up(env.Context())
	}
	if runErr == nil {
		runErr = env.Phase(env.Context(), "Tests", env.Timeouts().Tests, func(context.Context) error {
			result := tests()
			if result != 0 {
				return testFailure{result}
			}
			return nil
		})
	}
	if cause := context.Cause(env.Context()); cause != nil {
		runErr = cause
	}
	var failure testFailure
	aborted := runErr != nil && !errors.As(runErr, &failure)
	if aborted {
		env.Abort(runErr)
	}
	if runErr != nil {
		env.DumpLogs()
	}
	if runErr == nil {
		code = 0
	} else if !aborted {
		code = failure.code
	}
	if err := env.Down(context.Background()); err != nil {
		env.Logger().Error("environment cleanup failed", "error", err)
		env.DumpLogs()
		code = 1
		aborted = true
	}
	return code, aborted
}

type testFailure struct{ code int }

func (err testFailure) Error() string {
	return fmt.Sprintf("test run failed with exit code %d", err.code)
}
