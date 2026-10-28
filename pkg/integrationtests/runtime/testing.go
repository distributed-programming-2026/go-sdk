package runtime

import (
	"context"
	"testing"
	"time"

	tclog "github.com/testcontainers/testcontainers-go/log"
)

type binding struct {
	ctx   context.Context
	since time.Time
}

// Bind associates a test with environment cancellation and failure diagnostics.
// It is idempotent for the same *testing.T. Use the returned context for I/O.
// Nested plain subtests must bind their own *testing.T to get separate diagnostics.
func (env *Environment) Bind(t *testing.T) context.Context {
	t.Helper()
	env.mu.Lock()
	if b := env.bindings[t]; b != nil {
		ctx := b.ctx
		env.mu.Unlock()
		return ctx
	}
	if cause := context.Cause(env.ctx); cause != nil {
		env.mu.Unlock()
		t.Fatalf("integration environment unavailable: %v", cause)
		return env.ctx
	}
	ctx, cancel := context.WithCancel(t.Context())
	stop := context.AfterFunc(env.ctx, cancel)
	b := &binding{ctx: ctx, since: time.Now()}
	env.bindings[t] = b
	env.active[t] = b
	t.Cleanup(func() {
		env.mu.Lock()
		delete(env.active, t)
		if t.Failed() {
			env.testLogsLocked(t, b.since)
		}
		env.mu.Unlock()
		stop()
		cancel()
	})
	env.mu.Unlock()
	return ctx
}

func (env *Environment) testLogsLocked(t *testing.T, since time.Time) {
	for _, server := range env.servers {
		if server.policy == OnFailure {
			if output := server.logs.text(since); output != "" {
				t.Logf("server %s:\n%s", server.name, output)
			}
		}
	}
	for _, inst := range env.instances {
		if inst.policy != OnFailure {
			continue
		}
		if output := inst.logs.text(since); output != "" {
			t.Logf("container %s:\n%s", inst.name, output)
		}
	}
}

// CaptureFrameworkLogs redirects testcontainers' process-global diagnostic logger
// into the environment buffer. Call only from TestMain, before starting Docker
// operations or tests; testcontainers does not synchronize changing its logger.
// Run/RunContext call this automatically. The logger remains installed until exit.
func (env *Environment) CaptureFrameworkLogs() { tclog.SetDefault(env.frameworkLogs) }

// DumpLogs writes all enabled diagnostic tails to the environment logger.
func (env *Environment) DumpLogs() {
	env.mu.Lock()
	defer env.mu.Unlock()
	if output := env.frameworkLogs.text(time.Time{}); output != "" {
		env.logger.Error("testcontainers diagnostics", "output", output)
	}
	for name, server := range env.servers {
		if server.policy == OnFailure {
			if output := server.logs.text(time.Time{}); output != "" {
				env.logger.Error("server diagnostics", "server", name, "output", output)
			}
		}
	}
	for name, buffer := range env.preparationLogs {
		if output := buffer.text(time.Time{}); output != "" {
			env.logger.Error("image preparation diagnostics", "container", name, "output", output)
		}
	}
	for _, inst := range env.instances {
		if inst.policy != OnFailure {
			continue
		}
		if output := inst.logs.text(time.Time{}); output != "" {
			env.logger.Error("container diagnostics", "container", inst.name, "output", output)
		}
	}
}
