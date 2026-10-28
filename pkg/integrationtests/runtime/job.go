package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// RunJob creates a fresh invocation of a manual spec. Its context is linked to
// environment cancellation. Nonzero exits are returned to the calling test.
func (env *Environment) RunJob(ctx context.Context, name string, options ...JobOption) (result JobResult, err error) {
	result.ExitCode = -1
	if err := env.begin(); err != nil {
		return result, err
	}
	defer env.operations.Done()
	env.mu.Lock()
	spec, ok := env.specs[name]
	up := env.up
	env.mu.Unlock()
	if !ok || !spec.Manual || !up {
		return result, fmt.Errorf("%s is not an available manual job", name)
	}
	ctx, cancel := env.linked(ctx)
	defer cancel()
	inst, err := env.start(ctx, spec, options)
	if inst == nil {
		return result, err
	}
	result.ID = inst.name
	result.ExitCode = -1
	defer func() {
		if inst.container != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), env.config.Timeouts.Down)
			// A short-lived job may exit before the streaming consumer receives its
			// last frame. Read its final output before removal, then stop the producer.
			logsCtx, logsCancel := context.WithTimeout(cleanupCtx, min(2*time.Second, env.config.Timeouts.Down/4))
			snapshot := env.finalJobLogs(logsCtx, inst)
			logsCancel()
			err = errors.Join(err, bounded(cleanupCtx, inst.terminate))
			if snapshot != nil {
				inst.logs.replace(snapshot)
			}
			cleanupCancel()
		}
		result.Logs = inst.logs.text(time.Time{})
	}()
	if err != nil {
		return result, err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, stateErr := inst.container.State(ctx)
		if stateErr != nil {
			return result, stateErr
		}
		if state == nil {
			return result, errors.New("job state is unavailable")
		}
		if !state.Running {
			result.ExitCode = state.ExitCode
			if state.ExitCode != 0 {
				return result, fmt.Errorf("job %s exited with code %d", name, state.ExitCode)
			}
			return result, nil
		}
		select {
		case <-ctx.Done():
			return result, context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func (env *Environment) finalJobLogs(ctx context.Context, inst *instance) *logBuffer {
	if inst.policy == Disabled {
		return nil
	}
	stream, err := inst.container.Logs(ctx)
	if err != nil {
		env.logger.Warn("read final job logs", "container", inst.name, "error", err)
		return nil
	}
	defer stream.Close()
	snapshot := &logBuffer{limit: inst.logs.limit}
	if _, err := io.Copy(snapshot, stream); err != nil {
		env.logger.Warn("read final job logs", "container", inst.name, "error", err)
		return nil
	}
	return snapshot
}
