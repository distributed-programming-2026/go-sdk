package runtime

import (
	"context"
	"errors"
	"fmt"
)

// bounded allows cleanup to continue even if a user hook ignores its context.
func bounded(ctx context.Context, fn func(context.Context) error) error {
	result := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("cleanup panic: %v", r)
			}
			result <- err
		}()
		err = fn(ctx)
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Down closes clients and removes resources once. The caller should supply an
// independent cleanup context, not the canceled execution context.
func (env *Environment) Down(ctx context.Context) error {
	env.downOnce.Do(func() {
		env.mu.Lock()
		env.closing = true
		env.cancel(errors.New("environment shut down"))
		env.mu.Unlock()
		go func() {
			defer close(env.downDone)
			env.downErr = env.Phase(ctx, "Down", env.config.Timeouts.Down, env.cleanup)
			if env.downErr != nil {
				env.reportRemaining()
			}
		}()
	})
	<-env.downDone
	return env.downErr
}

func (env *Environment) cleanup(ctx context.Context) error {
	var errs []error
	env.mu.Lock()
	clients := append([]cleanup(nil), env.cleanups...)
	resources := append([]*instance(nil), env.instances...)
	network := env.network
	env.mu.Unlock()
	for i := len(clients) - 1; i >= 0; i-- {
		closer := clients[i]
		if err := bounded(ctx, closer.run); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", closer.name, err))
			env.logger.Error("client cleanup failed", "client", closer.name, "error", err)
		}
	}
	for i := len(resources) - 1; i >= 0; i-- {
		inst := resources[i]
		if err := bounded(ctx, inst.terminate); err != nil {
			errs = append(errs, fmt.Errorf("remove %s (%s): %w", inst.name, inst.container.GetContainerID(), err))
			env.logger.Error("container cleanup failed", "container", inst.name, "id", inst.container.GetContainerID(), "error", err)
		}
	}
	if network.remove != nil {
		if err := bounded(ctx, network.remove); err != nil {
			errs = append(errs, fmt.Errorf("remove network %s: %w", network.name, err))
			env.logger.Error("network cleanup failed", "network", network.name, "error", err)
		} else {
			env.mu.Lock()
			env.networkRemoved = true
			env.mu.Unlock()
		}
	}
	// Mocks remain available until dependent containers have stopped.
	env.mu.Lock()
	servers := make([]*serverInstance, 0, len(env.servers))
	for _, server := range env.servers {
		servers = append(servers, server)
	}
	env.mu.Unlock()
	for _, server := range servers {
		if err := bounded(ctx, server.shutdown); err != nil {
			server.closeListener()
			errs = append(errs, fmt.Errorf("close server %s: %w", server.name, err))
			env.logger.Error("server cleanup failed", "server", server.name, "error", err)
		}
	}
	// Remove known resources before waiting: an uncooperative setup hook must
	// not consume the cleanup budget while its already-created service stays up.
	finished := make(chan struct{})
	go func() { env.operations.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-ctx.Done():
		errs = append(errs, fmt.Errorf("waiting for in-flight operations: %w", context.Cause(ctx)))
	}
	return errors.Join(errs...)
}

func (env *Environment) reportRemaining() {
	env.mu.Lock()
	defer env.mu.Unlock()
	for _, inst := range env.instances {
		if !inst.removed.Load() {
			env.logger.Error("container removal not confirmed", "container", inst.name, "id", inst.container.GetContainerID())
		}
	}
	if env.network.remove != nil && !env.networkRemoved {
		env.logger.Error("network removal not confirmed", "network", env.network.name)
	}
}
