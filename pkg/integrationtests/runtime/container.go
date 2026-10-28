package runtime

import (
	"context"
	"maps"
	"slices"

	"github.com/testcontainers/testcontainers-go"
)

// LogPolicy controls whether diagnostics include a container's output.
type LogPolicy uint8

const (
	// OnFailure buffers output and prints it only on failure.
	OnFailure LogPolicy = iota
	// Disabled does not collect container output.
	Disabled
)

// ContainerSpec describes a reusable service or manually invoked job.
// Configure runs on a fresh copy after dependencies are ready. It must not change
// the image/build definition: images are resolved during Prepare.
// Opaque extensions (wait strategies, readers and modifier closures) are owned by
// the caller; use Configure to allocate fresh instances for concurrent jobs.
type ContainerSpec struct {
	Name           string
	Request        testcontainers.ContainerRequest
	DependsOn      []string
	Manual         bool
	Logs           LogPolicy
	LogBufferBytes int
	Configure      func(context.Context, *Environment, *testcontainers.ContainerRequest) error
	AfterReady     func(context.Context, *Environment, testcontainers.Container) error
}

// JobResult identifies one invocation, including its exit status and log tail.
type JobResult struct {
	ID       string
	ExitCode int
	Logs     string
}

// JobOption changes one invocation without modifying its template.
type JobOption func(*testcontainers.ContainerRequest)

// WithCommand replaces the command for one invocation.
func WithCommand(command ...string) JobOption {
	command = slices.Clone(command)
	return func(r *testcontainers.ContainerRequest) { r.Cmd = slices.Clone(command) }
}

// WithEnv merges environment variables into one invocation.
func WithEnv(values map[string]string) JobOption {
	values = maps.Clone(values)
	return func(r *testcontainers.ContainerRequest) {
		if r.Env == nil {
			r.Env = make(map[string]string)
		}
		maps.Copy(r.Env, values)
	}
}

func cloneRequest(r testcontainers.ContainerRequest) testcontainers.ContainerRequest {
	r.Env = maps.Clone(r.Env)
	r.Labels = maps.Clone(r.Labels)
	r.Tmpfs = maps.Clone(r.Tmpfs)
	r.Cmd = slices.Clone(r.Cmd)
	r.Entrypoint = slices.Clone(r.Entrypoint)
	r.ExposedPorts = slices.Clone(r.ExposedPorts)
	r.HostAccessPorts = slices.Clone(r.HostAccessPorts)
	r.Networks = slices.Clone(r.Networks)
	r.NetworkAliases = maps.Clone(r.NetworkAliases)
	for key, aliases := range r.NetworkAliases {
		r.NetworkAliases[key] = slices.Clone(aliases)
	}
	r.Files = slices.Clone(r.Files)
	r.Mounts = slices.Clone(r.Mounts)
	r.ImageSubstitutors = slices.Clone(r.ImageSubstitutors)
	r.LifecycleHooks = slices.Clone(r.LifecycleHooks)
	for i := range r.LifecycleHooks {
		h := &r.LifecycleHooks[i]
		h.PreBuilds = slices.Clone(h.PreBuilds)
		h.PostBuilds = slices.Clone(h.PostBuilds)
		h.PreCreates = slices.Clone(h.PreCreates)
		h.PostCreates = slices.Clone(h.PostCreates)
		h.PreStarts = slices.Clone(h.PreStarts)
		h.PostStarts = slices.Clone(h.PostStarts)
		h.PostReadies = slices.Clone(h.PostReadies)
		h.PreStops = slices.Clone(h.PreStops)
		h.PostStops = slices.Clone(h.PostStops)
		h.PreTerminates = slices.Clone(h.PreTerminates)
		h.PostTerminates = slices.Clone(h.PostTerminates)
	}
	r.BuildArgs = maps.Clone(r.BuildArgs)
	for k, v := range r.BuildArgs {
		if v != nil {
			value := *v
			r.BuildArgs[k] = &value
		}
	}
	if r.LogConsumerCfg != nil {
		cfg := *r.LogConsumerCfg
		cfg.Consumers = slices.Clone(cfg.Consumers)
		r.LogConsumerCfg = &cfg
	}
	return r
}
