// Package runtime manages integration-test resources independently of a test runner.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/logging"
	"github.com/testcontainers/testcontainers-go"
)

// Timeouts bound entire phases, not individual containers. Zero uses defaults.
type Timeouts struct{ Prepare, Up, Tests, Down time.Duration }

// Config describes one environment. Setup initializes typed client dependencies
// after all services are ready. Register their closers through AddCleanup.
type Config struct {
	Containers     []ContainerSpec
	Servers        []ServerSpec
	Timeouts       Timeouts
	Logger         *slog.Logger
	LogBufferBytes int
	Setup          func(context.Context, *Environment) error
}

type instance struct {
	name          string
	container     testcontainers.Container
	logs          *logBuffer
	policy        LogPolicy
	ready         bool
	terminateOnce sync.Once
	terminateErr  error
	removed       atomic.Bool
}

func (i *instance) terminate(ctx context.Context) error {
	i.terminateOnce.Do(func() {
		defer func() {
			if r := recover(); r != nil {
				i.terminateErr = fmt.Errorf("terminate panic: %v", r)
			}
		}()
		i.terminateErr = i.container.Terminate(ctx)
		if i.terminateErr == nil {
			i.removed.Store(true)
		}
	})
	return i.terminateErr
}

type cleanup struct {
	name string
	run  func(context.Context) error
}

// Environment owns services, jobs, clients and active tests for one package.
// Create a fresh instance for every run; an environment cannot be restarted.
type Environment struct {
	config          Config
	logger          *slog.Logger
	driver          driver
	ctx             context.Context
	cancel          context.CancelCauseFunc
	mu              sync.Mutex
	specs           map[string]ContainerSpec
	serverSpecs     map[string]ServerSpec
	servers         map[string]*serverInstance
	order           []string
	services        map[string]*instance
	instances       []*instance
	preparationLogs map[string]*logBuffer
	frameworkLogs   *logBuffer
	network         networkResource
	gatewayAddress  string
	networkRemoved  bool
	cleanups        []cleanup
	active          map[*testing.T]*binding
	bindings        map[*testing.T]*binding
	closing         bool
	prepareStarted  bool
	prepared        bool
	upStarted       bool
	up              bool
	operations      sync.WaitGroup
	sequence        uint64
	abortOnce       sync.Once
	downOnce        sync.Once
	downDone        chan struct{}
	downErr         error
}

// New constructs an environment without contacting Docker.
func New(config Config) *Environment {
	defaults := Timeouts{5 * time.Minute, time.Minute, 5 * time.Minute, time.Minute}
	if config.Timeouts.Prepare == 0 {
		config.Timeouts.Prepare = defaults.Prepare
	}
	if config.Timeouts.Up == 0 {
		config.Timeouts.Up = defaults.Up
	}
	if config.Timeouts.Tests == 0 {
		config.Timeouts.Tests = defaults.Tests
	}
	if config.Timeouts.Down == 0 {
		config.Timeouts.Down = defaults.Down
	}
	if config.LogBufferBytes == 0 {
		config.LogBufferBytes = 1 << 20
	}
	config.Servers = slices.Clone(config.Servers)
	config.Containers = slices.Clone(config.Containers)
	for i := range config.Containers {
		config.Containers[i].Request = cloneRequest(config.Containers[i].Request)
		config.Containers[i].DependsOn = slices.Clone(config.Containers[i].DependsOn)
	}
	logger := config.Logger
	if logger == nil {
		logger = logging.New(logging.Config{AppID: "integrationtests"})
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Environment{config: config, logger: logger, driver: dockerDriver{}, ctx: ctx, cancel: cancel, frameworkLogs: &logBuffer{limit: config.LogBufferBytes},
		specs: make(map[string]ContainerSpec), serverSpecs: make(map[string]ServerSpec), servers: make(map[string]*serverInstance), services: make(map[string]*instance), preparationLogs: make(map[string]*logBuffer),
		bindings: make(map[*testing.T]*binding), active: make(map[*testing.T]*binding), downDone: make(chan struct{})}
}

// Timeouts returns the resolved phase limits.
func (env *Environment) Timeouts() Timeouts { return env.config.Timeouts }

// Context is canceled when the environment aborts or shuts down.
func (env *Environment) Context() context.Context { return env.ctx }

// Logger returns the environment's structured logger.
func (env *Environment) Logger() *slog.Logger { return env.logger }

// Abort reports an unrecoverable environment error and fails active bound tests.
// Ordinary assertion and job errors should not be reported here.
func (env *Environment) Abort(err error) {
	if err == nil {
		err = errors.New("environment aborted")
	}
	env.abortOnce.Do(func() {
		env.mu.Lock()
		defer env.mu.Unlock()
		names := make([]string, 0, len(env.active))
		for handle := range env.active {
			handle.Errorf("integration environment aborted: %v", err)
			names = append(names, handle.Name())
		}
		env.logger.Error("integration environment aborted", "error", err, "active_tests", names)
		env.cancel(err)
	})
}

func (env *Environment) begin() error {
	env.mu.Lock()
	defer env.mu.Unlock()
	if env.closing {
		return errors.New("environment is shutting down")
	}
	if err := context.Cause(env.ctx); err != nil {
		return err
	}
	env.operations.Add(1)
	return nil
}

var validName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

func (env *Environment) validate() error {
	t := env.config.Timeouts
	if t.Prepare <= 0 || t.Up <= 0 || t.Tests <= 0 || t.Down <= 0 {
		return errors.New("phase timeouts must be positive")
	}
	if env.config.LogBufferBytes < 0 {
		return errors.New("log buffer size must be positive")
	}
	for _, spec := range env.config.Containers {
		if !validName.MatchString(spec.Name) {
			return fmt.Errorf("invalid container name %q", spec.Name)
		}
		if _, exists := env.specs[spec.Name]; exists {
			return fmt.Errorf("duplicate container %q", spec.Name)
		}
		if spec.Logs > Disabled || spec.LogBufferBytes < 0 {
			return fmt.Errorf("invalid log configuration for %s", spec.Name)
		}
		if spec.Request.Image == "" && !spec.Request.ShouldBuildImage() {
			return fmt.Errorf("container %s needs an image or Dockerfile", spec.Name)
		}
		if spec.Request.Name != "" {
			return fmt.Errorf("container %s: use logical Name, not Request.Name", spec.Name)
		}
		env.specs[spec.Name] = spec
	}
	if err := env.validateServers(); err != nil {
		return err
	}
	return env.validateGraph()
}

func (env *Environment) validateGraph() error {
	visited, visiting := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(name string) error {
		if visiting[name] {
			return fmt.Errorf("dependency cycle at %s", name)
		}
		if visited[name] {
			return nil
		}
		if _, ok := env.serverSpecs[name]; ok {
			visited[name] = true
			env.order = append(env.order, name)
			return nil
		}
		spec, ok := env.specs[name]
		if !ok {
			return fmt.Errorf("unknown dependency %q", name)
		}
		visiting[name] = true
		for _, dependency := range spec.DependsOn {
			if dep, ok := env.specs[dependency]; ok && dep.Manual {
				return fmt.Errorf("%s depends on manual job %s", name, dependency)
			}
			if err := visit(dependency); err != nil {
				return err
			}
		}
		visiting[name] = false
		visited[name] = true
		env.order = append(env.order, name)
		return nil
	}
	for _, spec := range env.config.Containers {
		if err := visit(spec.Name); err != nil {
			return err
		}
	}
	for _, spec := range env.config.Servers {
		if err := visit(spec.Name); err != nil {
			return err
		}
	}
	return nil
}

// Phase runs a phase with a hard wall-clock limit and progress logging. Callbacks
// must honor context cancellation; Go cannot forcibly stop their goroutines.
func (env *Environment) Phase(ctx context.Context, name string, limit time.Duration, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	started := time.Now()
	env.logger.Info("phase started", "phase", name)
	result := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in %s: %v", name, r)
			}
			result <- err
		}()
		err = fn(ctx)
	}()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	var err error
wait:
	for {
		select {
		case err = <-result:
			break wait
		case <-ctx.Done():
			err = context.Cause(ctx)
			break wait
		case <-ticker.C:
			env.logger.Info("phase running", "phase", name, logging.Duration(time.Since(started)))
		}
	}
	if err == nil {
		env.logger.Info("phase finished", "phase", name, logging.Duration(time.Since(started)))
	} else {
		env.logger.Error("phase failed", "phase", name, logging.Error(err), logging.Duration(time.Since(started)))
	}
	return err
}

func (env *Environment) linked(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(env.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

// Prepare validates the dependency graph and resolves every image, including jobs.
func (env *Environment) Prepare(ctx context.Context) error {
	if err := env.begin(); err != nil {
		return err
	}
	env.mu.Lock()
	if env.prepareStarted {
		env.mu.Unlock()
		env.operations.Done()
		return errors.New("environment already prepared")
	}
	env.prepareStarted = true
	err := env.validate()
	env.mu.Unlock()
	if err != nil {
		env.operations.Done()
		return err
	}
	ctx, cancel := env.linked(ctx)
	defer cancel()
	return env.Phase(ctx, "Prepare", env.config.Timeouts.Prepare, func(ctx context.Context) error {
		defer env.operations.Done()
		if err := env.prepareGateway(ctx); err != nil {
			return err
		}
		for _, name := range env.order {
			env.mu.Lock()
			if _, ok := env.serverSpecs[name]; ok {
				env.mu.Unlock()
				continue
			}
			spec := env.specs[name]
			buffer := env.newBuffer(spec)
			env.preparationLogs[name] = buffer
			env.mu.Unlock()
			request, err := env.driver.prepare(ctx, cloneRequest(spec.Request), buffer)
			if err != nil {
				return fmt.Errorf("prepare %s: %w", name, err)
			}
			spec.Request = request
			env.mu.Lock()
			env.specs[name] = spec
			env.mu.Unlock()
		}
		env.mu.Lock()
		if ctx.Err() != nil {
			env.mu.Unlock()
			return ctx.Err()
		}
		env.prepared = true
		env.mu.Unlock()
		return nil
	})
}

func (env *Environment) newBuffer(spec ContainerSpec) *logBuffer {
	size := spec.LogBufferBytes
	if size == 0 {
		size = env.config.LogBufferBytes
	}
	if spec.Logs == Disabled {
		size = 0
	}
	return &logBuffer{limit: size}
}

// Up starts automatic services, then initializes the consumer's typed clients.
func (env *Environment) Up(ctx context.Context) error {
	if err := env.begin(); err != nil {
		return err
	}
	env.mu.Lock()
	if !env.prepared || env.upStarted {
		env.mu.Unlock()
		env.operations.Done()
		return errors.New("Up requires a successfully prepared, unused environment")
	}
	env.upStarted = true
	env.mu.Unlock()
	ctx, cancel := env.linked(ctx)
	defer cancel()
	return env.Phase(ctx, "Up", env.config.Timeouts.Up, func(ctx context.Context) error {
		defer env.operations.Done()
		if err := env.startNetwork(ctx); err != nil {
			return err
		}
		for _, spec := range env.config.Servers {
			if err := env.startServer(ctx, spec); err != nil {
				return fmt.Errorf("start server %s: %w", spec.Name, err)
			}
		}
		if err := env.startGateway(ctx); err != nil {
			return err
		}

		if err := env.startServices(ctx); err != nil {
			return err
		}
		if env.config.Setup != nil {
			if err := env.config.Setup(ctx, env); err != nil {
				return err
			}
		}
		env.mu.Lock()
		if ctx.Err() != nil {
			env.mu.Unlock()
			return ctx.Err()
		}
		env.up = true
		env.mu.Unlock()
		return nil
	})
}

func (env *Environment) startNetwork(ctx context.Context) error {
	if len(env.specs) == 0 {
		return nil
	}
	n, err := env.driver.network(ctx)
	if err != nil {
		return fmt.Errorf("create network: %w", err)
	}
	env.mu.Lock()
	closing := env.closing
	if !closing {
		env.network = n
	}
	env.mu.Unlock()
	if closing {
		return env.removeLateNetwork(n)
	}
	return nil
}

func (env *Environment) startServices(ctx context.Context) error {
	for _, name := range env.order {
		env.mu.Lock()
		spec := env.specs[name]
		_, isServer := env.serverSpecs[name]
		env.mu.Unlock()
		if isServer {
			continue
		}
		if spec.Manual {
			continue
		}
		inst, err := env.start(ctx, spec, nil)
		if err != nil {
			return fmt.Errorf("start %s: %w", name, err)
		}
		go env.monitor(inst)
	}
	return nil
}

func (env *Environment) removeLateNetwork(n networkResource) error {
	ctx, cancel := context.WithTimeout(context.Background(), env.config.Timeouts.Down)
	defer cancel()
	return errors.Join(errors.New("network created during shutdown"), n.remove(ctx))
}

func (env *Environment) start(ctx context.Context, spec ContainerSpec, options []JobOption) (result *instance, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic starting %s: %v", spec.Name, r)
		}
	}()
	env.mu.Lock()
	if env.closing {
		env.mu.Unlock()
		return nil, errors.New("environment is shutting down")
	}
	for _, name := range spec.DependsOn {
		if !env.dependencyReady(name) {
			env.mu.Unlock()
			return nil, fmt.Errorf("dependency %s is not ready", name)
		}
	}
	network := env.network.name
	env.sequence++
	sequence := env.sequence
	env.mu.Unlock()
	request, err := env.instanceRequest(ctx, spec, options, network)
	if err != nil {
		return nil, err
	}
	buffer := env.newBuffer(spec)
	if spec.Logs != Disabled {
		if request.LogConsumerCfg == nil {
			request.LogConsumerCfg = &testcontainers.LogConsumerConfig{}
		}
		request.LogConsumerCfg.Consumers = append(request.LogConsumerCfg.Consumers, buffer)
	}
	name := spec.Name
	if spec.Manual {
		name += "/" + strconv.FormatUint(sequence, 10)
	}
	inst := &instance{name: name, logs: buffer, policy: spec.Logs}
	result = inst
	// Track the handle before user PostCreates hooks, including hooks that panic
	// or ignore cancellation. The fallback handles providers returning partials.
	request.LifecycleHooks = append([]testcontainers.ContainerLifecycleHooks{{PostCreates: []testcontainers.ContainerHook{
		func(_ context.Context, c testcontainers.Container) error { return env.track(inst, c) },
	}}}, request.LifecycleHooks...)
	container, err := env.driver.create(ctx, request, buffer)
	if container != nil {
		err = errors.Join(err, env.track(inst, container))
	}
	if err != nil {
		return inst, err
	}
	if container == nil {
		return inst, errors.New("driver returned a nil container")
	}
	if err := container.Start(ctx); err != nil {
		return inst, err
	}
	if !spec.Manual {
		env.mu.Lock()
		env.services[spec.Name] = inst
		env.mu.Unlock()
	}
	if spec.AfterReady != nil {
		if err := spec.AfterReady(ctx, env, container); err != nil {
			return inst, err
		}
	}
	env.mu.Lock()
	defer env.mu.Unlock()
	if env.closing {
		return inst, errors.New("environment is shutting down")
	}
	inst.ready = true
	if !spec.Manual {
		env.services[spec.Name] = inst
	}
	return inst, nil
}

func (env *Environment) track(inst *instance, container testcontainers.Container) error {
	env.mu.Lock()
	if inst.container == nil {
		inst.container = container
		env.instances = append(env.instances, inst)
	}
	closing := env.closing
	env.mu.Unlock()
	if !closing {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), env.config.Timeouts.Down)
	defer cancel()
	err := bounded(ctx, inst.terminate)
	if err != nil {
		env.logger.Error("late container cleanup failed", "container", inst.name, "id", container.GetContainerID(), "error", err)
	}
	return errors.Join(errors.New("container created during shutdown"), err)
}

func (env *Environment) instanceRequest(ctx context.Context, spec ContainerSpec, options []JobOption, network string) (testcontainers.ContainerRequest, error) {
	request := cloneRequest(spec.Request)
	if spec.Configure != nil {
		if err := spec.Configure(ctx, env, &request); err != nil {
			return request, err
		}
	}
	for _, option := range options {
		option(&request)
	}
	if request.Image != spec.Request.Image || request.ShouldBuildImage() {
		return request, errors.New("configure/job options must not change prepared image")
	}
	if request.Name != "" {
		return request, errors.New("fixed Docker container names are not supported")
	}
	if network != "" {
		request.Networks = append(request.Networks, network)
		if request.NetworkAliases == nil {
			request.NetworkAliases = make(map[string][]string)
		}
		request.NetworkAliases[network] = append(request.NetworkAliases[network], spec.Name)
	}
	env.connectMockGateway(spec, &request)
	return request, nil
}

func (env *Environment) monitor(inst *instance) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-env.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(env.ctx, 5*time.Second)
			state, err := inst.container.State(ctx)
			cancel()
			if env.ctx.Err() != nil {
				return
			}
			if err != nil {
				env.Abort(fmt.Errorf("inspect service %s: %w", inst.name, err))
				return
			}
			if state == nil || !state.Running {
				env.Abort(fmt.Errorf("service %s stopped unexpectedly", inst.name))
				return
			}
		}
	}
}

// HostEndpoint returns host:published-port for a container, or the local address
// of a mock server when the port argument is omitted.
func (env *Environment) HostEndpoint(ctx context.Context, name string, ports ...string) (string, error) {
	if len(ports) == 0 {
		return env.serverEndpoint(name, false)
	}
	if len(ports) != 1 {
		return "", errors.New("expected one container port or no port for a mock server")
	}
	port := ports[0]
	env.mu.Lock()
	inst := env.services[name]
	env.mu.Unlock()
	if inst == nil {
		return "", fmt.Errorf("service %s is not ready", name)
	}
	host, err := inst.container.Host(ctx)
	if err != nil {
		return "", err
	}
	mapped, err := inst.container.MappedPort(ctx, port)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, mapped.Port()), nil
}

// NetworkEndpoint returns alias:internal-port for a container. With no port, it
// returns the mock gateway address; declare that server in DependsOn to enable access.
func (env *Environment) NetworkEndpoint(name string, ports ...string) (string, error) {
	if len(ports) == 0 {
		return env.serverEndpoint(name, true)
	}
	if len(ports) != 1 {
		return "", errors.New("expected one container port or no port for a mock server")
	}
	port := ports[0]
	env.mu.Lock()
	defer env.mu.Unlock()
	if inst := env.services[name]; inst == nil || !inst.ready {
		return "", fmt.Errorf("service %s is not ready", name)
	}
	number, _, _ := strings.Cut(port, "/")
	value, err := strconv.Atoi(number)
	if err != nil || value < 1 || value > 65535 {
		return "", fmt.Errorf("invalid port %q", port)
	}
	return net.JoinHostPort(name, number), nil
}

// AddCleanup registers a client closer; closers run in reverse registration order.
func (env *Environment) AddCleanup(name string, fn func(context.Context) error) error {
	if fn == nil {
		return errors.New("nil cleanup")
	}
	env.mu.Lock()
	if !env.closing {
		env.cleanups = append(env.cleanups, cleanup{name, fn})
		env.mu.Unlock()
		return nil
	}
	env.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), env.config.Timeouts.Down)
	defer cancel()
	return errors.Join(errors.New("cleanup registered during shutdown"), bounded(ctx, fn))
}
