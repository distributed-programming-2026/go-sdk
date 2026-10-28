package runtime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
)

// Server is an in-process network dependency. Serve blocks until shutdown.
// Shutdown must stop active connections and honor its context.
type Server interface {
	Serve(net.Listener) error
	Shutdown(context.Context) error
}

// ServerLogger buffers diagnostics using the environment's failure-only policy.
type ServerLogger interface{ Printf(string, ...any) }

// ServerSpec creates a fresh mock server during Up, before dependent containers.
// New's context bounds startup only; do not use it as a request lifetime context.
// Servers listen on a dynamically allocated loopback port, never a public socket.
type ServerSpec struct {
	Name           string
	Logs           LogPolicy
	LogBufferBytes int
	New            func(context.Context, ServerLogger) (Server, error)
}

type serverInstance struct {
	name     string
	server   Server
	logs     *logBuffer
	policy   LogPolicy
	mu       sync.Mutex
	listener net.Listener
	address  string
	port     int
	ready    bool
	stopOnce sync.Once
	stopErr  error
}

func (s *serverInstance) shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() {
		defer s.closeListener()
		s.stopErr = s.server.Shutdown(ctx)
	})
	return s.stopErr
}

func (s *serverInstance) closeListener() {
	s.mu.Lock()
	listener := s.listener
	s.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
}

func (env *Environment) validateServers() error {
	for _, spec := range env.config.Servers {
		if !validName.MatchString(spec.Name) {
			return fmt.Errorf("invalid server name %q", spec.Name)
		}
		if _, ok := env.specs[spec.Name]; ok {
			return fmt.Errorf("duplicate dependency %q", spec.Name)
		}
		if _, ok := env.serverSpecs[spec.Name]; ok {
			return fmt.Errorf("duplicate server %q", spec.Name)
		}
		if spec.New == nil || spec.Logs > Disabled || spec.LogBufferBytes < 0 {
			return fmt.Errorf("invalid server configuration for %s", spec.Name)
		}
		env.serverSpecs[spec.Name] = spec
	}
	return nil
}

func (env *Environment) startServer(ctx context.Context, spec ServerSpec) error {
	buffer := env.newBuffer(ContainerSpec{Logs: spec.Logs, LogBufferBytes: spec.LogBufferBytes})
	server, err := spec.New(ctx, buffer)
	if server == nil {
		return errors.Join(err, fmt.Errorf("server %s factory returned nil", spec.Name))
	}
	inst := &serverInstance{name: spec.Name, server: server, logs: buffer, policy: spec.Logs}
	env.mu.Lock()
	env.servers[spec.Name] = inst
	closing := env.closing
	env.mu.Unlock()
	if closing {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), env.config.Timeouts.Down)
		defer cancel()
		return errors.Join(err, errors.New("server created during shutdown"), bounded(cleanupCtx, inst.shutdown))
	}
	if err != nil {
		return err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		return err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		_ = listener.Close()
		return err
	}
	env.mu.Lock()
	if env.closing {
		env.mu.Unlock()
		_ = listener.Close()
		return errors.New("server listening during shutdown")
	}
	inst.mu.Lock()
	inst.listener = listener
	inst.mu.Unlock()
	inst.address = listener.Addr().String()
	inst.port = port
	inst.ready = true
	env.mu.Unlock()
	go env.serveServer(inst, listener)
	return nil
}

func (env *Environment) serveServer(inst *serverInstance, listener net.Listener) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
		env.mu.Lock()
		closing := env.closing
		env.mu.Unlock()
		if !closing && env.ctx.Err() == nil {
			env.Abort(fmt.Errorf("server %s stopped unexpectedly: %w", inst.name, errors.Join(err, errors.New("serve returned"))))
		}
	}()
	err = inst.server.Serve(listener)
}

func (env *Environment) serverEndpoint(name string, network bool) (string, error) {
	env.mu.Lock()
	defer env.mu.Unlock()
	server := env.servers[name]
	if server == nil || !server.ready || env.closing {
		return "", fmt.Errorf("server %s is not ready", name)
	}
	if network {
		return net.JoinHostPort(serverGatewayHost, strconv.Itoa(server.port)), nil
	}
	return server.address, nil
}

func (env *Environment) dependencyReady(name string) bool {
	if server := env.servers[name]; server != nil {
		return server.ready
	}
	return env.services[name] != nil && env.services[name].ready
}
