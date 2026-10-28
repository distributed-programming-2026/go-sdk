package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

const (
	gatewayImage = "alpine:3.22"
	// Matches testcontainers-go v0.44.0's HostAccessPorts helper.
	forwardingImage   = "testcontainers/sshd:1.4.0"
	serverGatewayHost = "mocks.integrationtests.internal"
)

func (env *Environment) needsGateway() bool {
	for _, spec := range env.specs {
		for _, name := range spec.DependsOn {
			if _, ok := env.serverSpecs[name]; ok {
				return true
			}
		}
	}
	return false
}

func (env *Environment) prepareGateway(ctx context.Context) error {
	if !env.needsGateway() {
		return nil
	}
	for _, image := range []string{gatewayImage, forwardingImage} {
		buffer := env.newBuffer(ContainerSpec{})
		env.mu.Lock()
		env.preparationLogs[image] = buffer
		env.mu.Unlock()
		if _, err := env.driver.prepare(ctx, testcontainers.ContainerRequest{Image: image}, buffer); err != nil {
			return fmt.Errorf("prepare mock gateway: %w", err)
		}
	}
	return nil
}

func (env *Environment) startGateway(ctx context.Context) error {
	if !env.needsGateway() {
		return nil
	}
	var ports []int
	for _, spec := range env.config.Servers {
		ports = append(ports, env.servers[spec.Name].port)
	}
	// This container's readiness creates testcontainers' SSH forwarding before any
	// application is started. Its helper and tunnels live until all clients stop.
	name := fmt.Sprintf("sdk-mock-gateway-%d", time.Now().UnixNano())
	inst, err := env.start(ctx, ContainerSpec{Name: name, Request: testcontainers.ContainerRequest{Image: gatewayImage, Cmd: []string{"sh", "-c", "trap 'exit 0' TERM INT; sleep 2147483647 & wait"}, HostAccessPorts: ports}}, nil)
	if err != nil {
		return fmt.Errorf("start mock gateway: %w", err)
	}
	info, err := inst.container.Inspect(ctx)
	if err != nil {
		return fmt.Errorf("inspect mock gateway: %w", err)
	}
	if info.HostConfig == nil {
		return errors.New("mock gateway has no host configuration")
	}
	for _, entry := range info.HostConfig.ExtraHosts {
		address, ok := strings.CutPrefix(entry, testcontainers.HostInternal+":")
		if !ok {
			continue
		}
		env.mu.Lock()
		env.gatewayAddress = address
		env.mu.Unlock()
		go env.monitor(inst)
		return nil
	}
	return errors.New("mock gateway has no forwarding address")
}

func (env *Environment) connectMockGateway(spec ContainerSpec, request *testcontainers.ContainerRequest) {
	env.mu.Lock()
	defer env.mu.Unlock()
	for _, name := range spec.DependsOn {
		if env.servers[name] == nil {
			continue
		}
		address := env.gatewayAddress
		original := request.HostConfigModifier
		request.HostConfigModifier = func(config *dockercontainer.HostConfig) {
			if original != nil {
				original(config)
			}
			config.ExtraHosts = append(config.ExtraHosts, serverGatewayHost+":"+address)
		}
		return
	}
}
