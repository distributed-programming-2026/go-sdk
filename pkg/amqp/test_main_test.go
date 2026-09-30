package amqp

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcwait "github.com/testcontainers/testcontainers-go/wait"
)

var (
	testAMQPURL string
	testRabbit  testcontainers.Container
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "rabbitmq:4.1-alpine", ExposedPorts: []string{"5672/tcp"},
			Env:        map[string]string{"RABBITMQ_DEFAULT_USER": "sdk", "RABBITMQ_DEFAULT_PASS": "sdk-password"},
			WaitingFor: tcwait.ForLog("Server startup complete").WithStartupTimeout(defaultTestTimeout),
		}, Started: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start RabbitMQ test container: %v\n", err)
		os.Exit(1)
	}
	testRabbit = container
	host, err := container.Host(ctx)
	if err == nil {
		var port string
		mapped, mapErr := container.MappedPort(ctx, "5672/tcp")
		if mapErr != nil {
			err = mapErr
		} else {
			port = mapped.Port()
		}
		if err == nil {
			testAMQPURL = fmt.Sprintf("amqp://sdk:sdk-password@%s:%s/", host, port)
		}
	}
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		fmt.Fprintf(os.Stderr, "get RabbitMQ address: %v\n", err)
		os.Exit(1)
	}
	exitCode := m.Run()
	if err := testcontainers.TerminateContainer(container); err != nil && exitCode == 0 {
		fmt.Fprintf(os.Stderr, "terminate RabbitMQ test container: %v\n", err)
		exitCode = 1
	}
	os.Exit(exitCode)
}
