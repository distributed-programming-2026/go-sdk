package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcwait "github.com/testcontainers/testcontainers-go/wait"
)

var (
	testMySQLDSN string
	testAMQPURL  string
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	mysqlContainer, err := tcmysql.Run(
		ctx,
		"mysql:8.4",
		tcmysql.WithDatabase("event_sdk_test"),
		tcmysql.WithUsername("event_sdk"),
		tcmysql.WithPassword("event_sdk_password"),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start MySQL test container: %v\n", err)
		os.Exit(1)
	}
	testMySQLDSN, err = mysqlContainer.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		_ = testcontainers.TerminateContainer(mysqlContainer)
		fmt.Fprintf(os.Stderr, "get MySQL test container address: %v\n", err)
		os.Exit(1)
	}

	rabbitContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "rabbitmq:4.1-alpine",
			ExposedPorts: []string{"5672/tcp"},
			Env: map[string]string{ // #nosec G101 -- ephemeral integration-test credentials.
				"RABBITMQ_DEFAULT_USER": "event-sdk",
				"RABBITMQ_DEFAULT_PASS": "event-sdk-password",
			},
			WaitingFor: tcwait.ForLog("Server startup complete").WithStartupTimeout(45 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		_ = testcontainers.TerminateContainer(mysqlContainer)
		fmt.Fprintf(os.Stderr, "start RabbitMQ test container: %v\n", err)
		os.Exit(1)
	}
	host, err := rabbitContainer.Host(ctx)
	if err == nil {
		var port string
		mapped, mapErr := rabbitContainer.MappedPort(ctx, "5672/tcp")
		if mapErr != nil {
			err = mapErr
		} else {
			port = mapped.Port()
		}
		if err == nil {
			testAMQPURL = fmt.Sprintf("amqp://event-sdk:event-sdk-password@%s:%s/", host, port)
		}
	}
	if err != nil {
		_ = testcontainers.TerminateContainer(rabbitContainer)
		_ = testcontainers.TerminateContainer(mysqlContainer)
		fmt.Fprintf(os.Stderr, "get RabbitMQ test container address: %v\n", err)
		os.Exit(1)
	}

	exitCode := m.Run()
	if err := testcontainers.TerminateContainer(rabbitContainer); err != nil && exitCode == 0 {
		fmt.Fprintf(os.Stderr, "terminate RabbitMQ test container: %v\n", err)
		exitCode = 1
	}
	if err := testcontainers.TerminateContainer(mysqlContainer); err != nil && exitCode == 0 {
		fmt.Fprintf(os.Stderr, "terminate MySQL test container: %v\n", err)
		exitCode = 1
	}
	os.Exit(exitCode)
}
