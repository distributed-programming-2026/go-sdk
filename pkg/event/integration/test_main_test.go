package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	testMySQLDSN string
	testAMQPURL  string
)

var testEnv = integrationtests.New(integrationtests.Config{
	Containers: []integrationtests.ContainerSpec{
		{Name: "mysql", Request: testcontainers.ContainerRequest{
			Image: "mysql:8.4", ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{ // #nosec G101 -- ephemeral integration-test credentials.
				"MYSQL_DATABASE": "event_sdk_test", "MYSQL_USER": "event_sdk", "MYSQL_PASSWORD": "event_sdk_password", "MYSQL_ROOT_PASSWORD": "event_sdk_root",
			},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server"),
		}},
		{Name: "rabbitmq", Request: testcontainers.ContainerRequest{
			Image: "rabbitmq:4.1-alpine", ExposedPorts: []string{"5672/tcp"},
			Env:        map[string]string{"RABBITMQ_DEFAULT_USER": "event-sdk", "RABBITMQ_DEFAULT_PASS": "event-sdk-password"}, // #nosec G101 -- ephemeral integration-test credentials.
			WaitingFor: wait.ForLog("Server startup complete").WithStartupTimeout(45 * time.Second),
		}},
	},
	Setup: func(ctx context.Context, env *integrationtests.Environment) error {
		mysql, err := env.HostEndpoint(ctx, "mysql", "3306/tcp")
		if err != nil {
			return err
		}
		rabbit, err := env.HostEndpoint(ctx, "rabbitmq", "5672/tcp")
		if err != nil {
			return err
		}
		testMySQLDSN = "event_sdk:event_sdk_password@tcp(" + mysql + ")/event_sdk_test?parseTime=true"
		testAMQPURL = "amqp://event-sdk:event-sdk-password@" + rabbit + "/"
		return nil
	},
})

func TestMain(m *testing.M) { os.Exit(integrationtests.Run(m, testEnv)) }
