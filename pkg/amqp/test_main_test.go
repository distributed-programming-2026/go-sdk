package amqp

import (
	"context"
	"os"
	"testing"

	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests"
	"github.com/testcontainers/testcontainers-go"
	tcwait "github.com/testcontainers/testcontainers-go/wait"
)

var (
	testAMQPURL string
	testRabbit  testcontainers.Container
)

var testEnv = integrationtests.New(integrationtests.Config{
	Containers: []integrationtests.ContainerSpec{{
		Name: "rabbitmq",
		Request: testcontainers.ContainerRequest{
			Image:        "rabbitmq:4.1-alpine",
			ExposedPorts: []string{"5672/tcp"},
			Env: map[string]string{ // #nosec G101 -- ephemeral integration-test credentials.
				"RABBITMQ_DEFAULT_USER": "sdk", "RABBITMQ_DEFAULT_PASS": "sdk-password",
			},
			WaitingFor: tcwait.ForLog("Server startup complete").WithStartupTimeout(defaultTestTimeout),
		},
		AfterReady: func(_ context.Context, _ *integrationtests.Environment, container testcontainers.Container) error {
			testRabbit = container
			return nil
		},
	}},
	Setup: func(ctx context.Context, env *integrationtests.Environment) error {
		endpoint, err := env.HostEndpoint(ctx, "rabbitmq", "5672/tcp")
		if err != nil {
			return err
		}
		testAMQPURL = "amqp://sdk:sdk-password@" + endpoint + "/"
		return nil
	},
})

func TestMain(m *testing.M) { os.Exit(integrationtests.Run(m, testEnv)) }
