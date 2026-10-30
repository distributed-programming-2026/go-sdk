package uow_test

import (
	"context"
	"os"
	"testing"

	"github.com/distributed-programming-2026/go-sdk/pkg/integrationtests"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const mysqlImage = "mysql:8.4"

var testDSN string

var testEnv = integrationtests.New(integrationtests.Config{
	Containers: []integrationtests.ContainerSpec{{
		Name: "mysql",
		Request: testcontainers.ContainerRequest{
			Image:        mysqlImage,
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{ // #nosec G101 -- ephemeral integration-test credentials.
				"MYSQL_DATABASE": "uow_sdk_test", "MYSQL_USER": "uow_sdk", "MYSQL_PASSWORD": "uow_sdk_password", "MYSQL_ROOT_PASSWORD": "uow_sdk_root",
			},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server"),
		},
	}},
	Setup: func(ctx context.Context, env *integrationtests.Environment) error {
		endpoint, err := env.HostEndpoint(ctx, "mysql", "3306/tcp")
		if err != nil {
			return err
		}
		testDSN = "uow_sdk:uow_sdk_password@tcp(" + endpoint + ")/uow_sdk_test?parseTime=true"
		return nil
	},
})

func TestMain(m *testing.M) { os.Exit(integrationtests.Run(m, testEnv)) }
