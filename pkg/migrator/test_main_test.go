package migrator

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
				"MYSQL_DATABASE": "migrator_test", "MYSQL_USER": "migrator", "MYSQL_PASSWORD": "migrator_password", "MYSQL_ROOT_PASSWORD": "migrator_root",
			},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server"),
		},
	}},
	Setup: func(ctx context.Context, env *integrationtests.Environment) error {
		endpoint, err := env.HostEndpoint(ctx, "mysql", "3306/tcp")
		if err != nil {
			return err
		}
		testDSN = "migrator:migrator_password@tcp(" + endpoint + ")/migrator_test?parseTime=true"
		return nil
	},
})

func TestMain(m *testing.M) { os.Exit(integrationtests.Run(m, testEnv)) }
