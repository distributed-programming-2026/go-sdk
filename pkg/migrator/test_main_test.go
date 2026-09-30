package migrator

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

const mysqlImage = "mysql:8.4"

var testDSN string

func TestMain(m *testing.M) {
	ctx := context.Background()
	container, err := tcmysql.Run(ctx, mysqlImage,
		tcmysql.WithDatabase("migrator_test"),
		tcmysql.WithUsername("migrator"),
		tcmysql.WithPassword("migrator_password"),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start MySQL test container: %v\n", err)
		os.Exit(1)
	}

	testDSN, err = container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		fmt.Fprintf(os.Stderr, "get MySQL test container connection string: %v\n", err)
		os.Exit(1)
	}

	exitCode := m.Run()
	if err := testcontainers.TerminateContainer(container); err != nil {
		fmt.Fprintf(os.Stderr, "terminate MySQL test container: %v\n", err)
		if exitCode == 0 {
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}
