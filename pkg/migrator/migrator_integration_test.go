package migrator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkmysql "github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

var tableSequence atomic.Uint64

func TestMigratorIntegration(t *testing.T) {
	client := newIntegrationClient(t)
	tableName := fmt.Sprintf("migrations_%d", tableSequence.Add(1))
	migrator, err := New(client, slog.New(slog.NewTextHandler(io.Discard, nil)), WithTableName(tableName))
	require.NoError(t, err)

	var applicationOrder []int64
	applicationMigrations := []Migration{
		testMigration{version: 3, description: "third", up: appendVersion(&applicationOrder, 3)},
		testMigration{version: 1, description: "first", up: appendVersion(&applicationOrder, 1)},
		testMigration{version: 2, description: "second", up: appendVersion(&applicationOrder, 2)},
	}
	require.NoError(t, migrator.Migrate(t.Context(), "application", applicationMigrations...))
	assert.Equal(t, []int64{1, 2, 3}, applicationOrder)

	// The same versions are independent in another target.
	var mysqlOrder []int64
	require.NoError(t, migrator.Migrate(t.Context(), "mysql",
		testMigration{version: 1, description: "mysql first", up: appendVersion(&mysqlOrder, 1)},
		testMigration{version: 2, description: "mysql second", up: appendVersion(&mysqlOrder, 2)},
	))
	assert.Equal(t, []int64{1, 2}, mysqlOrder)

	// Re-running a target does not call Up again.
	require.NoError(t, migrator.Migrate(t.Context(), "application", applicationMigrations...))
	assert.Equal(t, []int64{1, 2, 3}, applicationOrder)

	assertHistory(t, client, tableName, "application", []historyRow{
		{1, "first"}, {2, "second"}, {3, "third"},
	})
	assertHistory(t, client, tableName, "mysql", []historyRow{
		{1, "mysql first"}, {2, "mysql second"},
	})
}

func TestMigratorIntegrationRejectsLateMigration(t *testing.T) {
	client := newIntegrationClient(t)
	tableName := fmt.Sprintf("migrations_%d", tableSequence.Add(1))
	migrator, err := New(client, slog.Default(), WithTableName(tableName))
	require.NoError(t, err)
	require.NoError(t, migrator.Migrate(t.Context(), "application", testMigration{version: 2}))

	err = migrator.Migrate(t.Context(), "application", testMigration{version: 1})

	assert.ErrorIs(t, err, ErrOutOfOrder)
	assertHistory(t, client, tableName, "application", []historyRow{{2, ""}})
}

func TestMigratorIntegrationReleasesLockAfterPanic(t *testing.T) {
	client := newIntegrationClient(t)
	tableName := fmt.Sprintf("migrations_%d", tableSequence.Add(1))
	migrator, err := New(client, slog.Default(),
		WithTableName(tableName), WithLockTimeout(0))
	require.NoError(t, err)
	panicValue := &struct{ value string }{"migration panic"}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = migrator.Migrate(t.Context(), "application", testMigration{version: 1, up: func(context.Context) error {
			panic(panicValue)
		}})
	}()
	require.Same(t, panicValue, recovered)

	// A second run can acquire the same advisory lock immediately.
	applied := false
	require.NoError(t, migrator.Migrate(t.Context(), "application", testMigration{version: 1, up: func(context.Context) error {
		applied = true
		return nil
	}}))
	assert.True(t, applied)
}

func appendVersion(versions *[]int64, version int64) func(context.Context) error {
	return func(context.Context) error {
		*versions = append(*versions, version)
		return nil
	}
}

type historyRow struct {
	Version     int64  `db:"version"`
	Description string `db:"description"`
}

func assertHistory(t *testing.T, client sdkmysql.ClientContext, tableName, target string, expected []historyRow) {
	t.Helper()
	var actual []historyRow
	query := "SELECT version, description FROM `" + tableName + "` WHERE target = ? ORDER BY version"
	require.NoError(t, client.SelectContext(t.Context(), "test.migration-history", &actual, query, target))
	assert.Equal(t, expected, actual)

	var missingTimestamps int
	query = "SELECT COUNT(*) FROM `" + tableName + "` WHERE target = ? AND applied_at IS NULL"
	require.NoError(t, client.GetContext(t.Context(), "test.migration-timestamps", &missingTimestamps, query, target))
	assert.Zero(t, missingTimestamps)
}

func newIntegrationClient(t *testing.T) sdkmysql.TransactionalClient {
	t.Helper()
	connector := sdkmysql.NewConnector()
	require.NoError(t, connector.Open(testDSN, sdkmysql.Config{
		MaxConnections: 8, ConnectionMaxLifeTime: time.Minute, ConnectionMaxIdleTime: time.Minute,
	}))
	t.Cleanup(func() { assert.NoError(t, connector.Close()) })
	return connector.TransactionalClient()
}
