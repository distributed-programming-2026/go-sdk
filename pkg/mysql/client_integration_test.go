package mysql_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkmysql "github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

func TestClientIntegration(t *testing.T) {
	ctx := t.Context()
	client := newTestClient(t)
	require.NoError(t, createTestTable(ctx, client))

	t.Run("executes and reads queries", func(t *testing.T) {
		_, err := client.ExecContext(ctx, "test.insert-value", "INSERT INTO sdk_values(value) VALUES (?)", "client")
		require.NoError(t, err)

		var value string
		require.NoError(t, client.GetContext(ctx, "test.get-value", &value, "SELECT value FROM sdk_values WHERE value = ?", "client"))
		assert.Equal(t, "client", value)
	})

	t.Run("transaction commits and rolls back", func(t *testing.T) {
		committed, err := client.BeginTransaction()
		require.NoError(t, err)
		_, err = committed.ExecContext(ctx, "test.insert-committed-value", "INSERT INTO sdk_values(value) VALUES (?)", "committed")
		require.NoError(t, err)
		require.NoError(t, committed.Commit())
		assert.Equal(t, 1, countValues(ctx, t, client, "committed"))

		rolledBack, err := client.BeginTransaction()
		require.NoError(t, err)
		_, err = rolledBack.ExecContext(ctx, "test.insert-rolled-back-value", "INSERT INTO sdk_values(value) VALUES (?)", "rolled-back")
		require.NoError(t, err)
		require.NoError(t, rolledBack.Rollback())
		assert.Equal(t, 0, countValues(ctx, t, client, "rolled-back"))
	})

	t.Run("error contains query identifier", func(t *testing.T) {
		_, err := client.ExecContext(ctx, "test.invalid-query", "INVALID SQL")
		require.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), `query "test.invalid-query"`))
	})
}

func createTestTable(ctx context.Context, client sdkmysql.ClientContext) error {
	_, err := client.ExecContext(ctx, "test.create-values-table", `
		CREATE TABLE sdk_values (
			id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			value VARCHAR(255) NOT NULL
		)
	`)
	return err
}

func countValues(ctx context.Context, t *testing.T, client sdkmysql.ClientContext, value string) int {
	t.Helper()

	var count int
	require.NoError(t, client.GetContext(ctx, "test.count-values", &count, "SELECT COUNT(*) FROM sdk_values WHERE value = ?", value))
	return count
}
