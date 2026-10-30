package mysql_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"uuid"

	sdkmysql "github.com/distributed-programming-2026/go-sdk/pkg/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientIntegration(t *testing.T) {
	ctx := testEnv.Bind(t)
	client := newTestClient(t)
	require.NoError(t, createTestTable(ctx, client))
	require.NoError(t, createUUIDTestTable(ctx, client))

	t.Run("executes and reads queries", func(t *testing.T) {
		_, err := client.ExecContext(
			ctx,
			"test.insert-value",
			"INSERT INTO sdk_values(value) VALUES (?)",
			"client",
		)
		require.NoError(t, err)

		var value string
		require.NoError(
			t,
			client.GetContext(
				ctx,
				"test.get-value",
				&value,
				"SELECT value FROM sdk_values WHERE value = ?",
				"client",
			),
		)
		assert.Equal(t, "client", value)
	})

	t.Run("transaction commits and rolls back", func(t *testing.T) {
		committed, err := client.BeginTransaction()
		require.NoError(t, err)
		_, err = committed.ExecContext(
			ctx,
			"test.insert-committed-value",
			"INSERT INTO sdk_values(value) VALUES (?)",
			"committed",
		)
		require.NoError(t, err)
		require.NoError(t, committed.Commit())
		assert.Equal(t, 1, countValues(ctx, t, client, "committed"))

		rolledBack, err := client.BeginTransaction()
		require.NoError(t, err)
		_, err = rolledBack.ExecContext(
			ctx,
			"test.insert-rolled-back-value",
			"INSERT INTO sdk_values(value) VALUES (?)",
			"rolled-back",
		)
		require.NoError(t, err)
		require.NoError(t, rolledBack.Rollback())
		assert.Equal(t, 0, countValues(ctx, t, client, "rolled-back"))
	})

	t.Run("error contains query identifier", func(t *testing.T) {
		_, err := client.ExecContext(ctx, "test.invalid-query", "INVALID SQL")
		require.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), `query "test.invalid-query"`))
	})

	t.Run("stores and reads binary UUIDs", func(t *testing.T) {
		id := uuid.MustParse("0199c801-8a6c-7476-b79f-3b9d6f60af7e")
		optionalID := uuid.MustParse("0199c801-8a6c-7476-b79f-3b9d6f60af7f")
		_, err := client.ExecContext(
			ctx,
			"test.insert-uuid",
			"INSERT INTO sdk_uuid_values(id, optional_id) VALUES (?, ?), (?, ?)",
			id,
			sql.Null[uuid.UUID]{V: optionalID, Valid: true},
			uuid.New(),
			sql.Null[uuid.UUID]{},
		)
		require.NoError(t, err)

		var row struct {
			ID         uuid.UUID           `db:"id"`
			OptionalID sql.Null[uuid.UUID] `db:"optional_id"`
			HexID      string              `db:"hex_id"`
		}
		require.NoError(t, client.GetContext(
			ctx,
			"test.get-uuid",
			&row,
			"SELECT id, optional_id, HEX(id) AS hex_id FROM sdk_uuid_values WHERE id = ?",
			&id,
		))
		assert.Equal(t, id, row.ID)
		assert.Equal(t, sql.Null[uuid.UUID]{V: optionalID, Valid: true}, row.OptionalID)
		assert.Equal(t, "0199C8018A6C7476B79F3B9D6F60AF7E", row.HexID)

		var ids []uuid.UUID
		require.NoError(t, client.SelectContext(
			ctx,
			"test.select-uuid",
			&ids,
			"SELECT id FROM sdk_uuid_values WHERE id = ?",
			id,
		))
		assert.Equal(t, []uuid.UUID{id}, ids)

		var queried uuid.UUID
		rows, err := client.QueryContext(
			ctx,
			"test.query-uuid",
			"SELECT id FROM sdk_uuid_values WHERE id = ?",
			id)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, rows.Close()) })
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&queried))
		require.NoError(t, rows.Err())
		assert.Equal(t, id, queried)

		var queryRowID uuid.UUID
		require.NoError(t, client.QueryRowContext(
			ctx,
			"test.query-row-uuid",
			"SELECT id FROM sdk_uuid_values WHERE id = ?",
			id,
		).Scan(&queryRowID))
		assert.Equal(t, id, queryRowID)

		var nullOptionalID sql.Null[uuid.UUID]
		require.NoError(t, client.GetContext(
			ctx,
			"test.get-null-uuid",
			&nullOptionalID,
			"SELECT optional_id FROM sdk_uuid_values WHERE optional_id IS NULL",
		))
		assert.False(t, nullOptionalID.Valid)
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

func createUUIDTestTable(ctx context.Context, client sdkmysql.ClientContext) error {
	_, err := client.ExecContext(ctx, "test.create-uuid-values-table", `
		CREATE TABLE sdk_uuid_values (
			id BINARY(16) NOT NULL PRIMARY KEY,
			optional_id BINARY(16) NULL
		)
	`)
	return err
}

func countValues(ctx context.Context, t *testing.T, client sdkmysql.ClientContext, value string) int {
	t.Helper()

	var count int
	require.NoError(
		t,
		client.GetContext(
			ctx,
			"test.count-values",
			&count,
			"SELECT COUNT(*) FROM sdk_values WHERE value = ?",
			value,
		),
	)
	return count
}
