package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkmysql "github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

type contextKey struct{}

func newTestClient(t *testing.T) sdkmysql.TransactionalClient {
	t.Helper()

	connector := sdkmysql.NewConnector()
	require.NoError(t, connector.Open(testDSN, sdkmysql.Config{
		MaxConnections:        8,
		ConnectionMaxLifeTime: time.Minute,
		ConnectionMaxIdleTime: time.Minute,
	}))
	t.Cleanup(func() {
		assert.NoError(t, connector.Close())
	})

	return connector.TransactionalClient()
}

func connectionID(ctx context.Context, t *testing.T, client sdkmysql.ClientContext) int64 {
	t.Helper()

	var id int64
	require.NoError(t, client.GetContext(ctx, "test.connection-id", &id, "SELECT CONNECTION_ID()"))
	return id
}
