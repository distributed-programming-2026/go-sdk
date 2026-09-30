package mysql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkmysql "github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

func TestConnectionPoolIntegration(t *testing.T) {
	ctx := context.WithValue(t.Context(), contextKey{}, "shared-connection")
	pool := sdkmysql.NewConnectionPool(newTestClient(t))

	outer, err := pool.TransactionalConnection(ctx)
	require.NoError(t, err)

	inner, err := pool.TransactionalConnection(ctx)
	require.NoError(t, err)

	outerID := connectionID(ctx, t, outer)
	assert.Equal(t, outerID, connectionID(ctx, t, inner))
	require.NoError(t, inner.Close())
	assert.Equal(t, outerID, connectionID(ctx, t, outer))
	require.NoError(t, outer.Close())
}
