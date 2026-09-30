package mysql

import (
	"context"

	"github.com/distributed-programming-2026/go-sdk/internal/sharedpool"
)

type ConnectionPool interface {
	TransactionalConnection(ctx context.Context) (TransactionalConnection, error)
}

func NewConnectionPool(client TransactionalClient) ConnectionPool {
	return &connectionPool{pool: sharedpool.NewPool(client.Connection)}
}

type connectionPool struct {
	pool *sharedpool.Pool[context.Context, TransactionalConnection]
}

func (p *connectionPool) TransactionalConnection(ctx context.Context) (TransactionalConnection, error) {
	sharedConnection, err := p.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	return &wrappedTransactionalConnection{
		TransactionalConnection: sharedConnection.Value(),
		release:                 sharedConnection.Close,
	}, nil
}

type wrappedTransactionalConnection struct {
	TransactionalConnection
	release func() error
}

func (conn *wrappedTransactionalConnection) Close() error { return conn.release() }
