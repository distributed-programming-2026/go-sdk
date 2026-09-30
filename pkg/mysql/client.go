package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type ClientContext interface {
	QueryContext(ctx context.Context, queryID string, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, queryID string, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, queryID string, query string, args ...any) (sql.Result, error)
	SelectContext(ctx context.Context, queryID string, dest any, query string, args ...any) error
	GetContext(ctx context.Context, queryID string, dest any, query string, args ...any) error
}

type Transaction interface {
	ClientContext
	Commit() error
	Rollback() error
}

type TransactionalConnection interface {
	ClientContext
	BeginTransaction(ctx context.Context, opts *sql.TxOptions) (Transaction, error)
	Close() error
}

type TransactionalClient interface {
	ClientContext
	BeginTransaction() (Transaction, error)
	Connection(ctx context.Context) (TransactionalConnection, error)
}

func NewTransactionalClientFromSQLx(db *sqlx.DB) TransactionalClient {
	return &transactionalClient{clientContext: clientContext{delegate: db}, db: db}
}

type sqlxClientContext interface {
	sqlx.QueryerContext
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type clientContext struct {
	delegate sqlxClientContext
}

func (client clientContext) QueryContext(ctx context.Context, queryID, query string, args ...any) (*sql.Rows, error) {
	rows, err := client.delegate.QueryContext(ctx, query, args...)
	return rows, wrapQueryError(queryID, err)
}

func (client clientContext) QueryRowContext(ctx context.Context, _, query string, args ...any) *sql.Row {
	return client.delegate.QueryRowContext(ctx, query, args...)
}

func (client clientContext) ExecContext(ctx context.Context, queryID, query string, args ...any) (sql.Result, error) {
	result, err := client.delegate.ExecContext(ctx, query, args...)
	return result, wrapQueryError(queryID, err)
}

func (client clientContext) SelectContext(ctx context.Context, queryID string, dest any, query string, args ...any) error {
	return wrapQueryError(queryID, sqlx.SelectContext(ctx, client.delegate, dest, query, args...))
}

func (client clientContext) GetContext(ctx context.Context, queryID string, dest any, query string, args ...any) error {
	return wrapQueryError(queryID, sqlx.GetContext(ctx, client.delegate, dest, query, args...))
}

func wrapQueryError(queryID string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("query %q: %w", queryID, err)
}

type transactionalClient struct {
	clientContext
	db *sqlx.DB
}

func (client *transactionalClient) BeginTransaction() (Transaction, error) {
	tx, err := client.db.Beginx()
	if err != nil {
		return nil, err
	}
	return &transaction{clientContext: clientContext{delegate: tx}, tx: tx}, nil
}

func (client *transactionalClient) Connection(ctx context.Context) (TransactionalConnection, error) {
	conn, err := client.db.Connx(ctx)
	if err != nil {
		return nil, err
	}
	return &transactionalConnection{clientContext: clientContext{delegate: conn}, conn: conn}, nil
}

type transaction struct {
	clientContext
	tx *sqlx.Tx
}

func (tx *transaction) Commit() error   { return tx.tx.Commit() }
func (tx *transaction) Rollback() error { return tx.tx.Rollback() }

type transactionalConnection struct {
	clientContext
	conn *sqlx.Conn
}

func (conn *transactionalConnection) BeginTransaction(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	tx, err := conn.conn.BeginTxx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &transaction{clientContext: clientContext{delegate: tx}, tx: tx}, nil
}

func (conn *transactionalConnection) Close() error { return conn.conn.Close() }
