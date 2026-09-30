package uow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

type fakeTransaction struct {
	mysql.ClientContext
	commitErr   error
	rollbackErr error
	commits     int
	rollbacks   int
	events      *[]string
}

func (tx *fakeTransaction) Commit() error {
	tx.commits++
	tx.recordEvent("commit")
	return tx.commitErr
}

func (tx *fakeTransaction) Rollback() error {
	tx.rollbacks++
	tx.recordEvent("rollback")
	return tx.rollbackErr
}

func (tx *fakeTransaction) recordEvent(event string) {
	if tx.events != nil {
		*tx.events = append(*tx.events, event)
	}
}

type fakeConnection struct {
	mysql.ClientContext
	tx       *fakeTransaction
	beginErr error
	closeErr error
	begins   int
	closes   int
	events   *[]string
}

func (connection *fakeConnection) BeginTransaction(context.Context, *sql.TxOptions) (mysql.Transaction, error) {
	connection.begins++
	if connection.beginErr != nil {
		return nil, connection.beginErr
	}
	return connection.tx, nil
}

func (connection *fakeConnection) Close() error {
	connection.closes++
	connection.recordEvent("close")
	return connection.closeErr
}

func (connection *fakeConnection) GetContext(
	_ context.Context,
	queryID string,
	dest any,
	_ string,
	args ...any,
) error {
	result, ok := dest.(*sql.NullInt32)
	if !ok {
		return fmt.Errorf("unexpected destination type %T", dest)
	}
	result.Valid = true
	result.Int32 = 1
	if len(args) == 0 {
		return errors.New("lock name argument is missing")
	}
	connection.recordEvent(fmt.Sprintf("%s:%v", queryID, args[0]))
	return nil
}

func (connection *fakeConnection) recordEvent(event string) {
	if connection.events != nil {
		*connection.events = append(*connection.events, event)
	}
}

type fakeConnectionPool struct {
	connection *fakeConnection
	err        error
	gets       int
}

func (pool *fakeConnectionPool) TransactionalConnection(context.Context) (mysql.TransactionalConnection, error) {
	pool.gets++
	if pool.err != nil {
		return nil, pool.err
	}
	return pool.connection, nil
}

func newTestUnitOfWork() (UnitOfWorkWithRepositoryProvider[mysql.ClientContext], *fakeConnectionPool, *fakeConnection, *fakeTransaction) {
	tx := &fakeTransaction{}
	connection := &fakeConnection{tx: tx}
	pool := &fakeConnectionPool{connection: connection}
	work := NewUnitOfWork(pool, func(client mysql.ClientContext) mysql.ClientContext { return client })
	return work, pool, connection, tx
}

func TestUnitOfWorkCommitsAndClosesConnection(t *testing.T) {
	work, pool, connection, tx := newTestUnitOfWork()

	err := work.ExecuteWithClientContext(context.Background(), func(client mysql.ClientContext) error {
		if client == nil {
			t.Fatal("callback received a nil client")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("ExecuteWithClientContext() error = %v", err)
	}
	assertCounts(t, pool, connection, tx, 1, 1, 1, 0)
}

func TestUnitOfWorkRollsBackOnCallbackError(t *testing.T) {
	work, pool, connection, tx := newTestUnitOfWork()
	wantErr := errors.New("callback failed")

	err := work.ExecuteWithClientContext(context.Background(), func(mysql.ClientContext) error {
		return wantErr
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("ExecuteWithClientContext() error = %v, want errors.Is(_, %v)", err, wantErr)
	}
	assertCounts(t, pool, connection, tx, 1, 1, 0, 1)
}

func TestNestedUnitOfWorkSharesTransactionAndStaysRollbackOnly(t *testing.T) {
	work, pool, connection, tx := newTestUnitOfWork()
	ctx := context.Background()
	innerErr := errors.New("inner failed")

	err := work.ExecuteWithClientContext(ctx, func(outerClient mysql.ClientContext) error {
		got := work.ExecuteWithClientContext(ctx, func(innerClient mysql.ClientContext) error {
			if innerClient != outerClient {
				t.Error("nested unit of work did not share its transaction")
			}
			return innerErr
		})
		if !errors.Is(got, innerErr) {
			t.Fatalf("nested error = %v, want errors.Is(_, %v)", got, innerErr)
		}
		return nil // Even a swallowed nested error must force the final rollback.
	})

	if err != nil {
		t.Fatalf("outer ExecuteWithClientContext() error = %v", err)
	}
	assertCounts(t, pool, connection, tx, 1, 1, 0, 1)
}

func TestUnitOfWorkRollsBackAndRepanics(t *testing.T) {
	work, pool, connection, tx := newTestUnitOfWork()
	panicValue := struct{ message string }{"boom"}

	func() {
		defer func() {
			if got := recover(); got != panicValue {
				t.Fatalf("recovered %v, want %v", got, panicValue)
			}
		}()
		_ = work.ExecuteWithClientContext(context.Background(), func(mysql.ClientContext) error {
			panic(panicValue)
		})
	}()

	assertCounts(t, pool, connection, tx, 1, 1, 0, 1)
}

func TestUnitOfWorkJoinsFinalizationErrors(t *testing.T) {
	work, _, connection, tx := newTestUnitOfWork()
	commitErr := errors.New("commit failed")
	closeErr := errors.New("close failed")
	tx.commitErr = commitErr
	connection.closeErr = closeErr

	err := work.ExecuteWithClientContext(context.Background(), func(mysql.ClientContext) error { return nil })

	if !errors.Is(err, commitErr) || !errors.Is(err, closeErr) {
		t.Fatalf("error = %v, want joined commit and close errors", err)
	}
}

func TestUnitOfWorkClosesConnectionWhenBeginFails(t *testing.T) {
	work, pool, connection, tx := newTestUnitOfWork()
	beginErr := errors.New("begin failed")
	closeErr := errors.New("close failed")
	connection.beginErr = beginErr
	connection.closeErr = closeErr

	err := work.ExecuteWithClientContext(context.Background(), func(mysql.ClientContext) error {
		t.Fatal("callback must not be called")
		return nil
	})

	if !errors.Is(err, beginErr) || !errors.Is(err, closeErr) {
		t.Fatalf("error = %v, want joined begin and close errors", err)
	}
	assertCounts(t, pool, connection, tx, 1, 1, 0, 0)
}

func TestUnitOfWorkBuildsRepositoryProviderFromTransaction(t *testing.T) {
	tx := &fakeTransaction{}
	connection := &fakeConnection{tx: tx}
	pool := &fakeConnectionPool{connection: connection}
	type provider struct{ client mysql.ClientContext }
	work := NewUnitOfWork(pool, func(client mysql.ClientContext) provider { return provider{client: client} })

	err := work.ExecuteWithRepositoryProvider(context.Background(), func(got provider) error {
		if got.client == nil {
			t.Fatal("provider was not built with the transaction")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteWithRepositoryProvider() error = %v", err)
	}
}

func TestNestedLockableUnitOfWorkReleasesMultipleLocksAfterCommit(t *testing.T) {
	events := make([]string, 0, 6)
	tx := &fakeTransaction{events: &events}
	connection := &fakeConnection{tx: tx, events: &events}
	pool := &fakeConnectionPool{connection: connection}
	work := NewUnitOfWork(pool, func(client mysql.ClientContext) mysql.ClientContext { return client })
	lockableWork := NewLockableUnitOfWork(work)
	ctx := context.Background()

	err := work.ExecuteWithClientContext(ctx, func(mysql.ClientContext) error {
		err := lockableWork.ExecuteWithClientContext(ctx, []LockOptions{
			{Name: "second"},
			{Name: "first"},
		}, func(mysql.ClientContext) error {
			return nil
		})
		if err != nil {
			return err
		}
		if len(events) != 2 {
			t.Fatalf("events before outer transaction closes = %v, want only two lock acquisitions", events)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("execute nested lockable unit of work: %v", err)
	}

	want := []string{
		"mysql.lock.acquire:first",
		"mysql.lock.acquire:second",
		"commit",
		"mysql.lock.release:second",
		"mysql.lock.release:first",
		"close",
	}
	if fmt.Sprint(events) != fmt.Sprint(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func assertCounts(
	t *testing.T,
	pool *fakeConnectionPool,
	connection *fakeConnection,
	tx *fakeTransaction,
	wantGets, wantCloses, wantCommits, wantRollbacks int,
) {
	t.Helper()
	if pool.gets != wantGets || connection.closes != wantCloses || tx.commits != wantCommits || tx.rollbacks != wantRollbacks {
		t.Fatalf(
			"counts = gets:%d closes:%d commits:%d rollbacks:%d; want %d, %d, %d, %d",
			pool.gets, connection.closes, tx.commits, tx.rollbacks,
			wantGets, wantCloses, wantCommits, wantRollbacks,
		)
	}
}
