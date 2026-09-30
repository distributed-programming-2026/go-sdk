package uow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sdkmysql "github.com/distributed-programming-2026/go-sdk/pkg/mysql"
	"github.com/distributed-programming-2026/go-sdk/pkg/uow"
)

type contextKey struct{}

func TestLockableUnitOfWorkIntegration(t *testing.T) {
	client := newTestClient(t)
	pool := sdkmysql.NewConnectionPool(client)
	work := uow.NewUnitOfWork(pool, func(client sdkmysql.ClientContext) sdkmysql.ClientContext {
		return client
	})
	lockableWork := uow.NewLockableUnitOfWork(work)
	ctx := context.WithValue(t.Context(), contextKey{}, "shared-transaction")

	contenderConnection, err := client.Connection(ctx)
	if err != nil {
		t.Fatalf("open contender connection: %v", err)
	}
	defer func() {
		if err := contenderConnection.Close(); err != nil {
			t.Errorf("close contender connection: %v", err)
		}
	}()

	locks := []uow.LockOptions{
		{Name: "uow-integration-second", Timeout: 0},
		{Name: "uow-integration-first", Timeout: 0},
	}
	firstContender := sdkmysql.NewLock(ctx, locks[0].Name, 0, contenderConnection)
	secondContender := sdkmysql.NewLock(ctx, locks[1].Name, 0, contenderConnection)

	err = work.ExecuteWithClientContext(ctx, func(sdkmysql.ClientContext) error {
		if err := lockableWork.ExecuteWithClientContext(ctx, locks, func(sdkmysql.ClientContext) error {
			assertLockIsHeld(t, firstContender)
			assertLockIsHeld(t, secondContender)
			return nil
		}); err != nil {
			return err
		}

		// The nested lockable UoW has returned, but its shared outer transaction
		// is still open, so both locks must remain held.
		assertLockIsHeld(t, firstContender)
		assertLockIsHeld(t, secondContender)
		return nil
	})
	if err != nil {
		t.Fatalf("execute nested unit of work: %v", err)
	}

	assertLockCanBeAcquired(t, firstContender)
	assertLockCanBeAcquired(t, secondContender)
}

func TestNestedLockableUnitsOfWorkKeepAllLocksUntilTransactionCloses(t *testing.T) {
	client := newTestClient(t)
	pool := sdkmysql.NewConnectionPool(client)
	work := uow.NewUnitOfWork(pool, func(client sdkmysql.ClientContext) sdkmysql.ClientContext {
		return client
	})
	lockableWork := uow.NewLockableUnitOfWork(work)
	ctx := context.WithValue(t.Context(), contextKey{}, "nested-lockable-transaction")

	contenderConnection, err := client.Connection(ctx)
	if err != nil {
		t.Fatalf("open contender connection: %v", err)
	}
	defer func() {
		if err := contenderConnection.Close(); err != nil {
			t.Errorf("close contender connection: %v", err)
		}
	}()

	outerOptions := []uow.LockOptions{{Name: "nested-uow-outer-lock", Timeout: 0}}
	innerOptions := []uow.LockOptions{{Name: "nested-uow-inner-lock", Timeout: 0}}
	outerContender := sdkmysql.NewLock(ctx, outerOptions[0].Name, 0, contenderConnection)
	innerContender := sdkmysql.NewLock(ctx, innerOptions[0].Name, 0, contenderConnection)

	err = lockableWork.ExecuteWithClientContext(ctx, outerOptions, func(sdkmysql.ClientContext) error {
		assertLockIsHeld(t, outerContender)

		if err := lockableWork.ExecuteWithClientContext(ctx, innerOptions, func(sdkmysql.ClientContext) error {
			assertLockIsHeld(t, outerContender)
			assertLockIsHeld(t, innerContender)
			return nil
		}); err != nil {
			return err
		}

		assertLockIsHeld(t, outerContender)
		assertLockIsHeld(t, innerContender)
		return nil
	})
	if err != nil {
		t.Fatalf("execute nested lockable units of work: %v", err)
	}

	assertLockCanBeAcquired(t, outerContender)
	assertLockCanBeAcquired(t, innerContender)
}

func newTestClient(t *testing.T) sdkmysql.TransactionalClient {
	t.Helper()

	connector := sdkmysql.NewConnector()
	if err := connector.Open(testDSN, sdkmysql.Config{
		MaxConnections:        8,
		ConnectionMaxLifeTime: time.Minute,
		ConnectionMaxIdleTime: time.Minute,
	}); err != nil {
		t.Fatalf("open MySQL connector: %v", err)
	}
	t.Cleanup(func() {
		if err := connector.Close(); err != nil {
			t.Errorf("close MySQL connector: %v", err)
		}
	})
	return connector.TransactionalClient()
}

func assertLockIsHeld(t *testing.T, lock sdkmysql.Lock) {
	t.Helper()
	if err := lock.Lock(); !errors.Is(err, sdkmysql.ErrLockTimeout) {
		t.Fatalf("contender Lock() error = %v, want %v", err, sdkmysql.ErrLockTimeout)
	}
}

func assertLockCanBeAcquired(t *testing.T, lock sdkmysql.Lock) {
	t.Helper()
	if err := lock.Lock(); err != nil {
		t.Fatalf("contender Lock() error = %v", err)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatalf("contender Unlock() error = %v", err)
	}
}
