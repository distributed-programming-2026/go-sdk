package uow

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distributed-programming-2026/go-sdk/internal/sharedpool"
	"github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

type RepositoryProviderBuilder[RepositoryProvider any] func(client mysql.ClientContext) RepositoryProvider

type UnitOfWork interface {
	ExecuteWithClientContext(ctx context.Context, callback func(client mysql.ClientContext) error) error
}

type UnitOfWorkWithRepositoryProvider[RepositoryProvider any] interface {
	UnitOfWork
	ExecuteWithRepositoryProvider(ctx context.Context, callback func(provider RepositoryProvider) error) error
	executeWithClientAndRepositoryProvider(
		ctx context.Context,
		callback func(client mysql.ClientContext, provider RepositoryProvider) error,
	) error
}

func NewUnitOfWork[RepositoryProvider any](
	pool mysql.ConnectionPool,
	builder RepositoryProviderBuilder[RepositoryProvider],
) UnitOfWorkWithRepositoryProvider[RepositoryProvider] {
	return &unitOfWork[RepositoryProvider]{
		pool: sharedpool.NewPool(func(ctx context.Context) (_ *wrappedTransaction, err error) {
			connection, err := pool.TransactionalConnection(ctx)
			if err != nil {
				return nil, err
			}
			defer func() {
				if err != nil {
					err = errors.Join(err, connection.Close())
				}
			}()

			transaction, err := connection.BeginTransaction(ctx, nil)
			if err != nil {
				return nil, err
			}
			return &wrappedTransaction{
				Transaction:     transaction,
				lockClient:      connection,
				closeConnection: connection.Close,
			}, nil
		}),
		builder: builder,
	}
}

type unitOfWork[RepositoryProvider any] struct {
	pool    *sharedpool.Pool[context.Context, *wrappedTransaction]
	builder RepositoryProviderBuilder[RepositoryProvider]
}

func (uow *unitOfWork[RepositoryProvider]) ExecuteWithClientContext(
	ctx context.Context,
	callback func(client mysql.ClientContext) error,
) (err error) {
	sharedTransaction, err := uow.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, sharedTransaction.Close()) }()

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
			defer panic(recovered)
		}

		if err != nil {
			err = errors.Join(err, sharedTransaction.Value().Rollback())
			return
		}
		err = errors.Join(err, sharedTransaction.Value().Commit())
	}()

	err = callback(sharedTransaction.Value())
	return err
}

func (uow *unitOfWork[RepositoryProvider]) ExecuteWithRepositoryProvider(
	ctx context.Context,
	callback func(provider RepositoryProvider) error,
) error {
	return uow.executeWithClientAndRepositoryProvider(ctx, func(_ mysql.ClientContext, provider RepositoryProvider) error {
		return callback(provider)
	})
}

func (uow *unitOfWork[RepositoryProvider]) executeWithClientAndRepositoryProvider(
	ctx context.Context,
	callback func(client mysql.ClientContext, provider RepositoryProvider) error,
) error {
	return uow.ExecuteWithClientContext(ctx, func(client mysql.ClientContext) error {
		return callback(client, uow.builder(client))
	})
}

// wrappedTransaction defers the actual commit or rollback until the last
// nested unit-of-work lease is released.
type wrappedTransaction struct {
	mysql.Transaction
	rollbackOnly    atomic.Bool
	lockClient      mysql.ClientContext
	closeConnection func() error
	locksMu         sync.Mutex
	locks           []mysql.Lock
}

func (tx *wrappedTransaction) Commit() error { return nil }

func (tx *wrappedTransaction) Rollback() error {
	tx.rollbackOnly.Store(true)
	return nil
}

func (tx *wrappedTransaction) acquireLocks(ctx context.Context, locks []LockOptions) (err error) {
	orderedLocks := append([]LockOptions(nil), locks...)
	sort.SliceStable(orderedLocks, func(i, j int) bool {
		return orderedLocks[i].Name < orderedLocks[j].Name
	})

	tx.locksMu.Lock()
	defer tx.locksMu.Unlock()

	acquired := make([]mysql.Lock, 0, len(orderedLocks))
	defer func() {
		if err == nil {
			tx.locks = append(tx.locks, acquired...)
			return
		}
		for i := len(acquired) - 1; i >= 0; i-- {
			err = errors.Join(err, acquired[i].Unlock())
		}
	}()

	for _, options := range orderedLocks {
		lock := mysql.NewLock(ctx, options.Name, options.Timeout, tx.lockClient)
		if err = lock.Lock(); err != nil {
			return err
		}
		acquired = append(acquired, lock)
	}
	return nil
}

func (tx *wrappedTransaction) Close() error {
	var err error
	if tx.rollbackOnly.Load() {
		err = tx.Transaction.Rollback()
	} else {
		err = tx.Transaction.Commit()
	}

	tx.locksMu.Lock()
	for i := len(tx.locks) - 1; i >= 0; i-- {
		err = errors.Join(err, tx.locks[i].Unlock())
	}
	tx.locksMu.Unlock()

	return errors.Join(err, tx.closeConnection())
}

type LockOptions struct {
	Name    string
	Timeout time.Duration
}
