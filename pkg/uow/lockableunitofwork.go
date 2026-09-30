package uow

import (
	"context"
	"errors"

	"github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

var errUnsupportedUnitOfWork = errors.New("lockable unit of work requires the default unit of work implementation")

type LockableUnitOfWork interface {
	ExecuteWithClientContext(
		ctx context.Context,
		locks []LockOptions,
		callback func(client mysql.ClientContext) error,
	) error
}

type LockableUnitOfWorkWithRepositoryProvider[RepositoryProvider any] interface {
	LockableUnitOfWork
	ExecuteWithRepositoryProvider(
		ctx context.Context,
		locks []LockOptions,
		callback func(provider RepositoryProvider) error,
	) error
}

func NewLockableUnitOfWork[RepositoryProvider any](
	unitOfWork UnitOfWorkWithRepositoryProvider[RepositoryProvider],
) LockableUnitOfWorkWithRepositoryProvider[RepositoryProvider] {
	return &lockableUnitOfWork[RepositoryProvider]{unitOfWork: unitOfWork}
}

type lockableUnitOfWork[RepositoryProvider any] struct {
	unitOfWork UnitOfWorkWithRepositoryProvider[RepositoryProvider]
}

func (uow *lockableUnitOfWork[RepositoryProvider]) ExecuteWithClientContext(
	ctx context.Context,
	locks []LockOptions,
	callback func(client mysql.ClientContext) error,
) error {
	return uow.unitOfWork.ExecuteWithClientContext(ctx, func(client mysql.ClientContext) error {
		transaction, ok := client.(*wrappedTransaction)
		if !ok {
			return errUnsupportedUnitOfWork
		}
		if err := transaction.acquireLocks(ctx, locks); err != nil {
			return err
		}
		return callback(client)
	})
}

func (uow *lockableUnitOfWork[RepositoryProvider]) ExecuteWithRepositoryProvider(
	ctx context.Context,
	locks []LockOptions,
	callback func(provider RepositoryProvider) error,
) error {
	return uow.unitOfWork.executeWithClientAndRepositoryProvider(ctx, func(
		client mysql.ClientContext,
		provider RepositoryProvider,
	) error {
		transaction, ok := client.(*wrappedTransaction)
		if !ok {
			return errUnsupportedUnitOfWork
		}
		if err := transaction.acquireLocks(ctx, locks); err != nil {
			return err
		}
		return callback(provider)
	})
}
