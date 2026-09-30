package mysql

import (
	"context"
	"errors"
	"time"
)

type Locker interface {
	ExecuteWithLock(ctx context.Context, lockName string, lockTimeout time.Duration, callback func() error) error
}

func NewLocker(pool ConnectionPool) Locker {
	return &locker{pool: pool}
}

type locker struct {
	pool ConnectionPool
}

func (l locker) ExecuteWithLock(ctx context.Context, lockName string, lockTimeout time.Duration, callback func() error) (err error) {
	defer func() {
		if err != nil {
			err = wrapLockError(lockName, err)
		}
	}()

	connection, err := l.pool.TransactionalConnection(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, connection.Close()) }()

	lock := NewLock(ctx, lockName, lockTimeout, connection)
	if err = lock.Lock(); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Unlock()) }()

	return callback()
}
