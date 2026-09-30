package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var (
	ErrLockTimeout   = errors.New("lock timed out")
	ErrLockNotLocked = errors.New("lock not locked")
	ErrLockNotFound  = errors.New("lock not found")
)

type Lock interface {
	Lock() error
	Unlock() error
}

func NewLock(ctx context.Context, lockName string, timeout time.Duration, client ClientContext) Lock {
	return &lock{ctx: ctx, lockName: lockName, timeout: timeout, client: client}
}

type lock struct {
	ctx      context.Context
	lockName string
	timeout  time.Duration
	client   ClientContext
}

func (l lock) Lock() error {
	const queryID = "mysql.lock.acquire"
	const query = "SELECT GET_LOCK(SUBSTRING(CONCAT(?, '.', DATABASE()), 1, 64), ?)"
	var result sql.NullInt32
	err := l.client.GetContext(l.ctx, queryID, &result, query, l.lockName, int(l.timeout.Seconds()))
	if err != nil {
		return wrapLockError(l.lockName, err)
	}
	if err == nil && result.Valid && result.Int32 == 0 {
		return wrapLockError(l.lockName, wrapQueryError(queryID, ErrLockTimeout))
	}
	return nil
}

func (l lock) Unlock() error {
	const queryID = "mysql.lock.release"
	const query = "SELECT RELEASE_LOCK(SUBSTRING(CONCAT(?, '.', DATABASE()), 1, 64))"
	var result sql.NullInt32
	if err := l.client.GetContext(l.ctx, queryID, &result, query, l.lockName); err != nil {
		return wrapLockError(l.lockName, err)
	}
	if !result.Valid {
		return wrapLockError(l.lockName, wrapQueryError(queryID, ErrLockNotFound))
	}
	if result.Int32 == 0 {
		return wrapLockError(l.lockName, wrapQueryError(queryID, ErrLockNotLocked))
	}
	return nil
}

type lockError struct {
	lockName string
	err      error
}

func (e *lockError) Error() string { return fmt.Sprintf("lock %q: %v", e.lockName, e.err) }
func (e *lockError) Unwrap() error { return e.err }

func wrapLockError(lockName string, err error) error {
	if err == nil {
		return nil
	}

	var existing *lockError
	if errors.As(err, &existing) && existing.lockName == lockName {
		return err
	}
	return &lockError{lockName: lockName, err: err}
}
