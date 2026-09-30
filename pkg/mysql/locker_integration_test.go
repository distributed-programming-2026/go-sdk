package mysql_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkmysql "github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

func TestLockIntegration(t *testing.T) {
	ctx := t.Context()
	client := newTestClient(t)
	firstConnection, err := client.Connection(ctx)
	require.NoError(t, err)
	defer func() { assert.NoError(t, firstConnection.Close()) }()

	secondConnection, err := client.Connection(ctx)
	require.NoError(t, err)
	defer func() { assert.NoError(t, secondConnection.Close()) }()

	firstLock := sdkmysql.NewLock(ctx, "integration-lock", 0, firstConnection)
	secondLock := sdkmysql.NewLock(ctx, "integration-lock", 0, secondConnection)
	require.NoError(t, firstLock.Lock())
	err = secondLock.Lock()
	assert.ErrorIs(t, err, sdkmysql.ErrLockTimeout)
	assert.True(t, strings.Contains(err.Error(), `lock "integration-lock"`))
	require.NoError(t, firstLock.Unlock())
	require.NoError(t, secondLock.Lock())
	require.NoError(t, secondLock.Unlock())
}

func TestLockerIntegration(t *testing.T) {
	ctx := t.Context()
	client := newTestClient(t)

	t.Run("executes callback and releases lock", func(t *testing.T) {
		locker := sdkmysql.NewLocker(sdkmysql.NewConnectionPool(client))
		lockerCtx := context.WithValue(ctx, contextKey{}, "locker")
		called := false

		require.NoError(t, locker.ExecuteWithLock(lockerCtx, "locker-integration-lock", 0, func() error {
			called = true
			return nil
		}))
		assert.True(t, called)

		connection, err := client.Connection(ctx)
		require.NoError(t, err)
		defer func() { assert.NoError(t, connection.Close()) }()
		lock := sdkmysql.NewLock(ctx, "locker-integration-lock", 0, connection)
		require.NoError(t, lock.Lock())
		require.NoError(t, lock.Unlock())
	})

	t.Run("returns callback error", func(t *testing.T) {
		callbackErr := errors.New("callback failed")
		locker := sdkmysql.NewLocker(sdkmysql.NewConnectionPool(client))
		lockerCtx := context.WithValue(ctx, contextKey{}, "locker-error")

		err := locker.ExecuteWithLock(lockerCtx, "locker-error-lock", 0, func() error {
			return callbackErr
		})
		assert.ErrorIs(t, err, callbackErr)
		assert.True(t, strings.Contains(err.Error(), `lock "locker-error-lock"`))
	})

	t.Run("nested calls release different locks after their callbacks", func(t *testing.T) {
		locker := sdkmysql.NewLocker(sdkmysql.NewConnectionPool(client))
		lockerCtx := context.WithValue(ctx, contextKey{}, "nested-locker")
		contenderConnection, err := client.Connection(ctx)
		require.NoError(t, err)
		defer func() { assert.NoError(t, contenderConnection.Close()) }()
		outerContender := sdkmysql.NewLock(ctx, "outer-nested-lock", 0, contenderConnection)
		innerContender := sdkmysql.NewLock(ctx, "inner-nested-lock", 0, contenderConnection)

		err = locker.ExecuteWithLock(lockerCtx, "outer-nested-lock", 0, func() error {
			err := locker.ExecuteWithLock(lockerCtx, "inner-nested-lock", 0, func() error {
				assert.ErrorIs(t, outerContender.Lock(), sdkmysql.ErrLockTimeout)
				assert.ErrorIs(t, innerContender.Lock(), sdkmysql.ErrLockTimeout)
				return nil
			})
			require.NoError(t, err)

			assert.ErrorIs(t, outerContender.Lock(), sdkmysql.ErrLockTimeout)
			require.NoError(t, innerContender.Lock())
			require.NoError(t, innerContender.Unlock())
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, outerContender.Lock())
		require.NoError(t, outerContender.Unlock())
	})

	t.Run("nested calls release the same lock after the last callback", func(t *testing.T) {
		locker := sdkmysql.NewLocker(sdkmysql.NewConnectionPool(client))
		lockerCtx := context.WithValue(ctx, contextKey{}, "nested-same-locker")
		contenderConnection, err := client.Connection(ctx)
		require.NoError(t, err)
		defer func() { assert.NoError(t, contenderConnection.Close()) }()
		contender := sdkmysql.NewLock(ctx, "same-nested-lock", 0, contenderConnection)

		err = locker.ExecuteWithLock(lockerCtx, "same-nested-lock", 0, func() error {
			err := locker.ExecuteWithLock(lockerCtx, "same-nested-lock", 0, func() error {
				assert.ErrorIs(t, contender.Lock(), sdkmysql.ErrLockTimeout)
				return nil
			})
			require.NoError(t, err)

			assert.ErrorIs(t, contender.Lock(), sdkmysql.ErrLockTimeout)
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, contender.Lock())
		require.NoError(t, contender.Unlock())
	})
}
