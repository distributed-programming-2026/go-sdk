package sharedpool

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testValue struct {
	closeErr  error
	closeOnce sync.Once
	closed    chan struct{}
	closes    atomic.Int32
}

func newTestValue() *testValue {
	return &testValue{closed: make(chan struct{})}
}

func (v *testValue) Close() error {
	v.closes.Add(1)
	v.closeOnce.Do(func() { close(v.closed) })
	return v.closeErr
}

func TestPoolSharesValueUntilLastLeaseIsClosed(t *testing.T) {
	var calls int
	value := newTestValue()
	pool := NewPool(func(string) (*testValue, error) {
		calls++
		return value, nil
	})

	first, err := pool.Get("key")
	require.NoError(t, err)
	second, err := pool.Get("key")
	require.NoError(t, err)

	assert.NotSame(t, first, second)
	assert.Same(t, first.Value(), second.Value())
	assert.Equal(t, 1, calls)
	require.NoError(t, first.Close())
	assert.EqualValues(t, 0, value.closes.Load())
	require.NoError(t, second.Close())
	assert.EqualValues(t, 1, value.closes.Load())
}

func TestLeaseCloseIsIdempotent(t *testing.T) {
	value := newTestValue()
	pool := NewPool(func(string) (*testValue, error) { return value, nil })
	first, err := pool.Get("key")
	require.NoError(t, err)
	second, err := pool.Get("key")
	require.NoError(t, err)

	require.NoError(t, first.Close())
	require.NoError(t, first.Close())
	assert.EqualValues(t, 0, value.closes.Load())
	require.NoError(t, second.Close())
	assert.EqualValues(t, 1, value.closes.Load())
}

func TestPoolDoesNotShareValuesBetweenKeys(t *testing.T) {
	pool := NewPool(func(string) (*testValue, error) { return newTestValue(), nil })
	first, err := pool.Get("first")
	require.NoError(t, err)
	second, err := pool.Get("second")
	require.NoError(t, err)

	assert.NotSame(t, first.Value(), second.Value())
	require.NoError(t, first.Close())
	require.NoError(t, second.Close())
}

func TestPoolRetriesAfterFactoryError(t *testing.T) {
	factoryErr := errors.New("factory failed")
	value := newTestValue()
	var calls int
	pool := NewPool(func(string) (*testValue, error) {
		calls++
		if calls == 1 {
			return nil, factoryErr
		}
		return value, nil
	})

	got, err := pool.Get("key")
	assert.Nil(t, got)
	assert.ErrorIs(t, err, factoryErr)
	got, err = pool.Get("key")
	require.NoError(t, err)
	assert.Same(t, value, got.Value())
	assert.Equal(t, 2, calls)
	require.NoError(t, got.Close())
}

func TestLastCloseReturnsValueErrorAndRemovesIt(t *testing.T) {
	closeErr := errors.New("close failed")
	var values []*testValue
	pool := NewPool(func(string) (*testValue, error) {
		value := newTestValue()
		value.closeErr = closeErr
		values = append(values, value)
		return value, nil
	})

	first, err := pool.Get("key")
	require.NoError(t, err)
	assert.ErrorIs(t, first.Close(), closeErr)
	assert.ErrorIs(t, first.Close(), closeErr)
	second, err := pool.Get("key")
	require.NoError(t, err)
	assert.NotSame(t, first.Value(), second.Value())
	assert.Len(t, values, 2)
	assert.ErrorIs(t, second.Close(), closeErr)
}

func TestConcurrentGetsCreateOneValue(t *testing.T) {
	const goroutines = 100
	value := newTestValue()
	startFactory := make(chan struct{})
	finishFactory := make(chan struct{})
	var calls atomic.Int32
	pool := NewPool(func(string) (*testValue, error) {
		calls.Add(1)
		close(startFactory)
		<-finishFactory
		return value, nil
	})

	leases := make(chan *SharedValue[string, *testValue], goroutines)
	errCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := pool.Get("key")
			if err != nil {
				errCh <- err
				return
			}
			leases <- lease
		}()
	}
	<-startFactory
	close(finishFactory)
	wg.Wait()
	close(leases)
	close(errCh)

	for err := range errCh {
		assert.NoError(t, err)
	}
	assert.EqualValues(t, 1, calls.Load())
	for lease := range leases {
		require.NoError(t, lease.Close())
	}
	assert.EqualValues(t, 1, value.closes.Load())
}

func TestFactoryCanGetAnotherKeyFromSamePool(t *testing.T) {
	var pool *Pool[string, *testValue]
	pool = NewPool(func(key string) (*testValue, error) {
		if key == "outer" {
			inner, err := pool.Get("inner")
			if err != nil {
				return nil, err
			}
			if err := inner.Close(); err != nil {
				return nil, err
			}
		}
		return newTestValue(), nil
	})

	outer, err := pool.Get("outer")
	require.NoError(t, err)
	require.NoError(t, outer.Close())
}
