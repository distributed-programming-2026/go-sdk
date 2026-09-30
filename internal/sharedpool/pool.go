package sharedpool

import (
	"io"
	"sync"
)

// SharedValue is a single lease on a value owned by a Pool.
// Close releases only this lease and is safe to call more than once.
type SharedValue[K comparable, V io.Closer] struct {
	v       V
	release func() error

	once sync.Once
	err  error
}

func (v *SharedValue[K, V]) Value() V {
	return v.v
}

func (v *SharedValue[K, V]) Close() error {
	v.once.Do(func() {
		v.err = v.release()
	})
	return v.err
}

type ValueFactory[K comparable, V io.Closer] func(key K) (V, error)

func NewPool[K comparable, V io.Closer](factory ValueFactory[K, V]) *Pool[K, V] {
	return &Pool[K, V]{
		valueFactory: factory,
		pool:         make(map[K]*entry[V]),
	}
}

type entry[V io.Closer] struct {
	value V
	refs  int
	ready chan struct{}
	err   error
}

type Pool[K comparable, V io.Closer] struct {
	valueFactory ValueFactory[K, V]

	mu   sync.Mutex
	pool map[K]*entry[V]
}

func (p *Pool[K, V]) Get(key K) (*SharedValue[K, V], error) {
	p.mu.Lock()
	e, ok := p.pool[key]
	if ok {
		e.refs++
		p.mu.Unlock()
		<-e.ready
	} else {
		e = &entry[V]{refs: 1, ready: make(chan struct{})}
		p.pool[key] = e
		p.mu.Unlock()

		e.value, e.err = p.valueFactory(key)
		if e.err != nil {
			p.mu.Lock()
			delete(p.pool, key)
			p.mu.Unlock()
		}
		close(e.ready)
	}

	if e.err != nil {
		return nil, e.err
	}

	return &SharedValue[K, V]{
		v:       e.value,
		release: func() error { return p.release(key, e) },
	}, nil
}

func (p *Pool[K, V]) release(key K, e *entry[V]) error {
	p.mu.Lock()
	e.refs--
	if e.refs > 0 {
		p.mu.Unlock()
		return nil
	}
	delete(p.pool, key)
	p.mu.Unlock()

	return e.value.Close()
}
