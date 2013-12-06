package store

import (
	"github.com/alecthomas/tuplespace"
	"sync"
	"time"
)

// A LockingMiddleware provides locking around another TupleStore.
type LockingMiddleware struct {
	lock  sync.Mutex
	store tuplespace.TupleStore
}

// NewLockingMiddleware wraps an existing store in a Mutex.
func NewLockingMiddleware(store tuplespace.TupleStore) tuplespace.TupleStore {
	return &LockingMiddleware{
		store: store,
	}
}

func (m *LockingMiddleware) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.store.Put(tuples, timeout)
}

func (m *LockingMiddleware) Match(match tuplespace.Tuple) ([]*tuplespace.TupleEntry, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.store.Match(match)
}

func (m *LockingMiddleware) Delete(id uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.store.Delete(id)
}

func (m *LockingMiddleware) Shutdown() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.store.Shutdown()
}
