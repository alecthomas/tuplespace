package store

import (
	"github.com/alecthomas/tuplespace"
	"sync"
	"time"
)

// A LockingStore provides RW locks around another TupleStore.
type LockingStore struct {
	lock  sync.RWMutex
	store tuplespace.TupleStore
}

// NewLockingStore wraps an existing store in a RWMutex.
func NewLockingStore(store tuplespace.TupleStore) tuplespace.TupleStore {
	return &LockingStore{
		store: store,
	}
}

func (m *LockingStore) Put(tuple tuplespace.Tuple, timeout time.Time) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.store.Put(tuple, timeout)
}

func (m *LockingStore) Match(match tuplespace.Tuple) ([]*tuplespace.TupleEntry, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.store.Match(match)
}

func (m *LockingStore) Delete(id uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.store.Delete(id)
}

func (m *LockingStore) Shutdown() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.store.Shutdown()
}
