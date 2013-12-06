package store

import (
	"github.com/alecthomas/tuplespace"
	"time"
)

// A TimingMiddleware provides aggregated timing statistics for storage backends.
type TimingMiddleware struct {
	store tuplespace.TupleStore
}

// NewTimingStore wraps an existing store in a Mutex.
func NewTimingStore(store tuplespace.TupleStore) tuplespace.TupleStore {
	return &TimingMiddleware{
		store: store,
	}
}

func (m *TimingMiddleware) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	return m.store.Put(tuples, timeout)
}

func (m *TimingMiddleware) Match(match tuplespace.Tuple) ([]*tuplespace.TupleEntry, error) {
	return m.store.Match(match)
}

func (m *TimingMiddleware) Delete(id uint64) error {
	return m.store.Delete(id)
}

func (m *TimingMiddleware) Shutdown() {
	m.store.Shutdown()
}
