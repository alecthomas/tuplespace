package middleware

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

func (t *TimingMiddleware) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	return t.store.Put(tuples, timeout)
}

func (t *TimingMiddleware) Match(match tuplespace.Tuple) ([]*tuplespace.TupleEntry, error) {
	return t.store.Match(match)
}

func (t *TimingMiddleware) Delete(ids []uint64) error {
	return t.store.Delete(ids)
}

func (t *TimingMiddleware) Shutdown() {
	t.store.Shutdown()
}

func (t *TimingMiddleware) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	t.store.UpdateStats(stats)
}
