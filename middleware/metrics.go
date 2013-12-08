package middleware

import (
	"github.com/alecthomas/tuplespace"
	"time"
)

type metricsMiddleware struct {
	store tuplespace.TupleStore
}

// NewTimingStore provides aggregated timing statistics for storage backends,
// via go-metrics.
func NewTimingStore(store tuplespace.TupleStore) tuplespace.TupleStore {
	return &metricsMiddleware{
		store: store,
	}
}

func (t *metricsMiddleware) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	return t.store.Put(tuples, timeout)
}

func (t *metricsMiddleware) Match(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, error) {
	return t.store.Match(match, limit)
}

func (t *metricsMiddleware) Delete(entries []*tuplespace.TupleEntry) error {
	return t.store.Delete(entries)
}

func (t *metricsMiddleware) Shutdown() {
	t.store.Shutdown()
}

func (t *metricsMiddleware) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	t.store.UpdateStats(stats)
}
