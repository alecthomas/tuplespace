package middleware

import (
	"github.com/alecthomas/tuplespace"
	"time"
)

type MetricsMiddleware struct {
	store tuplespace.TupleStore
}

// NewTimingStore provides aggregated timing statistics for storage backends,
// via go-metrics.
func NewTimingStore(store tuplespace.TupleStore) *MetricsMiddleware {
	return &MetricsMiddleware{
		store: store,
	}
}

func (t *MetricsMiddleware) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	return t.store.Put(tuples, timeout)
}

func (t *MetricsMiddleware) Match(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, []*tuplespace.TupleEntry, error) {
	return t.store.Match(match, limit)
}

func (t *MetricsMiddleware) Delete(entries []*tuplespace.TupleEntry) error {
	return t.store.Delete(entries)
}

func (t *MetricsMiddleware) Shutdown() {
	t.store.Shutdown()
}

func (t *MetricsMiddleware) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	t.store.UpdateStats(stats)
}
