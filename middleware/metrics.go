package middleware

import (
	"github.com/alecthomas/tuplespace"
	"github.com/rcrowley/go-metrics"
	"time"
)

type Interval struct {
	t     metrics.Timer
	start time.Time
}

func NewInterval(name string) *Interval {
	t := metrics.NewTimer()
	metrics.Register(name, t)
	return &Interval{
		t: t,
	}
}

func (i *Interval) Start() {
	i.start = time.Now()
}

func (i *Interval) End() {
	i.t.UpdateSince(i.start)
}

type TimingMiddleware struct {
	store           tuplespace.TupleStore
	put, match, del *Interval
}

// NewTimingMiddleware provides aggregated timing statistics for storage backends,
// via go-metrics.
func NewTimingMiddleware(store tuplespace.TupleStore) *TimingMiddleware {
	return &TimingMiddleware{
		store: store,
		put:   NewInterval("TupleSpace.Put"),
		match: NewInterval("TupleSpace.Match"),
		del:   NewInterval("TupleSpace.Delete"),
	}
}

func (t *TimingMiddleware) Put(tuples []tuplespace.Tuple, timeout time.Time) ([]*tuplespace.TupleEntry, error) {
	t.put.Start()
	defer t.put.End()
	return t.store.Put(tuples, timeout)
}

func (t *TimingMiddleware) Match(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, []*tuplespace.TupleEntry, error) {
	t.match.Start()
	defer t.match.End()
	return t.store.Match(match, limit)
}

func (t *TimingMiddleware) Delete(entries []*tuplespace.TupleEntry) error {
	t.del.Start()
	defer t.del.End()
	return t.store.Delete(entries)
}

func (t *TimingMiddleware) Shutdown() {
	t.store.Shutdown()
}

func (t *TimingMiddleware) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	t.store.UpdateStats(stats)
}
