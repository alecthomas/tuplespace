package middleware

import (
	"github.com/alecthomas/tuplespace"
	"sync"
	"time"
)

type lockingMiddleware struct {
	lock  sync.Mutex
	store tuplespace.TupleStore
}

// NewLockingMiddleware wraps an existing store in a Mutex.
func NewLockingMiddleware(store tuplespace.TupleStore) tuplespace.TupleStore {
	return &lockingMiddleware{
		store: store,
	}
}

func (l *lockingMiddleware) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.store.Put(tuples, timeout)
}

func (l *lockingMiddleware) Match(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.store.Match(match, limit)
}

func (l *lockingMiddleware) Delete(entries []*tuplespace.TupleEntry) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.store.Delete(entries)
}

func (l *lockingMiddleware) Shutdown() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.store.Shutdown()
}

func (l *lockingMiddleware) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.store.UpdateStats(stats)
}
