package middleware

import (
	"github.com/alecthomas/tuplespace"
	"sync"
	"time"
)

type LockingMiddleware struct {
	lock  sync.Mutex
	store tuplespace.TupleStore
}

// NewLockingMiddleware wraps an existing store in a Mutex.
func NewLockingMiddleware(store tuplespace.TupleStore) *LockingMiddleware {
	return &LockingMiddleware{
		store: store,
	}
}

func (l *LockingMiddleware) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.store.Put(tuples, timeout)
}

func (l *LockingMiddleware) Match(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.store.Match(match, limit)
}

func (l *LockingMiddleware) Delete(entries []*tuplespace.TupleEntry) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.store.Delete(entries)
}

func (l *LockingMiddleware) Shutdown() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.store.Shutdown()
}

func (l *LockingMiddleware) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.store.UpdateStats(stats)
}
