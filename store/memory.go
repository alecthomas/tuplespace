package store

import (
	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/middleware"
	"sync/atomic"
	"time"
)

// MemoryStore is an in-memory tuple store.
type MemoryStore struct {
	tuplespace.TupleStore
	id     uint64
	tuples map[uint64]*tuplespace.TupleEntry
}

// NewMemoryStore creates a new in-memory tuple store.
func NewMemoryStore() tuplespace.TupleStore {
	store := &MemoryStore{
		tuples: make(map[uint64]*tuplespace.TupleEntry),
	}
	return middleware.NewLockingMiddleware(store)
}

func (m *MemoryStore) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	for _, tuple := range tuples {
		entry := &tuplespace.TupleEntry{
			ID:      atomic.AddUint64(&m.id, 1),
			Tuple:   tuple,
			Timeout: timeout,
		}
		m.tuples[entry.ID] = entry
	}
	return nil
}

func (m *MemoryStore) Match(match tuplespace.Tuple) ([]*tuplespace.TupleEntry, error) {
	now := time.Now()
	matches := []*tuplespace.TupleEntry{}
	deletes := []uint64{}
	for _, entry := range m.tuples {
		if now.After(entry.Timeout) {
			deletes = append(deletes, entry.ID)
			continue
		}
		if match.Match(entry.Tuple) {
			matches = append(matches, entry)
		}
	}
	if len(deletes) > 0 {
		m.Delete(deletes)
	}
	return matches, nil
}

func (m *MemoryStore) Delete(ids []uint64) error {
	for _, id := range ids {
		delete(m.tuples, id)
	}
	return nil
}

func (m *MemoryStore) Shutdown() {
}

func (m *MemoryStore) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	stats.Tuples = len(m.tuples)
}
