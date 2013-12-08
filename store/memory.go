package store

import (
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/middleware"
	"sync/atomic"
	"time"
)

type memoryStore struct {
	tuplespace.TupleStore
	id     uint64
	tuples map[uint64]*tuplespace.TupleEntry
}

// NewMemoryStore creates a new in-memory tuple store.
func NewMemoryStore() tuplespace.TupleStore {
	store := &memoryStore{
		tuples: make(map[uint64]*tuplespace.TupleEntry),
	}
	return middleware.NewLockingMiddleware(store)
}

func (m *memoryStore) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	log.Info("Putting %d tuples", len(tuples))
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

func (m *memoryStore) Match(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, error) {
	now := time.Now()
	matches := make([]*tuplespace.TupleEntry, 0, 32)
	deletes := make([]uint64, 0, 32)

	log.Info("Matching %s against %d tuples limit %d", match, len(m.tuples), limit)
	for _, entry := range m.tuples {
		if entry.IsExpired(now) {
			deletes = append(deletes, entry.ID)
			continue
		}
		if match.Match(entry.Tuple) {
			matches = append(matches, entry)
			if len(matches) == limit {
				break
			}
		}
	}

	if len(deletes) > 0 {
		m.delete(deletes)
	}
	return matches, nil
}

func (m *memoryStore) delete(ids []uint64) error {
	log.Finest("Deleting %d tuples", len(ids))
	for _, id := range ids {
		delete(m.tuples, id)
	}
	return nil
}

func (m *memoryStore) Shutdown() {
}

func (m *memoryStore) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	stats.Tuples = len(m.tuples)
}
