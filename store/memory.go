package store

import (
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/middleware"
	"time"
)

// MemoryStore is an in-memory implementation of a TupleStore. It is mostly useful for testing.
type MemoryStore struct {
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

func (m *MemoryStore) Put(tuples []tuplespace.Tuple, timeout time.Time) ([]*tuplespace.TupleEntry, error) {
	log.Fine("Putting %d tuples", len(tuples))
	entries := make([]*tuplespace.TupleEntry, 0, len(tuples))
	for _, tuple := range tuples {
		m.id++
		entry := &tuplespace.TupleEntry{
			ID:      m.id,
			Tuple:   tuple,
			Timeout: timeout,
		}
		entries = append(entries, entry)
		m.tuples[entry.ID] = entry
	}
	return entries, nil
}

func (m *MemoryStore) Match(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, []*tuplespace.TupleEntry, error) {
	now := time.Now()
	matches := make([]*tuplespace.TupleEntry, 0, 32)
	deletes := make([]*tuplespace.TupleEntry, 0, 32)

	log.Fine("Matching %s against %d tuples limit %d", match, len(m.tuples), limit)
	for _, entry := range m.tuples {
		if entry.IsExpired(now) {
			deletes = append(deletes, entry)
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
		m.Delete(deletes)
	}
	return matches, nil, nil
}

func (m *MemoryStore) Delete(entries []*tuplespace.TupleEntry) error {
	log.Finest("Deleting %d tuples", len(entries))
	for _, entry := range entries {
		delete(m.tuples, entry.ID)
	}
	return nil
}

func (m *MemoryStore) Shutdown() {
}

func (m *MemoryStore) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	stats.Tuples = int64(len(m.tuples))
}
