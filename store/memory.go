package store

import (
	"github.com/alecthomas/tuplespace"
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
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		tuples: make(map[uint64]*tuplespace.TupleEntry),
	}
}

func (m *MemoryStore) Put(tuple tuplespace.Tuple, timeout time.Time) error {
	entry := &tuplespace.TupleEntry{
		ID:      atomic.AddUint64(&m.id, 1),
		Tuple:   tuple,
		Timeout: timeout,
	}
	m.tuples[entry.ID] = entry
	return nil
}

func (m *MemoryStore) Match(match tuplespace.Tuple) ([]*tuplespace.TupleEntry, error) {
	now := time.Now()
	matches := []*tuplespace.TupleEntry{}
	for _, entry := range m.tuples {
		if now.After(entry.Timeout) {
			m.Delete(entry.ID)
			continue
		}
		if match.Match(entry.Tuple) {
			matches = append(matches, entry)
		}
	}
	return matches, nil
}

func (m *MemoryStore) Delete(id uint64) error {
	delete(m.tuples, id)
	return nil
}

func (m *MemoryStore) Shutdown() {
}
