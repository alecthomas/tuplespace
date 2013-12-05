package memory

import (
	"github.com/alecthomas/tuplespace"
	"sync/atomic"
	"time"
)

type MemoryStore struct {
	tuplespace.TupleStore
	id     uint64
	tuples map[uint64]*tuplespace.TupleEntry
}

func NewMemoryStore() tuplespace.TupleStore {
	return &MemoryStore{
		tuples: make(map[uint64]*tuplespace.TupleEntry),
	}
}

func (m *MemoryStore) Put(tuple tuplespace.Tuple, timeout time.Duration) error {
	var expires time.Time
	if timeout != 0 {
		expires = time.Now().Add(timeout)
	}
	entry := &tuplespace.TupleEntry{
		ID:      atomic.AddUint64(&m.id, 1),
		Tuple:   tuple,
		Timeout: expires,
	}
	m.tuples[entry.ID] = entry
	return nil
}

func (m *MemoryStore) Near(match tuplespace.Tuple) ([]*tuplespace.TupleEntry, error) {
	matches := []*tuplespace.TupleEntry{}
	for _, entry := range m.tuples {
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
