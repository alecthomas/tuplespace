package store

import (
	"bytes"
	"encoding/binary"
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/steveyen/gkvlite"
	"github.com/vmihailenco/msgpack"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// GKVLiteStore is a TupleStore backed by https://github.com/steveyen/gkvlite
type GKVLiteStore struct {
	lock       sync.Mutex
	id         uint64 // Only updated via atomic ops
	tupleCount int64  // Only updated via atomic ops
	f          *os.File
	db         *gkvlite.Store
	data       *gkvlite.Collection
	idx        *gkvlite.Collection
}

// NewGKVLiteStore creates a new TupleStore backed by gkvlite.
func NewGKVLiteStore(path string) (*GKVLiteStore, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0644)
	if err != nil {
		return nil, err
	}
	db, err := gkvlite.NewStore(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	db.SetCollection("data", bytes.Compare)
	db.SetCollection("index", bytes.Compare)

	l := &GKVLiteStore{
		f:    f,
		db:   db,
		data: db.GetCollection("data"),
		idx:  db.GetCollection("index"),
	}

	log.Info("GKVLiteStore: loading state")
	l.initState()
	log.Info("GKVLiteStore: ready, %d tuples, last ID was %d",
		atomic.LoadInt64(&l.tupleCount), atomic.LoadUint64(&l.id))
	return l, nil
}

func (l *GKVLiteStore) initState() {
	l.data.VisitItemsAscend(entryPrefix, false, func(it *gkvlite.Item) bool {
		if bytes.Compare(entryPrefix, it.Key[:len(entryPrefix)]) != 0 {
			return false
		}
		l.tupleCount++
		id := idFromKey(it.Key)
		if id > l.id {
			l.id = id
		}
		return true
	})
}

func (l *GKVLiteStore) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	var entryKey []byte
	empty := []byte{}

	for _, tuple := range tuples {
		// Update ID
		id := atomic.AddUint64(&l.id, 1)
		entryKey = keyForID(id)

		// Serialize value
		entry := &tuplespace.TupleEntry{
			ID:      id,
			Tuple:   tuple,
			Timeout: timeout,
		}
		value, err := msgpack.Marshal(entry)
		if err != nil {
			l.db.FlushRevert()
			return err
		}
		// Write update
		l.data.Set(entryKey, value)
		atomic.AddInt64(&l.tupleCount, 1)

		// Write index
		for _, idx := range indexEntry(entry) {
			l.idx.Set(idx, empty)
		}
	}

	return l.db.Flush()
}

func (l *GKVLiteStore) Match(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, error) {
	entries, deleted, err := l.match(match, limit)
	if err != nil {
		return nil, err
	}

	if deleted > 0 {
		log.Debug("Purged %d expired tuples from GKVLiteStore", deleted)
	}
	return entries, nil
}

func (l *GKVLiteStore) Delete(entries []*tuplespace.TupleEntry) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	atomic.AddInt64(&l.tupleCount, -int64(len(entries)))
	for _, entry := range entries {
		for _, idx := range indexEntry(entry) {
			l.idx.Delete(idx)
		}
		l.idx.Delete(keyForID(entry.ID))
	}
	return l.db.Flush()
}

func (l *GKVLiteStore) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	stats.Tuples = atomic.LoadInt64(&l.tupleCount)
}

func (l *GKVLiteStore) Shutdown() {
	l.db.Close()
	l.f.Close()
}

// Use the index to retrieve matches.
func (l *GKVLiteStore) match(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, int, error) {
	var intersection []uint64
	ln := len(match)
	for i, v := range match {
		if v == nil {
			continue
		}
		hits := make([]uint64, 0, 32)
		prefix := make([]byte, 8)
		makeIndexPrefix(prefix, ln, i, v)

		l.idx.VisitItemsAscend(prefix, false, func(it *gkvlite.Item) bool {
			if bytes.Compare(it.Key[:8], prefix) != 0 {
				return false
			}
			hits = append(hits, binary.BigEndian.Uint64(it.Key[8:]))
			return true
		})

		if intersection == nil {
			intersection = hits
		} else {
			intersection = intersect(intersection, hits)
		}
	}

	// Have IDs from the index, retrieve the entries.
	now := time.Now()
	deletes := []*tuplespace.TupleEntry{}
	entries := make([]*tuplespace.TupleEntry, 0, len(intersection))
	for _, id := range intersection {
		key := keyForID(id)
		value, err := l.data.Get(key)
		if err != nil {
			return nil, 0, err
		}
		entry := &tuplespace.TupleEntry{}
		err = msgpack.Unmarshal(value, entry)
		if err != nil {
			return nil, 0, err
		}
		if entry.IsExpired(now) {
			deletes = append(deletes, entry)
		} else {
			if match.Match(entry.Tuple) {
				entries = append(entries, entry)
				if len(entries) == limit {
					break
				}
			}
		}
	}
	if len(deletes) > 0 {
		l.Delete(deletes)
	}
	return entries, len(deletes), nil
}
