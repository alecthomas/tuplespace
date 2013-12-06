package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/jmhodges/levigo"
	"sync/atomic"
	"time"
)

var (
	entryPrefix = []byte("e:")
)

type levelDBStore struct {
	id         uint64 // Only updated via atomic ops
	tupleCount int64  // Only updated via atomic ops
	db         *levigo.DB
	roptions   *levigo.ReadOptions
	woptions   *levigo.WriteOptions
}

// NewLevelDBStore creates a new TupleStore backed by LevelDB.
func NewLevelDBStore(path string) (tuplespace.TupleStore, error) {
	options := levigo.NewOptions()
	options.SetEnv(levigo.NewDefaultEnv())
	options.SetCreateIfMissing(true)
	options.SetFilterPolicy(levigo.NewBloomFilter(16))
	options.SetCache(levigo.NewLRUCache(1 << 20))
	options.SetMaxOpenFiles(500)
	options.SetWriteBufferSize(62914560)

	roptions := levigo.NewReadOptions()
	roptions.SetVerifyChecksums(true)

	woptions := levigo.NewWriteOptions()
	woptions.SetSync(true)

	db, err := levigo.Open(path, options)
	if err != nil {
		return nil, err
	}

	log.Info("LevelDBStore: compacting")
	db.CompactRange(levigo.Range{Start: nil, Limit: nil})

	l := &levelDBStore{
		db:       db,
		roptions: roptions,
		woptions: woptions,
	}

	log.Info("LevelDBStore: loading state")
	l.initState()
	log.Info("LevelDBStore: ready, %d tuples, last ID was %d",
		atomic.LoadInt64(&l.tupleCount), atomic.LoadUint64(&l.id))
	return l, nil
}

func (l *levelDBStore) initState() {
	it := l.db.NewIterator(l.roptions)
	for it.Seek(entryPrefix); it.Valid(); it.Next() {
		if bytes.Compare(entryPrefix, it.Key()[:len(entryPrefix)]) != 0 {
			break
		}
		l.tupleCount++
		id := idFromKey(it.Key())
		if id > l.id {
			l.id = id
		}
	}
}

func (l *levelDBStore) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	wb := levigo.NewWriteBatch()
	var entryKey []byte

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
		value, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		// Write update
		wb.Put(entryKey, value)
		atomic.AddInt64(&l.tupleCount, 1)
	}

	l.db.Write(l.woptions, wb)
	return nil
}

func (l *levelDBStore) Match(match tuplespace.Tuple) ([]*tuplespace.TupleEntry, error) {
	// Batch delete all expired entries.
	deletes := levigo.NewWriteBatch()
	deleted := 0

	now := time.Now()
	matches := []*tuplespace.TupleEntry{}

	it := l.db.NewIterator(l.roptions)
	for it.Seek(entryPrefix); it.Valid(); it.Next() {
		if bytes.Compare(entryPrefix, it.Key()[:len(entryPrefix)]) != 0 {
			break
		}
		entry := &tuplespace.TupleEntry{}
		err := json.Unmarshal(it.Value(), entry)
		if err != nil {
			return nil, err
		}
		if !entry.Timeout.IsZero() && now.After(entry.Timeout) {
			deletes.Delete(it.Key())
			deleted++
		} else {
			if match.Match(entry.Tuple) {
				matches = append(matches, entry)
			}
		}
	}
	if deleted > 0 {
		atomic.AddInt64(&l.tupleCount, -1)
		l.db.Write(l.woptions, deletes)
		log.Debug("Purged %d expired tuples from levelDBStore", deleted)
	}
	return matches, nil
}

func (l *levelDBStore) Delete(ids []uint64) error {
	atomic.AddInt64(&l.tupleCount, -int64(len(ids)))
	deletes := levigo.NewWriteBatch()
	for _, id := range ids {
		key := keyForID(id)
		deletes.Delete(key)
	}
	l.db.Write(l.woptions, deletes)
	return nil
}

func (l *levelDBStore) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	stats.Tuples = int(atomic.LoadInt64(&l.tupleCount))
}

func (l *levelDBStore) Shutdown() {
	l.db.Close()
}

func keyForID(id uint64) []byte {
	entryKey := make([]byte, len(entryPrefix)+8)
	copy(entryKey, entryPrefix)
	key := entryKey[len(entryPrefix):]
	binary.LittleEndian.PutUint64(key, id)
	return entryKey
}

func idFromKey(key []byte) uint64 {
	return binary.LittleEndian.Uint64(key[len(entryPrefix):])
}
