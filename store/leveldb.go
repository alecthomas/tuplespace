package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/jmhodges/levigo"
	"github.com/vmihailenco/msgpack"
	"hash/fnv"
	"sync/atomic"
	"time"
)

var (
	entryPrefix = []byte("e:")
	indexPrefix = []byte("i:")
)

type levelDBStore struct {
	id         uint64 // Only updated via atomic ops
	tupleCount int64  // Only updated via atomic ops
	db         *levigo.DB
	roptions   *levigo.ReadOptions
	itoptions  *levigo.ReadOptions
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

	itoptions := levigo.NewReadOptions()
	itoptions.SetVerifyChecksums(true)
	itoptions.SetFillCache(false)

	woptions := levigo.NewWriteOptions()
	woptions.SetSync(true)

	db, err := levigo.Open(path, options)
	if err != nil {
		return nil, err
	}

	log.Info("LevelDBStore: compacting")
	db.CompactRange(levigo.Range{Start: nil, Limit: nil})

	l := &levelDBStore{
		db:        db,
		roptions:  roptions,
		itoptions: itoptions,
		woptions:  woptions,
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
		value, err := msgpack.Marshal(entry)
		if err != nil {
			return err
		}
		// Write update
		wb.Put(entryKey, value)
		atomic.AddInt64(&l.tupleCount, 1)

		// Write index
		indexEntry(wb, entry)
	}

	l.db.Write(l.woptions, wb)
	return nil
}

func (l *levelDBStore) Match(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, error) {
	entries, deleted, err := l.heuristicMatch(match, limit)
	if err != nil {
		return nil, err
	}

	if deleted > 0 {
		atomic.AddInt64(&l.tupleCount, -int64(deleted))
		log.Debug("Purged %d expired tuples from levelDBStore", deleted)
	}
	return entries, nil
}

func (l *levelDBStore) Delete(entries []*tuplespace.TupleEntry) error {
	atomic.AddInt64(&l.tupleCount, -int64(len(entries)))
	deletes := levigo.NewWriteBatch()
	for _, entry := range entries {
		l := len(entry.Tuple)
		for i, v := range entry.Tuple {
			deletes.Delete(indexKey(entry.ID, l, i, v))
		}
		deletes.Delete(keyForID(entry.ID))
	}
	l.db.Write(l.woptions, deletes)
	return nil
}

func (l *levelDBStore) UpdateStats(stats *tuplespace.TupleSpaceStats) {
	stats.Tuples = atomic.LoadInt64(&l.tupleCount)
}

func (l *levelDBStore) Shutdown() {
	l.db.Close()
}

func (l *levelDBStore) heuristicMatch(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, int, error) {
	c := atomic.LoadInt64(&l.tupleCount)
	if c > 10000 {
		return l.matchWithIndex(match, limit)
	}
	return l.matchWithIteration(match, limit)
}

// Iterate over the entire e: prefix key-space looking for matching entries.
func (l *levelDBStore) matchWithIteration(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, int, error) {
	now := time.Now()
	deletes := levigo.NewWriteBatch()
	defer deletes.Close()
	deleted := 0
	entries := make([]*tuplespace.TupleEntry, 0, 32)
	it := l.db.NewIterator(l.itoptions)
	defer it.Close()
	for it.Seek(entryPrefix); it.Valid(); it.Next() {
		if err := it.GetError(); err != nil {
			return nil, 0, err
		}
		if bytes.Compare(entryPrefix, it.Key()[:len(entryPrefix)]) != 0 {
			break
		}
		entry := &tuplespace.TupleEntry{}
		err := msgpack.Unmarshal(it.Value(), entry)
		if err != nil {
			return nil, 0, err
		}
		if entry.IsExpired(now) {
			deletes.Delete(keyForID(entry.ID))
			deleted++
		} else {
			if match.Match(entry.Tuple) {
				entries = append(entries, entry)
				if len(entries) == limit {
					break
				}
			}
		}
	}

	if deleted > 0 {
		l.db.Write(l.woptions, deletes)
	}
	return entries, deleted, nil
}

// Use the index to retrieve matches.
func (l *levelDBStore) matchWithIndex(match tuplespace.Tuple, limit int) ([]*tuplespace.TupleEntry, int, error) {
	var intersection []uint64
	ln := len(match)
	for i, v := range match {
		if v == nil {
			continue
		}
		hits := make([]uint64, 0, 32)
		prefix := make([]byte, 8)
		makeIndexPrefix(prefix, ln, i, v)

		it := l.db.NewIterator(l.roptions)
		for it.Seek(prefix); it.Valid(); it.Next() {
			key := it.Key()
			if bytes.Compare(key[:8], prefix) != 0 {
				break
			}
			hits = append(hits, binary.BigEndian.Uint64(key[8:]))
		}

		if intersection == nil {
			intersection = hits
		} else {
			intersection = intersect(intersection, hits)
		}
	}

	// Have IDs from the index, retrieve the entries.
	// FIXME: This code is almost identical to the code in matchWithIteration.
	// It should probably be refactored.
	now := time.Now()
	deletes := []*tuplespace.TupleEntry{}
	entries := make([]*tuplespace.TupleEntry, 0, len(intersection))
	for _, id := range intersection {
		key := keyForID(id)
		value, err := l.db.Get(l.roptions, key)
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

// Retrieve IDs using the indexes.
// Index keys are in the form "i:<tuple-length><tuple-field-index><tuple-field-hash>".
func indexEntry(batch *levigo.WriteBatch, entry *tuplespace.TupleEntry) {
	l := len(entry.Tuple)
	for i, v := range entry.Tuple {
		key := indexKey(entry.ID, l, i, v)
		batch.Put(key, []byte{})
	}
}

func indexKey(id uint64, l, i int, v interface{}) []byte {
	key := make([]byte, 16)
	idb := makeIndexPrefix(key, l, i, v)
	binary.BigEndian.PutUint64(idb, id)
	return key
}

// Build the index prefix for a tuple element. "l" is the length of the tuple,
// "i" is the element index, "v" is the element value.
func makeIndexPrefix(key []byte, l, i int, v interface{}) []byte {
	copy(key, indexPrefix)
	key[2] = byte(l)
	key[3] = byte(i)
	h := fnv.New32()
	// FIXME: This is probably quite slow :\ benchmark? Alternatives?
	enc := json.NewEncoder(h)
	enc.Encode(v)
	binary.BigEndian.PutUint32(key[4:], h.Sum32())
	return key[8:]
}

// Return the lookup key for an ID (something like "e:<id>")
func keyForID(id uint64) []byte {
	pl := len(entryPrefix)
	entryKey := make([]byte, pl+8)
	copy(entryKey, entryPrefix)
	binary.BigEndian.PutUint64(entryKey[pl:], id)
	return entryKey
}

func idFromKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(entryPrefix):])
}

func intersect(s, u []uint64) []uint64 {
	shortest := len(u)
	if len(s) < shortest {
		shortest = len(s)
	}
	out := make([]uint64, 0, shortest)
	i := 0
	j := 0
	il := len(s)
	jl := len(u)
	for i < il && j < jl {
		if s[i] == u[j] {
			out = append(out, s[i])
			i++
			j++
		} else if s[i] < u[j] {
			i++
		} else if s[i] > u[j] {
			j++
		}
	}
	return out
}
