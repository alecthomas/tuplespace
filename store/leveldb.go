package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/jmhodges/levigo"
	"time"
)

var (
	idKey       = []byte("idcounter")
	entryPrefix = []byte("e:")
)

type LevelDBStore struct {
	id       uint64
	db       *levigo.DB
	roptions *levigo.ReadOptions
	woptions *levigo.WriteOptions
}

func NewLevelDBStore(path string) (tuplespace.TupleStore, error) {
	options := levigo.NewOptions()
	cache := levigo.NewLRUCache(1 << 20)
	env := levigo.NewDefaultEnv()
	options.SetCache(cache)
	options.SetEnv(env)
	options.SetCreateIfMissing(true)

	roptions := levigo.NewReadOptions()
	roptions.SetVerifyChecksums(true)

	woptions := levigo.NewWriteOptions()
	woptions.SetSync(true)

	db, err := levigo.Open(path, options)
	if err != nil {
		return nil, err
	}

	log.Info("Compacting database")
	db.CompactRange(levigo.Range{Start: nil, Limit: nil})

	b, err := db.Get(roptions, idKey)
	if err != nil {
		db.Close()
		return nil, err
	}

	l := &LevelDBStore{
		db:       db,
		roptions: roptions,
		woptions: woptions,
	}
	if len(b) > 0 {
		l.id = binary.LittleEndian.Uint64(b)
	}
	return NewLockingMiddleware(l), nil
}

func (l *LevelDBStore) Put(tuples []tuplespace.Tuple, timeout time.Time) error {
	wb := levigo.NewWriteBatch()
	var entryKey, key []byte

	for _, tuple := range tuples {
		// Update ID
		l.id++
		entryKey, key = keyForID(l.id)

		// Serialize value
		entry := &tuplespace.TupleEntry{
			ID:      l.id,
			Tuple:   tuple,
			Timeout: timeout,
		}
		value, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		// Write update
		wb.Put(entryKey, value)
	}

	wb.Put(idKey, key)
	l.db.Write(l.woptions, wb)
	return nil
}

func (l *LevelDBStore) Match(match tuplespace.Tuple) ([]*tuplespace.TupleEntry, error) {
	// Batch delete all expired entries.
	deletes := levigo.NewWriteBatch()
	deleted := 0

	now := time.Now()
	matches := []*tuplespace.TupleEntry{}

	it := l.db.NewIterator(l.roptions)
	it.Seek(entryPrefix)
	for it.Seek(entryPrefix); it.Valid(); it.Next() {
		// TODO: Figure out whether iteration from a seek should be contiguous
		// with respect to the prefix. It seemed like it wasn't, but that seems
		// weird.
		if bytes.Compare(entryPrefix, it.Key()[:len(entryPrefix)]) != 0 {
			continue
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
		l.db.Write(l.woptions, deletes)
		log.Debug("Purged %d expired tuples from LevelDBStore", deleted)
	}
	return matches, nil
}

func (l *LevelDBStore) Delete(id uint64) error {
	key, _ := keyForID(id)
	l.db.Delete(l.woptions, key)
	return nil
}

func (l *LevelDBStore) Shutdown() {
	l.db.Close()
}

func keyForID(id uint64) ([]byte, []byte) {
	entryKey := make([]byte, len(entryPrefix)+8)
	copy(entryKey, entryPrefix)
	key := entryKey[len(entryPrefix):]
	binary.LittleEndian.PutUint64(key, id)
	return entryKey, key
}
