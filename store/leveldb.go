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

type LevelDBTupleStore struct {
	id       uint64
	db       *levigo.DB
	roptions *levigo.ReadOptions
	woptions *levigo.WriteOptions
}

func NewLevelDBTupleStore(path string) (*LevelDBTupleStore, error) {
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

	l := &LevelDBTupleStore{
		db:       db,
		roptions: roptions,
		woptions: woptions,
	}
	if len(b) > 0 {
		l.id = binary.LittleEndian.Uint64(b)
	}
	return l, nil
}

func (l *LevelDBTupleStore) Put(tuple tuplespace.Tuple, timeout time.Time) error {
	// Update ID
	l.id++
	entryKey, key := keyForID(l.id)

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
	wb := levigo.NewWriteBatch()
	wb.Put(idKey, key)
	wb.Put(entryKey, value)
	l.db.Write(l.woptions, wb)
	return nil
}

func (l *LevelDBTupleStore) Match(match tuplespace.Tuple) ([]*tuplespace.TupleEntry, error) {
	// Batch delete all expired entries.
	deletes := levigo.NewWriteBatch()
	deleted := 0

	now := time.Now()
	matches := []*tuplespace.TupleEntry{}

	it := l.db.NewIterator(l.roptions)
	it.Seek(entryPrefix)
	for it.Seek(entryPrefix); it.Valid(); it.Next() {
		if bytes.Compare(entryPrefix, it.Key()[:len(entryPrefix)]) != 0 {
			continue
		}
		entry := &tuplespace.TupleEntry{}
		err := json.Unmarshal(it.Value(), entry)
		if err != nil {
			return nil, err
		}
		if now.After(entry.Timeout) {
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
		log.Debug("Purged %d expired tuples from LevelDBTupleStore", deleted)
	}
	return matches, nil
}

func (l *LevelDBTupleStore) Delete(id uint64) error {
	key, _ := keyForID(id)
	l.db.Delete(l.woptions, key)
	return nil
}

func (l *LevelDBTupleStore) Shutdown() {
	l.db.Close()
}

func keyForID(id uint64) ([]byte, []byte) {
	entryKey := make([]byte, len(entryPrefix)+8)
	copy(entryKey, entryPrefix)
	key := entryKey[len(entryPrefix):]
	binary.LittleEndian.PutUint64(key, id)
	return entryKey, key
}
