package tuplespace_test

import (
	"fmt"
	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/store"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TupleStoreBuilder func(string) tuplespace.TupleStore

type TemporaryDiskStore struct {
	tuplespace.TupleStore
	dir string
}

func NewTemporaryDiskStore(builder TupleStoreBuilder) tuplespace.TupleStore {
	tds := &TemporaryDiskStore{}
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		panic(err.Error())
	}
	tds.dir = dir
	tds.TupleStore = builder(dir)
	return tds
}

func (t *TemporaryDiskStore) Shutdown() {
	t.TupleStore.Shutdown()
	if t.dir != "" {
		os.RemoveAll(t.dir)
	}
}

func BuildLevelDBStore(dir string) tuplespace.TupleStore {
	s, err := store.NewLevelDBStore(dir)
	if err != nil {
		panic(err.Error())
	}
	return s
}

func BuildGKVLiteStore(dir string) tuplespace.TupleStore {
	s, err := store.NewGKVLiteStore(path.Join(dir, "gkvlite.db"))
	if err != nil {
		panic(err.Error())
	}
	return s
}

func BuildMemoryStore(dir string) tuplespace.TupleStore {
	return store.NewMemoryStore()
}

// Benchmark LevelDB
func BenchmarkTupleSpaceLevelDBSend(b *testing.B) { benchmarkTupleSpaceSend(b, BuildLevelDBStore) }
func BenchmarkTupleSpaceLevelDBRead(b *testing.B) { benchmarkTupleSpaceRead(b, BuildLevelDBStore) }
func BenchmarkTupleSpaceLevelDBReadAll100(b *testing.B) {
	benchmarkTupleSpaceReadAll100(b, BuildLevelDBStore)
}
func BenchmarkTupleSpaceLevelDBReadAll1000(b *testing.B) {
	benchmarkTupleSpaceReadAll1000(b, BuildLevelDBStore)
}
func BenchmarkTupleSpaceLevelDBTake(b *testing.B) { benchmarkTupleSpaceTake(b, BuildLevelDBStore) }
func BenchmarkTupleSpaceLevelDBConcurrency32(b *testing.B) {
	benchmarkTupleSpaceStressTestConcurrency32(b, BuildLevelDBStore)
}

// GKVLite benchmarks.
func BenchmarkTupleSpaceGKVLiteSend(b *testing.B) { benchmarkTupleSpaceSend(b, BuildGKVLiteStore) }
func BenchmarkTupleSpaceGKVLiteRead(b *testing.B) { benchmarkTupleSpaceRead(b, BuildGKVLiteStore) }
func BenchmarkTupleSpaceGKVLiteReadAll100(b *testing.B) {
	benchmarkTupleSpaceReadAll100(b, BuildGKVLiteStore)
}
func BenchmarkTupleSpaceGKVLiteReadAll1000(b *testing.B) {
	benchmarkTupleSpaceReadAll1000(b, BuildGKVLiteStore)
}
func BenchmarkTupleSpaceGKVLiteTake(b *testing.B) { benchmarkTupleSpaceTake(b, BuildGKVLiteStore) }
func BenchmarkTupleSpaceGKVLiteConcurrency32(b *testing.B) {
	benchmarkTupleSpaceStressTestConcurrency32(b, BuildGKVLiteStore)
}

// Memory benchmarks.
func BenchmarkTupleSpaceMemorySend(b *testing.B) { benchmarkTupleSpaceSend(b, BuildMemoryStore) }
func BenchmarkTupleSpaceMemoryRead(b *testing.B) { benchmarkTupleSpaceRead(b, BuildMemoryStore) }
func BenchmarkTupleSpaceMemoryReadAll100(b *testing.B) {
	benchmarkTupleSpaceReadAll100(b, BuildMemoryStore)
}
func BenchmarkTupleSpaceMemoryReadAll1000(b *testing.B) {
	benchmarkTupleSpaceReadAll1000(b, BuildMemoryStore)
}
func BenchmarkTupleSpaceMemoryTake(b *testing.B) { benchmarkTupleSpaceTake(b, BuildMemoryStore) }
func BenchmarkTupleSpaceMemoryConcurrency32(b *testing.B) {
	benchmarkTupleSpaceStressTestConcurrency32(b, BuildMemoryStore)
}

// Benchmark implementations
func benchmarkTupleSpaceSend(b *testing.B, builder TupleStoreBuilder) {
	ts := tuplespace.NewTupleSpace(NewTemporaryDiskStore(builder))
	defer ts.Shutdown()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.Send(tuplespace.Tuple{"cmd", "uname -a"}, 0)
	}
}

func benchmarkTupleSpaceRead(b *testing.B, builder TupleStoreBuilder) {
	// ts := tuplespace.NewTupleSpace(store.NewMemoryStore())
	ts := tuplespace.NewTupleSpace(NewTemporaryDiskStore(builder))
	defer ts.Shutdown()
	sendN(ts, b.N, 0)
	sendN(ts, b.N, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match := tuplespace.Tuple{"cmd", int64(i)}
		_, err := ts.ReadAll(match, 0)
		if err != nil {
			panic(err.Error())
		}
	}
}

func benchmarkTupleSpaceReadAll100(b *testing.B, builder TupleStoreBuilder) {
	ts := tuplespace.NewTupleSpace(NewTemporaryDiskStore(builder))
	defer ts.Shutdown()
	sendN(ts, 100, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.ReadAll(tuplespace.Tuple{"cmd", nil}, 0)
	}
}

func benchmarkTupleSpaceReadAll1000(b *testing.B, builder TupleStoreBuilder) {
	ts := tuplespace.NewTupleSpace(NewTemporaryDiskStore(builder))
	defer ts.Shutdown()
	sendN(ts, 1000, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.ReadAll(tuplespace.Tuple{"cmd", nil}, 0)
	}
}

func benchmarkTupleSpaceTake(b *testing.B, builder TupleStoreBuilder) {
	ts := tuplespace.NewTupleSpace(NewTemporaryDiskStore(builder))
	defer ts.Shutdown()
	sendN(ts, b.N, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.Take(tuplespace.Tuple{"cmd", i}, 0)
	}
}

func benchmarkTupleSpaceStressTestConcurrency32(b *testing.B, builder TupleStoreBuilder) {
	ts := tuplespace.NewTupleSpace(NewTemporaryDiskStore(builder))
	defer ts.Shutdown()

	threads := 32
	messages := b.N

	var sent int32
	var read int32

	readWait := sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		readWait.Add(1)
		go func(i int) {
			for n := 0; n < messages; n++ {
				match := tuplespace.Tuple{"reader", int64(i), int64(n)}
				_, err := ts.Take(match, time.Minute)
				if err != nil {
					fmt.Errorf("failed to take: %s", err)
				} else {
					atomic.AddInt32(&read, 1)
				}
			}
			readWait.Done()
		}(i)
	}

	writeWait := sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		writeWait.Add(1)
		go func(i int) {
			for n := 0; n < messages; n++ {
				tuple := tuplespace.Tuple{"reader", int64(i), int64(n)}
				err := ts.Send(tuple, 0)
				if err != nil {
					fmt.Errorf("failed to take: %s", err)
				} else {
					atomic.AddInt32(&sent, 1)
				}
			}
			writeWait.Done()
		}(i)
	}

	writeWait.Wait()
	if int(sent) != threads*messages {
		panic("consistency check failed")
	}

	readWait.Wait()
	if sent != read {
		panic("consistency check failed")
	}

	// assert.Equal(t, ts.Stats(), tuplespace.TupleSpaceStats{
	// 	TuplesSeen:  int64(threads * messages),
	// 	WaitersSeen: int64(threads * messages),
	// 	TuplesTaken: int64(threads * messages),
	// 	TuplesRead:  0,
	// })
}
