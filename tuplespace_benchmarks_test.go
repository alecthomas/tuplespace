package tuplespace_test

import (
	"fmt"
	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/store"
	"io/ioutil"
	"os"
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

func BuildMemoryStore(dir string) tuplespace.TupleStore {
	return store.NewMemoryStore()
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
		ts.Send(tuplespace.Tuple{"cmd": "uname -a"}, 0)
	}
}

func benchmarkTupleSpaceRead(b *testing.B, builder TupleStoreBuilder) {
	ts := tuplespace.NewTupleSpace(NewTemporaryDiskStore(builder))
	defer ts.Shutdown()
	sendN(ts, b.N, 0)
	sendN(ts, b.N, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match := tuplespace.MustMatch(`cmd == %d`, int64(i))
		_, err := ts.Read(match, 0)
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
	match := tuplespace.MustMatch(`cmd != nil`)
	for i := 0; i < b.N; i++ {
		ts.ReadAll(match, 0)
	}
}

func benchmarkTupleSpaceReadAll1000(b *testing.B, builder TupleStoreBuilder) {
	ts := tuplespace.NewTupleSpace(NewTemporaryDiskStore(builder))
	defer ts.Shutdown()
	sendN(ts, 1000, 0)
	b.ResetTimer()
	match := tuplespace.MustMatch(`cmd != nil`)
	for i := 0; i < b.N; i++ {
		ts.ReadAll(match, 0)
	}
}

func benchmarkTupleSpaceTake(b *testing.B, builder TupleStoreBuilder) {
	ts := tuplespace.NewTupleSpace(NewTemporaryDiskStore(builder))
	defer ts.Shutdown()
	sendN(ts, b.N, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match := tuplespace.MustMatch(`cmd == %d`, i)
		ts.Take(match, 0)
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
				match := tuplespace.MustMatch(`"a" == "reader" && "b" == %d && "c" == %d`, i, n)
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
				tuple := tuplespace.Tuple{"a": "reader", "b": int64(i), "c": int64(n)}
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
