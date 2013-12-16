package tuplespace_test

import (
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/store"
	"github.com/stretchrcom/testify/assert"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	log.AddFilter("stdout", log.WARNING, log.NewConsoleLogWriter())
}

func sendN(ts tuplespace.TupleSpace, n int, timeout time.Duration) {
	tuples := make([]tuplespace.Tuple, n)
	for i := 0; i < n; i++ {
		tuple := tuplespace.Tuple{"cmd", int64(i)}
		tuples[i] = tuple
	}
	ts.SendMany(tuples, timeout)
}

func newTupleSpace() (s tuplespace.TupleSpace, dir string) {
	// return newGKVLiteTupleStore()
	return newLevelDBTupleSpace()
}

func newLevelDBTupleSpace() (s tuplespace.TupleSpace, dir string) {
	dir, err := ioutil.TempDir("", "tuplespace_test.")
	if err != nil {
		panic(err.Error())
	}
	st, err := store.NewLevelDBStore(dir)
	if err != nil {
		panic(err.Error())
	}
	s = tuplespace.NewTupleSpace(st)
	return
}

func newGKVLiteTupleStore() (s tuplespace.TupleSpace, dir string) {
	dir, err := ioutil.TempDir("", "tuplespace_test.")
	if err != nil {
		panic(err.Error())
	}
	st, err := store.NewGKVLiteStore(path.Join(dir, "gkvlite.db"))
	if err != nil {
		panic(err.Error())
	}
	s = tuplespace.NewTupleSpace(st)
	return
}

func TestTupleSpaceRead(t *testing.T) {
	ts, dir := newTupleSpace()
	defer ts.Shutdown()
	defer os.RemoveAll(dir)
	ts.Send(tuplespace.Tuple{"cmd", "uname -a"}, 0)
	ts.Send(tuplespace.Tuple{"cmd", "uptime"}, 0)
	_, err := ts.Read(tuplespace.Tuple{"cmd", nil}, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ts.Stats().Tuples, 2)
}

func TestTupleSpaceReadAll(t *testing.T) {
	ts, dir := newTupleSpace()
	defer ts.Shutdown()
	defer os.RemoveAll(dir)
	ts.Send(tuplespace.Tuple{"cmd", "uname -a"}, 0)
	ts.Send(tuplespace.Tuple{"cmd", "uptime"}, 0)
	time.Sleep(time.Millisecond * 100)
	tuples, err := ts.ReadAll(tuplespace.Tuple{"cmd", nil}, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, tuples, []tuplespace.Tuple{tuplespace.Tuple{"cmd", "uname -a"}, tuplespace.Tuple{"cmd", "uptime"}})
	assert.Equal(t, ts.Stats().Tuples, 2)
}

func TestTupleSpaceTake(t *testing.T) {
	ts, dir := newTupleSpace()
	defer ts.Shutdown()
	defer os.RemoveAll(dir)
	ts.Send(tuplespace.Tuple{"cmd", "uname -a"}, 0)
	ts.Send(tuplespace.Tuple{"cmd", "uptime"}, 0)
	_, err := ts.Take(tuplespace.Tuple{"cmd", nil}, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ts.Stats().Tuples, 1)
}

func TestTupleSpaceTakeAll(t *testing.T) {
	ts, dir := newTupleSpace()
	defer ts.Shutdown()
	defer os.RemoveAll(dir)
	ts.Send(tuplespace.Tuple{"cmd", "uname -a"}, 0)
	ts.Send(tuplespace.Tuple{"cmd", "uptime"}, 0)
	time.Sleep(time.Millisecond * 100)
	tuples, err := ts.TakeAll(tuplespace.Tuple{"cmd", nil}, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, len(tuples), 2)
	assert.Equal(t, ts.Stats().Tuples, 0)
}

func TestTupleSpaceStressTestSendTake(t *testing.T) {
	s := store.NewMemoryStore()
	ts := tuplespace.NewTupleSpace(s)
	defer ts.Shutdown()

	threads := 64
	messages := 100

	var sent int32
	var read int32

	readWait := sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		readWait.Add(1)
		go func(i int) {
			for n := 0; n < messages; n++ {
				match := tuplespace.Tuple{"reader", int64(i), int64(n)}
				tuple, err := ts.Take(match, time.Minute)
				assert.NoError(t, err)
				assert.Equal(t, tuple, match)
				if err == nil {
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
				assert.NoError(t, err)
				if err == nil {
					atomic.AddInt32(&sent, 1)
				}
			}
			writeWait.Done()
		}(i)
	}

	writeWait.Wait()
	assert.Equal(t, sent, threads*messages)

	readWait.Wait()
	assert.Equal(t, sent, read)

	assert.Equal(t, ts.Stats(), tuplespace.TupleSpaceStats{
		TuplesSeen:  int64(threads * messages),
		WaitersSeen: int64(threads * messages),
		TuplesTaken: int64(threads * messages),
		TuplesRead:  0,
	})
}

func BenchmarkTupleSpaceSend(b *testing.B) {
	ts, dir := newTupleSpace()
	defer ts.Shutdown()
	defer os.RemoveAll(dir)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.Send(tuplespace.Tuple{"cmd", "uname -a"}, 0)
	}
}

func BenchmarkTupleSpaceRead(b *testing.B) {
	// ts := tuplespace.NewTupleSpace(store.NewMemoryStore())
	ts, dir := newTupleSpace()
	defer ts.Shutdown()
	defer os.RemoveAll(dir)
	sendN(ts, b.N, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match := tuplespace.Tuple{"cmd", int64(i)}
		_, err := ts.Read(match, 0)
		if err != nil {
			panic(err.Error())
		}
	}
}

func BenchmarkTupleSpaceReadAll100(b *testing.B) {
	ts, dir := newTupleSpace()
	defer ts.Shutdown()
	defer os.RemoveAll(dir)
	sendN(ts, 100, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.ReadAll(tuplespace.Tuple{"cmd", nil}, 0)
	}
}

func BenchmarkTupleSpaceReadAll1000(b *testing.B) {
	ts, dir := newTupleSpace()
	defer ts.Shutdown()
	defer os.RemoveAll(dir)
	sendN(ts, 1000, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.ReadAll(tuplespace.Tuple{"cmd", nil}, 0)
	}
}

func BenchmarkTupleSpaceTake(b *testing.B) {
	ts, dir := newTupleSpace()
	defer ts.Shutdown()
	defer os.RemoveAll(dir)
	sendN(ts, b.N, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.Take(tuplespace.Tuple{"cmd", i}, 0)
	}
}
