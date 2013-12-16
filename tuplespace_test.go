package tuplespace_test

import (
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/store"
	"github.com/stretchrcom/testify/assert"
	"io/ioutil"
	"os"
	"path"
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
