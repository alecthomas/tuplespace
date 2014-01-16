package tuplespace_test

import (
	log "github.com/alecthomas/log4go"
	"github.com/alecthomas/tuplespace"
	"github.com/alecthomas/tuplespace/store"
	"github.com/stretchrcom/testify/assert"
	"testing"
	"time"
)

func init() {
	log.AddFilter("stdout", log.WARNING, log.NewConsoleLogWriter())
}

func sendN(ts tuplespace.TupleSpace, n int, timeout time.Duration) {
	tuples := make([]tuplespace.Tuple, n)
	for i := 0; i < n; i++ {
		tuple := tuplespace.Tuple{"cmd": int64(i)}
		tuples[i] = tuple
	}
	ts.SendMany(tuples, timeout)
}

func TestTupleSpaceRead(t *testing.T) {
	ts := tuplespace.NewTupleSpace(store.NewMemoryStore())
	defer ts.Shutdown()
	ts.Send(tuplespace.Tuple{"cmd": "uname -a"}, 0)
	ts.Send(tuplespace.Tuple{"cmd": "uptime"}, 0)
	_, err := ts.Read(tuplespace.MustMatch(`cmd != nil`), time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ts.Stats().Tuples, 2)
}

func TestTupleSpaceReadAll(t *testing.T) {
	ts := tuplespace.NewTupleSpace(store.NewMemoryStore())
	defer ts.Shutdown()
	ts.Send(tuplespace.Tuple{"cmd": "uname -a"}, 0)
	ts.Send(tuplespace.Tuple{"cmd": "uptime"}, 0)
	time.Sleep(time.Millisecond * 100)
	tuples, err := ts.ReadAll(tuplespace.MustMatch(`cmd != nil`), time.Second)
	assert.NoError(t, err)
	assert.Equal(t, tuples, []tuplespace.Tuple{tuplespace.Tuple{"cmd": "uname -a"}, tuplespace.Tuple{"cmd": "uptime"}})
	assert.Equal(t, ts.Stats().Tuples, 2)
}

func TestTupleSpaceTake(t *testing.T) {
	ts := tuplespace.NewTupleSpace(store.NewMemoryStore())
	defer ts.Shutdown()
	ts.Send(tuplespace.Tuple{"cmd": "uname -a"}, 0)
	ts.Send(tuplespace.Tuple{"cmd": "uptime"}, 0)
	_, err := ts.Take(tuplespace.MustMatch(`cmd != nil`), time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ts.Stats().Tuples, 1)
}

func TestTupleSpaceTakeAll(t *testing.T) {
	ts := tuplespace.NewTupleSpace(store.NewMemoryStore())
	defer ts.Shutdown()
	ts.Send(tuplespace.Tuple{"cmd": "uname -a"}, 0)
	ts.Send(tuplespace.Tuple{"cmd": "uptime"}, 0)
	time.Sleep(time.Millisecond * 100)
	tuples, err := ts.TakeAll(tuplespace.MustMatch(`cmd != nil`), time.Second)
	assert.NoError(t, err)
	assert.Equal(t, len(tuples), 2)
	assert.Equal(t, ts.Stats().Tuples, 0)
}
