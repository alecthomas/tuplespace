package tuplespace

import (
	log "github.com/alecthomas/log4go"
	"github.com/stretchrcom/testify/assert"
	"testing"
	"time"
)

func init() {
	log.AddFilter("stdout", log.INFO, log.NewConsoleLogWriter())
}

func TestTupleMatch(t *testing.T) {
	tuple := Tuple{"cmd", "uname -a"}
	match := Tuple{"cmd", nil}
	assert.True(t, match.Match(tuple))
}

func TestTupleDoesNotMatch(t *testing.T) {
	tuple := Tuple{"cmd", "uname -a"}
	match := Tuple{"not", nil}
	assert.False(t, match.Match(tuple))
}

func TestTupleSpaceRead(t *testing.T) {
	ts := NewTupleSpace()
	defer ts.Shutdown()
	ts.Send(Tuple{"cmd", "uname -a"}, 0)
	ts.Send(Tuple{"cmd", "uptime"}, 0)
	_, err := ts.Read(Tuple{"cmd", nil}, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ts.Stats(), TupleSpaceStats{Tuples: 2})
}

func TestTupleSpaceReadAll(t *testing.T) {
	ts := NewTupleSpace()
	defer ts.Shutdown()
	ts.Send(Tuple{"cmd", "uname -a"}, 0)
	ts.Send(Tuple{"cmd", "uptime"}, 0)
	time.Sleep(time.Millisecond * 100)
	tuples, err := ts.ReadAll(Tuple{"cmd", nil}, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, tuples, []Tuple{Tuple{"cmd", "uname -a"}, Tuple{"cmd", "uptime"}})
	assert.Equal(t, ts.Stats(), TupleSpaceStats{Tuples: 2})
}

func TestTupleSpaceTake(t *testing.T) {
	ts := NewTupleSpace()
	defer ts.Shutdown()
	ts.Send(Tuple{"cmd", "uname -a"}, 0)
	ts.Send(Tuple{"cmd", "uptime"}, 0)
	_, err := ts.Take(Tuple{"cmd", nil}, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, ts.Stats(), TupleSpaceStats{Tuples: 1})
}

func TestTupleSpaceTakeAll(t *testing.T) {
	ts := NewTupleSpace()
	defer ts.Shutdown()
	ts.Send(Tuple{"cmd", "uname -a"}, 0)
	ts.Send(Tuple{"cmd", "uptime"}, 0)
	time.Sleep(time.Millisecond * 100)
	tuples, err := ts.TakeAll(Tuple{"cmd", nil}, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, len(tuples), 2)
	assert.Equal(t, ts.Stats(), TupleSpaceStats{Tuples: 0})
}

func TestTupleSpaceStressTestSend(t *testing.T) {
	log.AddFilter("stdout", log.INFO, log.NewConsoleLogWriter())

	ts := NewTupleSpace()
	defer ts.Shutdown()

	for i := 0; i < 10000; i++ {
		ts.Send(Tuple{"cmd", "uname -a"}, time.Millisecond*500)
	}

	assert.Equal(t, ts.Stats(), TupleSpaceStats{Tuples: 10000})
	time.Sleep(time.Second)
	assert.Equal(t, ts.Stats(), TupleSpaceStats{Tuples: 0})
}

func TestTupleSpaceStressTestSendAndReadConcurrently(t *testing.T) {
	ts := NewTupleSpace()
	defer ts.Shutdown()

	for i := 0; i < 10000; i++ {
		ts.Send(Tuple{"cmd", "uname -a"}, time.Millisecond*500)
	}

	assert.Equal(t, ts.Stats(), TupleSpaceStats{Tuples: 10000})
	time.Sleep(time.Second)
	assert.Equal(t, ts.Stats(), TupleSpaceStats{Tuples: 0})
}

func BenchmarkTupleSpaceSend(b *testing.B) {
	ts := NewTupleSpace()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.Send(Tuple{"cmd", "uname -a"}, 0)
	}
}

func BenchmarkTupleSpaceRead(b *testing.B) {
	ts := NewTupleSpace()
	for i := 0; i < b.N; i++ {
		ts.Send(Tuple{"cmd", "uname -a"}, 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.Read(Tuple{"cmd", nil}, 0)
	}
}

func BenchmarkTupleSpaceTake(b *testing.B) {
	ts := NewTupleSpace()
	for i := 0; i < b.N; i++ {
		ts.Send(Tuple{"cmd", "uname -a"}, 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts.Take(Tuple{"cmd", nil}, 0)
	}
}
