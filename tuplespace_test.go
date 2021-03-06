package tuplespace

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchrcom/testify/assert"
)

type tupleEntriesSlice []*tupleEntry

func (t tupleEntriesSlice) Len() int           { return len(t) }
func (t tupleEntriesSlice) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t tupleEntriesSlice) Less(i, j int) bool { return t[i].Expires.Before(t[j].Expires) }

func makeSampleTuples() (*tupleEntries, tupleEntriesSlice) {
	tuples := &tupleEntries{
		entries: map[*tupleEntry]struct{}{},
	}
	expected := tupleEntriesSlice{
		&tupleEntry{Expires: time.Now().Add(time.Second)},
		&tupleEntry{Expires: time.Now().Add(time.Second * 2)},
		&tupleEntry{Expires: time.Now().Add(time.Second * 3)},
		&tupleEntry{Expires: time.Now().Add(time.Second * 4)},
	}
	tuples.Add(expected[0])
	tuples.Add(expected[1])
	tuples.Add(expected[2])
	tuples.Add(expected[3])
	sort.Sort(expected)
	return tuples, expected
}

func TestTuplesAdd(t *testing.T) {
	tuples, expected := makeSampleTuples()
	actual := (tupleEntriesSlice)(tuples.Copy())
	sort.Sort(actual)
	assert.Equal(t, expected, actual)
}

func TestTuplesRemove(t *testing.T) {
	tuples, expected := makeSampleTuples()
	i := 0
	for entry := range tuples.entries {
		if i == 1 {
			delete(tuples.entries, entry)
			break
		}
		i++
	}
	count := tuples.Size()
	assert.Equal(t, len(expected)-1, count)
	assert.Equal(t, 3, len(tuples.entries))
}

func TestTupleSpaceTake(t *testing.T) {
	ts := New()
	ts.Send(Tuple{"a": 10}, 0)
	assert.Equal(t, 1, len(ts.entries.entries))
	reservation, err := ts.Take("a == 10", 0, 0)
	assert.NoError(t, err)
	tuple := reservation.Tuple()
	reservation.Complete()
	assert.NotNil(t, tuple)
	assert.Equal(t, 10, tuple["a"])
}

func TestTupleSpaceTakeWaitTimeout(t *testing.T) {
	errors := make(chan error)
	ts := New()
	ts.Send(Tuple{"a": 9}, 0)

	go func() {
		reservation, err := ts.Take("a >= 10", time.Millisecond*10, 0)
		errors <- err
		if reservation != nil {
			reservation.Complete()
		}
	}()

	ts.Send(Tuple{"a": 9}, 0)
	err := <-errors
	assert.Equal(t, ErrTimeout, err)
}

func TestTupleSpaceCompleteAfterReservationTimeout(t *testing.T) {
	ts := New()
	ts.Send(Tuple{"a": 1}, 0)
	r, err := ts.Take("", 0, time.Millisecond*100)
	assert.NoError(t, err)
	time.Sleep(time.Millisecond * 200)
	assert.Equal(t, ErrReservationTimeout, r.Complete())
}

func TestTupleSpaceTakeConcurrent(t *testing.T) {
	ts := New()
	go func() {
		for i := 0; i < 1000; i++ {
			ts.Send(Tuple{"i": i}, 0)
		}
	}()
	tuples := make(chan Tuple, 1000)
	wg := sync.WaitGroup{}
	errors := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				reservation, err := ts.Take("", 0, 0)
				if err != nil {
					errors <- err
					return
				}
				reservation.Complete()
				tuples <- reservation.Tuple()
			}
		}()
	}
	wg.Wait()
	close(tuples)
	count := 0
	for _ = range tuples {
		count++
	}
	assert.Equal(t, 1000, count)
}

func TestTupleSpaceReadAll(t *testing.T) {
	ts := New()
	for i := 0; i < 100; i++ {
		ts.Send(Tuple{"i": i}, 0)
	}
	tuples, err := ts.ReadAll("i >= 50", 0)
	assert.NoError(t, err)
	assert.Equal(t, 50, len(tuples))
}

func TestTupleSpaceReadIsRandomish(t *testing.T) {
	ts := New()
	for i := 0; i < 100; i++ {
		ts.Send(Tuple{"i": i}, 0)
	}
	notequal := 0
	for i := 0; i < 100; i++ {
		tuple, err := ts.Read("i", 0)
		assert.NoError(t, err)
		j := tuple["i"].(int)
		if i != j {
			notequal++
		}
	}
	assert.True(t, notequal > 50)
}

func TestSendWithAcknowledgementTimesOut(t *testing.T) {
	ts := New()
	errors := make(chan error, 1)
	go func() {
		err := ts.SendWithAcknowledgement(Tuple{"i": 10}, time.Millisecond*100)
		errors <- err
	}()
	assert.Equal(t, ErrTimeout, <-errors)
}

func TestSendWithAcknowledgement(t *testing.T) {
	ts := New()
	errors := make(chan error, 1)
	go func() {
		err := ts.SendWithAcknowledgement(Tuple{"i": 10}, 0)
		errors <- err
	}()
	reservation, err := ts.Take("i", 0, 0)
	assert.NoError(t, err)
	reservation.Complete()
	assert.Equal(t, Tuple{"i": 10}, reservation.Tuple())
	assert.NoError(t, <-errors)
}

func TestSendWithAcknowledgementAndMultipleReads(t *testing.T) {
	ts := New()
	errors := make(chan error, 1)
	go func() {
		errors <- ts.SendWithAcknowledgement(Tuple{"i": 10}, time.Second*1)
	}()
	tuple, err := ts.Read("i", 0)
	assert.NoError(t, <-errors)
	assert.NoError(t, err)
	assert.Equal(t, Tuple{"i": 10}, tuple)

	tuple, err = ts.Read("i", 0)
	assert.NoError(t, err)
	assert.Equal(t, Tuple{"i": 10}, tuple)
}

func TestTakeTimeout(t *testing.T) {
	ts := New()
	ts.Send(Tuple{}, 0)
	r, err := ts.Take("", time.Second, time.Millisecond*50)
	assert.Equal(t, 0, ts.Status().Tuples.Count)
	assert.NoError(t, err)
	err = r.Wait()
	assert.Equal(t, ErrReservationTimeout, err)
	assert.Equal(t, 1, ts.Status().Tuples.Count)
}

func TestTakeComplete(t *testing.T) {
	ts := New()
	ts.Send(Tuple{}, 0)
	r, err := ts.Take("", time.Second, time.Millisecond*50)
	assert.Equal(t, 0, ts.Status().Tuples.Count)
	err = r.Complete()
	assert.NoError(t, err)
	assert.Equal(t, 0, ts.Status().Tuples.Count)
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 0, ts.Status().Tuples.Count)
}

func TestTakeCancel(t *testing.T) {
	ts := New()
	ts.Send(Tuple{}, 0)
	r, err := ts.Take("", time.Second, time.Millisecond*50)
	assert.Equal(t, 0, ts.Status().Tuples.Count)
	err = r.Cancel()
	assert.NoError(t, err)
	assert.Equal(t, 1, ts.Status().Tuples.Count)
}

func TestTakeCompleteWithAcknowledgement(t *testing.T) {
	ts := New()
	errors := make(chan error)
	go func() {
		errors <- ts.SendWithAcknowledgement(Tuple{"a": 10}, 0)
	}()
	r, err := ts.Take("", 0, time.Second*10)
	assert.NoError(t, err)
	r.Complete()
	err = <-errors
	assert.NoError(t, err)
}

func BenchmarkTupleSend(b *testing.B) {
	ts := New()
	tuple := Tuple{"i": 0}
	for i := 0; i < b.N; i++ {
		ts.Send(tuple, 0)
	}
}

func benchTupleSendTakeN(b *testing.B, n int) {
	ts := New()
	tuple := Tuple{"i": 0}
	for i := 0; i < b.N/n; i++ {
		for j := 0; j < n; j++ {
			ts.Send(tuple, 0)
		}
		for j := 0; j < n; j++ {
			r, err := ts.Take("", 0, 0)
			if err != nil {
				panic(err)
			}
			r.Complete()
		}
	}

}

func BenchmarkTupleSendTake10(b *testing.B) {
	benchTupleSendTakeN(b, 10)
}

func BenchmarkTupleSendTake100(b *testing.B) {
	benchTupleSendTakeN(b, 100)
}

func BenchmarkTupleSendTake1000(b *testing.B) {
	benchTupleSendTakeN(b, 1000)
}

func BenchmarkTupleSendTake10000(b *testing.B) {
	benchTupleSendTakeN(b, 10000)
}
