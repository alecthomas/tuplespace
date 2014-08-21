package tuplespace

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

const (
	MaxTupleLifetime = time.Second * 60
)

var (
	ErrTimeout = errors.New("no match")
)

type Tuple map[string]interface{}

type TupleEntry struct {
	Tuple   Tuple
	Expires time.Time
}

type Tuples struct {
	tuples []*TupleEntry
	free   []int
}

func (t *Tuples) Add(tuple *TupleEntry) {
	l := len(t.free)
	if l != 0 {
		i := t.free[l-1]
		t.tuples[i] = tuple
		t.free = t.free[:l-1]
	} else {
		t.tuples = append(t.tuples, tuple)
	}
}

func (t *Tuples) Remove(i int) {
	t.tuples[i] = nil
	t.free = append(t.free, i)
}

// Shuffle entry with another randomly selected entry.
func (t *Tuples) Shuffle(i int) {
	j := rand.Int() % len(t.tuples)
	t.tuples[i], t.tuples[j] = t.tuples[j], t.tuples[i]
}

func (t *Tuples) Begin() int {
	return t.Next(-1)
}

func (t *Tuples) Next(i int) int {
	i++
	n := len(t.tuples)
	for i < n && t.tuples[i] == nil {
		i++
	}
	if i >= n {
		return -1
	}
	return i
}

func (t *Tuples) End() int {
	return -1
}

func (t *Tuples) Get(i int) *TupleEntry {
	return t.tuples[i]
}

type waiter struct {
	match   *TupleMatcher
	expires <-chan time.Time
	tuple   chan Tuple
}

type waiters struct {
	waiters []*waiter
	lock    sync.Mutex
}

func (w *waiters) add(a *waiter) int {
	w.lock.Lock()
	defer w.lock.Unlock()
	for i, waiter := range w.waiters {
		if waiter == nil {
			w.waiters[i] = waiter
			return i
		}
	}
	w.waiters = append(w.waiters, a)
	return len(w.waiters) - 1
}

func (w *waiters) remove(i int) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.waiters[i] = nil
}

func (w *waiters) try(tuple Tuple) bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	for i, waiter := range w.waiters {
		if waiter != nil && waiter.match.Match(tuple) {
			waiter.tuple <- tuple
			w.waiters[i] = nil
			return true
		}
	}
	return false
}

type TupleSpace struct {
	lock    sync.Mutex
	tuples  Tuples
	waiters waiters
}

func New() *TupleSpace {
	return &TupleSpace{}
}

func (t *TupleSpace) Send(tuple Tuple, expires time.Duration) error {
	if expires == 0 || expires > MaxTupleLifetime {
		expires = MaxTupleLifetime
	}
	if t.waiters.try(tuple) {
		return nil
	}
	entry := &TupleEntry{
		Tuple:   tuple,
		Expires: time.Now().Add(expires),
	}
	t.tuples.Add(entry)
	return nil
}

func (t *TupleSpace) Take(match string, timeout time.Duration) (Tuple, error) {
	return t.consume(match, timeout, true)
}

func (t *TupleSpace) Read(match string, timeout time.Duration) (Tuple, error) {
	return t.consume(match, timeout, false)
}

func (t *TupleSpace) consume(match string, timeout time.Duration, take bool) (Tuple, error) {
	m, err := Match("%s", match)
	if err != nil {
		return nil, err
	}
	var expires <-chan time.Time
	if timeout != 0 {
		expires = time.After(timeout)
	}
	now := time.Now()
	t.lock.Lock()
	for i := t.tuples.Begin(); i != t.tuples.End(); i = t.tuples.Next(i) {
		tuple := t.tuples.Get(i)
		if tuple.Expires.Before(now) {
			t.tuples.Remove(i)
		} else if m.Match(tuple.Tuple) {
			if take {
				t.tuples.Remove(i)
			}
			t.tuples.Shuffle(i)
			t.lock.Unlock()
			return tuple.Tuple, nil
		}
	}
	t.lock.Unlock()

	waiter := &waiter{
		match:   m,
		expires: expires,
		tuple:   make(chan Tuple),
	}
	id := t.waiters.add(waiter)
	defer t.waiters.remove(id)
	select {
	case <-expires:
		return nil, ErrTimeout

	case tuple := <-waiter.tuple:
		return tuple, nil
	}
}
