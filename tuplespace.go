package tuplespace

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

const (
	// MaxTupleLifetime is the maximum time a tuple can be alive in the TupleSpace.
	MaxTupleLifetime = time.Second * 60
)

var (
	ErrTimeout           = errors.New("timeout")
	ErrTupleSpaceDeleted = errors.New("tuplespace deleted")
	ErrCancelled         = errors.New("cancelled")
)

type Tuple map[string]interface{}

type tupleEntry struct {
	Tuple   Tuple
	Expires time.Time
	ack     chan error
}

func (t *tupleEntry) Processed(err error) {
	if t.ack != nil {
		t.ack <- err
	}
}

type tupleEntries struct {
	entries   []*tupleEntry
	free      []int
	seenCount int
}

func (t *tupleEntries) Close() error {
	t.entries = nil
	t.free = nil
	return nil
}

func (t *tupleEntries) Size() int {
	return len(t.entries) - len(t.free)
}

func (t *tupleEntries) Seen() int {
	return t.seenCount
}

func (t *tupleEntries) Add(tuple *tupleEntry) {
	t.seenCount++
	l := len(t.free)
	if l != 0 {
		i := t.free[l-1]
		t.entries[i] = tuple
		t.free = t.free[:l-1]
	} else {
		t.entries = append(t.entries, tuple)
	}
}

func (t *tupleEntries) Remove(i int) {
	t.entries[i] = nil
	t.free = append(t.free, i)
}

// Shuffle entry with another randomly selected entry.
func (t *tupleEntries) Shuffle(i int) {
	j := rand.Int() % len(t.entries)
	t.entries[i], t.entries[j] = t.entries[j], t.entries[i]
}

func (t *tupleEntries) Begin() int {
	return t.Next(-1)
}

func (t *tupleEntries) Next(i int) int {
	i++
	n := len(t.entries)
	for i < n && t.entries[i] == nil {
		i++
	}
	if i >= n {
		return -1
	}
	return i
}

func (t *tupleEntries) End() int {
	return -1
}

func (t *tupleEntries) Get(i int) *tupleEntry {
	return t.entries[i]
}

type waiter struct {
	match   *TupleMatcher
	expires <-chan time.Time
	tuple   chan *tupleEntry
}

func (w *waiter) Close() error {
	close(w.tuple)
	return nil
}

type waiters struct {
	lock      sync.Mutex
	waiters   []*waiter
	seenCount int
}

func (w *waiters) add(a *waiter) int {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.seenCount++
	for i, waiter := range w.waiters {
		if waiter == nil {
			w.waiters[i] = a
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

func (w *waiters) try(entry *tupleEntry) bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	for i, waiter := range w.waiters {
		if waiter != nil && waiter.match.Match(entry.Tuple) {
			waiter.tuple <- entry
			w.waiters[i] = nil
			w.seenCount++
			return true
		}
	}
	return false
}

func (w *waiters) size() int {
	w.lock.Lock()
	defer w.lock.Unlock()
	return len(w.waiters)
}

func (w *waiters) seen() int {
	return w.seenCount
}

func (w *waiters) Close() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	for _, waiter := range w.waiters {
		if waiter != nil {
			waiter.Close()
		}
	}
	w.waiters = nil
	return nil
}

type TupleSpace struct {
	lock    sync.Mutex
	entries tupleEntries
	waiters waiters
}

func New() *TupleSpace {
	return &TupleSpace{}
}

func (t *TupleSpace) Close() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.entries.Close()
	return t.waiters.Close()
}

type Status struct {
	Tuples struct {
		Count int `json:"count"`
		Seen  int `json:"seen"`
	} `json:"tuples"`
	Waiters struct {
		Count    int `json:"count"`
		Seen     int `json:"seen"`
		TimedOut int `json:"timed_out"`
	} `json:"waiters"`
}

func (t *TupleSpace) Status() *Status {
	t.lock.Lock()
	defer t.lock.Unlock()
	status := &Status{}
	status.Tuples.Count = t.entries.Size()
	status.Tuples.Seen = t.entries.Seen()
	status.Waiters.Count = t.waiters.size()
	status.Waiters.Seen = t.waiters.seen()
	return status
}

func (t *TupleSpace) Send(tuple Tuple, expires time.Duration) error {
	t.sendTuple(tuple, expires, false)
	return nil
}

// SendWithAcknowledgement sends a tuple and waits for it to be ack at least once.
func (t *TupleSpace) SendWithAcknowledgement(tuple Tuple, expires time.Duration) error {
	if expires == 0 {
		expires = MaxTupleLifetime
	}
	timeout := time.After(expires)
	entry := t.sendTuple(tuple, expires, true)
	select {
	case <-timeout:
		// FIXME: There's a race here. The tuple may still be consumed in the
		// space after this timeout.
		return ErrTimeout

	case err := <-entry.ack:
		return err
	}
}

func (t *TupleSpace) sendTuple(tuple Tuple, expires time.Duration, ack bool) *tupleEntry {
	if expires == 0 || expires > MaxTupleLifetime {
		expires = MaxTupleLifetime
	}
	var ackch chan error
	if ack {
		ackch = make(chan error, 1)
	}
	entry := &tupleEntry{
		Tuple:   tuple,
		Expires: time.Now().Add(expires),
		ack:     ackch,
	}
	if t.waiters.try(entry) {
		return entry
	}
	t.addEntry(entry)
	return entry
}

// Add an entry to the tuplespace.
func (t *TupleSpace) addEntry(entry *tupleEntry) {
	t.entries.Add(entry)
}

func (t *TupleSpace) Take(match string, timeout time.Duration) (Tuple, error) {
	entry, err := t.consume(&ConsumeRequest{
		Match:   match,
		Timeout: timeout,
		Take:    true,
	})
	if err != nil {
		return nil, err
	}
	entry.Processed(nil)
	return entry.Tuple, nil
}

func (t *TupleSpace) Read(match string, timeout time.Duration) (Tuple, error) {
	entry, err := t.consume(&ConsumeRequest{
		Match:   match,
		Timeout: timeout,
		Take:    false,
	})
	if err != nil {
		return nil, err
	}
	entry.Processed(nil)
	return entry.Tuple, nil
}

func (t *TupleSpace) Transaction() *Transaction {
	return &Transaction{space: t}
}

type ConsumeRequest struct {
	Match   string
	Timeout time.Duration
	Take    bool
	Cancel  <-chan bool
}

func (t *TupleSpace) consume(req *ConsumeRequest) (*tupleEntry, error) {
	m, err := Match("%s", req.Match)
	if err != nil {
		return nil, err
	}
	now := time.Now()

	// Lock around searching and configuring waiters
	t.lock.Lock()
	for i := t.entries.Begin(); i != t.entries.End(); i = t.entries.Next(i) {
		entry := t.entries.Get(i)
		if entry.Expires.Before(now) {
			entry.Processed(ErrTimeout)
			t.entries.Remove(i)
		} else if m == nil || m.Match(entry.Tuple) {
			if req.Take {
				t.entries.Remove(i)
			}
			t.entries.Shuffle(i)
			t.lock.Unlock()
			return entry, nil
		}
	}
	var expires <-chan time.Time
	if req.Timeout != 0 {
		expires = time.After(req.Timeout)
	}
	waiter := &waiter{
		match:   m,
		expires: expires,
		tuple:   make(chan *tupleEntry),
	}
	id := t.waiters.add(waiter)
	t.lock.Unlock()

	// NOTE: We don't remove() the waiter if the tuple space has been deleted,
	// thus no "defer t.waiters.remove(id)"" here.
	select {
	case <-req.Cancel:
		t.waiters.remove(id)
		return nil, ErrCancelled

	case <-waiter.expires:
		t.waiters.remove(id)
		return nil, ErrTimeout

	case entry := <-waiter.tuple:
		if entry == nil {
			return nil, ErrTupleSpaceDeleted
		}
		t.waiters.remove(id)
		return entry, nil
	}
}

func (t *TupleSpace) Consume(req *ConsumeRequest) (Tuple, error) {
	entry, err := t.consume(req)
	if err != nil {
		return nil, err
	}
	entry.Processed(nil)
	return entry.Tuple, nil
}

type Transaction struct {
	lock    sync.Mutex
	space   *TupleSpace
	entries []*tupleEntry
	taken   []*tupleEntry
}

func (t *Transaction) Take(match string, timeout time.Duration) (Tuple, error) {
	entry, err := t.space.consume(&ConsumeRequest{
		Match:   match,
		Timeout: timeout,
		Take:    true,
	})
	if err != nil {
		return nil, err
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	t.entries = append(t.entries, entry)
	t.taken = append(t.taken, entry)
	return entry.Tuple, nil
}

func (t *Transaction) Read(match string, timeout time.Duration) (Tuple, error) {
	entry, err := t.space.consume(&ConsumeRequest{
		Match:   match,
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	t.entries = append(t.entries, entry)
	return entry.Tuple, nil
}

func (t *Transaction) Commit() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	for _, entry := range t.entries {
		entry.Processed(nil)
	}
	t.entries = nil
	t.taken = nil
	t.space = nil
	return nil
}

func (t *Transaction) Abort() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	for _, entry := range t.taken {
		t.space.addEntry(entry)
	}
	t.entries = nil
	t.taken = nil
	t.space = nil
	return nil
}
