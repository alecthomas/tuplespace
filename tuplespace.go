package tuplespace

import (
	"errors"
	"sync"
	"time"

	"gopkg.in/tomb.v2"
)

const (
	// MaxTupleLifetime is the maximum time a tuple can be alive in the TupleSpace.
	MaxTupleLifetime      = time.Second * 60
	MaxConsumeTimeout     = time.Hour * 24
	MaxReservationTimeout = time.Second * 60
)

var (
	ErrTimeout            = errors.New("timeout")
	ErrReservationTimeout = errors.New("reservation timeout")
	ErrTupleSpaceDeleted  = errors.New("tuplespace deleted")
	ErrCancelled          = errors.New("cancelled")
)

type Tuple map[string]interface{}

type ConsumeOptions struct {
	Match   string
	Timeout time.Duration
	Take    bool
	Cancel  <-chan bool // Cancel can be used to cancel a waiting consume.
}

type tupleEntry struct {
	Tuple   Tuple
	Expires time.Time
	ack     chan error
}

func (t *tupleEntry) Processed(err error) {
	defer func() { _ = recover() }()
	t.ack <- err
	close(t.ack)
}

type tupleEntries struct {
	lock      sync.Mutex
	entries   map[*tupleEntry]struct{}
	seenCount int
}

func (t *tupleEntries) Close() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.entries = nil
	return nil
}

func (t *tupleEntries) Size() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return len(t.entries)
}

func (t *tupleEntries) Seen() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.seenCount
}

func (t *tupleEntries) Add(tuple *tupleEntry) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.seenCount++
	t.entries[tuple] = struct{}{}
}

func (t *tupleEntries) Copy() []*tupleEntry {
	t.lock.Lock()
	defer t.lock.Unlock()
	entries := []*tupleEntry{}
	for entry := range t.entries {
		entries = append(entries, entry)
	}
	return entries
}

func (t *tupleEntries) ConsumeMatch(take bool, m *TupleMatcher) *tupleEntry {
	t.lock.Lock()
	defer t.lock.Unlock()
	now := time.Now()
	for entry := range t.entries {
		if entry.Expires.Before(now) {
			entry.Processed(ErrTimeout)
			delete(t.entries, entry)
		} else if m == nil || m.Match(entry.Tuple) {
			entry.Processed(nil)
			if take {
				delete(t.entries, entry)
			}
			return entry
		}
	}
	return nil
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

func (w *waiters) Try(entry *tupleEntry) bool {
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

func (w *waiters) Size() int {
	w.lock.Lock()
	defer w.lock.Unlock()
	return len(w.waiters)
}

func (w *waiters) Seen() int {
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
	return &TupleSpace{
		entries: tupleEntries{
			entries: map[*tupleEntry]struct{}{},
		},
	}
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
	status.Waiters.Count = t.waiters.Size()
	status.Waiters.Seen = t.waiters.Seen()
	return status
}

func (t *TupleSpace) Send(tuple Tuple, expires time.Duration) error {
	t.sendTuple(tuple, expires, false)
	return nil
}

// SendWithAcknowledgement sends a tuple and waits for an acknowledgement that
// the tuple has been processed.
func (t *TupleSpace) SendWithAcknowledgement(tuple Tuple, expires time.Duration) error {
	if expires == 0 {
		expires = MaxTupleLifetime
	}
	timeout := time.After(expires)
	entry := t.sendTuple(tuple, expires, true)
	select {
	case <-timeout:
		// FIXME: There's a race here. The tuple may still be consumed in the
		// window after this timeout.
		return ErrTimeout

	case err := <-entry.ack:
		return err
	}
}

func (t *TupleSpace) sendTuple(tuple Tuple, expires time.Duration, ack bool) *tupleEntry {
	if expires == 0 || expires > MaxTupleLifetime {
		expires = MaxTupleLifetime
	}
	entry := &tupleEntry{
		Tuple:   tuple,
		Expires: time.Now().Add(expires),
		ack:     make(chan error, 1),
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.waiters.Try(entry) {
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
	entry, err := t.consume(&ConsumeOptions{
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
	entry, err := t.consume(&ConsumeOptions{
		Match:   match,
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}
	entry.Processed(nil)
	return entry.Tuple, nil
}

func (t *TupleSpace) ReserveWithCancel(match string, timeout time.Duration, reservationTimeout time.Duration, cancel <-chan bool) (*Reservation, error) {
	entry, err := t.consume(&ConsumeOptions{
		Match:   match,
		Timeout: timeout,
		Take:    true,
		Cancel:  cancel,
	})
	if err != nil {
		return nil, err
	}
	r := &Reservation{
		entry:   entry,
		space:   t,
		timeout: time.After(reservationTimeout),
	}
	r.tomb.Go(r.run)
	return r, nil
}

func (t *TupleSpace) Reserve(match string, timeout time.Duration, reservationTimeout time.Duration) (*Reservation, error) {
	return t.ReserveWithCancel(match, timeout, reservationTimeout, nil)
}

func (t *TupleSpace) consume(req *ConsumeOptions) (*tupleEntry, error) {
	m, err := Match("%s", req.Match)
	if err != nil {
		return nil, err
	}

	// Lock around searching and configuring waiters
	t.lock.Lock()
	entry := t.entries.ConsumeMatch(req.Take, m)
	if entry != nil {
		t.lock.Unlock()
		return entry, nil
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

func (t *TupleSpace) Consume(req *ConsumeOptions) (Tuple, error) {
	entry, err := t.consume(req)
	if err != nil {
		return nil, err
	}
	entry.Processed(nil)
	return entry.Tuple, nil
}

type Reservation struct {
	tomb    tomb.Tomb
	lock    sync.Mutex
	entry   *tupleEntry
	space   *TupleSpace
	timeout <-chan time.Time
}

func (r *Reservation) run() error {
	select {
	case <-r.timeout:
		r.lock.Lock()
		defer r.lock.Unlock()
		r.cancel()
		return ErrReservationTimeout

	case <-r.tomb.Dying():
		return nil
	}
}

func (r *Reservation) Wait() error {
	return r.tomb.Wait()
}

func (r *Reservation) Tuple() Tuple {
	return r.entry.Tuple
}

func (r *Reservation) Complete() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if !r.tomb.Alive() {
		return r.tomb.Err()
	}
	r.entry.Processed(nil)
	r.tomb.Kill(nil)
	return r.tomb.Wait()
}

func (r *Reservation) Cancel() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if !r.tomb.Alive() {
		return r.tomb.Err()
	}
	r.tomb.Kill(nil)
	r.cancel()
	return r.tomb.Wait()
}

func (r *Reservation) cancel() {
	r.space.addEntry(r.entry)
}
