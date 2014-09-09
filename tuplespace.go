package tuplespace

import (
	"errors"
	"sync"
	"time"

	"github.com/alecthomas/expr"
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

type Tuple expr.V

type ConsumeOptions struct {
	Match   string
	Timeout time.Duration
	All     bool
	Take    bool
	Cancel  <-chan bool // Cancel can be used to cancel a waiting consume.
}

type tupleEntry struct {
	Tuple   Tuple
	Expires time.Time
	lock    sync.Mutex
	ack     chan error
}

func (t *tupleEntry) Processed(err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.ack == nil {
		return
	}
	t.ack <- err
	close(t.ack)
	t.ack = nil
}

type tupleEntries struct {
	lock    sync.Mutex
	entries map[*tupleEntry]struct{}
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

func (t *tupleEntries) Add(tuple *tupleEntry) {
	t.lock.Lock()
	t.entries[tuple] = struct{}{}
	t.lock.Unlock()
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

func (t *tupleEntries) ConsumeMatch(take, all bool, m *expr.Expression) (entries []*tupleEntry) {
	t.lock.Lock()
	defer t.lock.Unlock()
	now := time.Now()
	for entry := range t.entries {
		if entry.Expires.Before(now) {
			entry.Processed(ErrTimeout)
			delete(t.entries, entry)
		} else if m == nil || m.Bool(expr.V(entry.Tuple)) {
			entry.Processed(nil)
			if take {
				delete(t.entries, entry)
			}
			entries = append(entries, entry)
			if !all {
				return
			}
		}
	}
	return
}

type waiter struct {
	match   *expr.Expression
	expires <-chan time.Time
	tuple   chan *tupleEntry
	take    bool
}

func (w *waiter) Close() error {
	close(w.tuple)
	return nil
}

type waiters struct {
	lock    sync.Mutex
	waiters map[*waiter]struct{}
}

func (w *waiters) add(a *waiter) int {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.waiters[a] = struct{}{}
	return len(w.waiters) - 1
}

func (w *waiters) remove(waiter *waiter) {
	w.lock.Lock()
	defer w.lock.Unlock()
	delete(w.waiters, waiter)
}

func (w *waiters) Try(entry *tupleEntry) (taken bool, ok bool) {
	w.lock.Lock()
	defer w.lock.Unlock()

	for waiter := range w.waiters {
		if waiter.match.Bool(expr.V(entry.Tuple)) {
			waiter.tuple <- entry
			delete(w.waiters, waiter)
			return waiter.take, true
		}
	}
	return false, false
}

func (w *waiters) Size() int {
	w.lock.Lock()
	defer w.lock.Unlock()
	return len(w.waiters)
}

func (w *waiters) Close() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	for waiter := range w.waiters {
		waiter.Close()
	}
	w.waiters = nil
	return nil
}

type TupleSpace struct {
	lock    sync.Mutex
	entries tupleEntries
	waiters waiters
	status  Status
}

func New() *TupleSpace {
	return &TupleSpace{
		entries: tupleEntries{
			entries: map[*tupleEntry]struct{}{},
		},
		waiters: waiters{
			waiters: map[*waiter]struct{}{},
		},
	}
}

func (t *TupleSpace) Close() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.entries.Close()
	return t.waiters.Close()
}

type TuplesStatus struct {
	Count int `json:"count"`
	Seen  int `json:"seen"`
}

type WaitersStatus struct {
	Count int `json:"count"`
	Seen  int `json:"seen"`
}

type Status struct {
	Tuples  TuplesStatus  `json:"tuples"`
	Waiters WaitersStatus `json:"waiters"`
}

func (t *TupleSpace) Status() *Status {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.status.Tuples.Count = t.entries.Size()
	t.status.Waiters.Count = t.waiters.Size()
	return &t.status
}

func (t *TupleSpace) Send(tuple Tuple, expires time.Duration) error {
	t.sendTuple(tuple, expires, false)
	return nil
}

func (t *TupleSpace) SendMany(tuples []Tuple, expires time.Duration) error {
	for _, tuple := range tuples {
		t.sendTuple(tuple, expires, false)
	}
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
	var ackch chan error
	if ack {
		ackch = make(chan error, 1)
	}
	entry := &tupleEntry{
		Tuple:   tuple,
		Expires: time.Now().Add(expires),
		ack:     ackch,
	}
	t.lock.Lock()
	t.status.Tuples.Seen++
	taken, ok := t.waiters.Try(entry)
	if !taken || !ok {
		t.addEntry(entry)
	}
	t.lock.Unlock()
	return entry
}

// Add an entry to the tuplespace.
func (t *TupleSpace) addEntry(entry *tupleEntry) {
	t.entries.Add(entry)
}

// Read an entry from the TupleSpace. Consecutive reads can return the same tuple.
func (t *TupleSpace) Read(match string, timeout time.Duration) (Tuple, error) {
	entries, err := t.consume(&ConsumeOptions{
		Match:   match,
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}
	entries[0].Processed(nil)
	return entries[0].Tuple, nil
}

func (t *TupleSpace) ReadAll(match string, timeout time.Duration) ([]Tuple, error) {
	entries, err := t.consume(&ConsumeOptions{
		Match:   match,
		Timeout: timeout,
		All:     true,
	})
	if err != nil {
		return nil, err
	}
	tuples := make([]Tuple, len(entries))
	for i, entry := range entries {
		tuples[i] = entry.Tuple
		entry.Processed(nil)
	}
	return tuples, nil
}

func (t *TupleSpace) TakeWithCancel(match string, timeout time.Duration, reservationTimeout time.Duration, cancel <-chan bool) (*Reservation, error) {
	if reservationTimeout == 0 {
		reservationTimeout = MaxReservationTimeout
	}
	entries, err := t.consume(&ConsumeOptions{
		Match:   match,
		Timeout: timeout,
		Take:    true,
		Cancel:  cancel,
	})
	if err != nil {
		return nil, err
	}
	r := &Reservation{
		entry:   entries[0],
		space:   t,
		timeout: time.After(reservationTimeout),
	}
	r.tomb.Go(r.run)
	return r, nil
}

// Take a tuple within a reservation. Takes must always be performed inside a
// reservation due so that in a multi-server setup, redundant taken tuples can
// be returned.
func (t *TupleSpace) Take(match string, timeout time.Duration, reservationTimeout time.Duration) (*Reservation, error) {
	return t.TakeWithCancel(match, timeout, reservationTimeout, nil)
}

func (t *TupleSpace) consume(req *ConsumeOptions) ([]*tupleEntry, error) {
	m, err := expr.Compile(req.Match)
	if err != nil {
		return nil, err
	}

	// Lock around searching and configuring waiters
	t.lock.Lock()
	entries := t.entries.ConsumeMatch(req.Take, req.All, m)
	if len(entries) > 0 {
		t.lock.Unlock()
		return entries, nil
	}
	var expires <-chan time.Time
	if req.Timeout != 0 {
		expires = time.After(req.Timeout)
	}
	waiter := &waiter{
		match:   m,
		expires: expires,
		tuple:   make(chan *tupleEntry),
		take:    req.Take,
	}
	t.status.Waiters.Seen++
	t.waiters.add(waiter)
	t.lock.Unlock()

	// NOTE: We don't remove() the waiter if the tuple space has been deleted,
	// thus no "defer t.waiters.remove(id)"" here.
	select {
	case <-req.Cancel:
		t.waiters.remove(waiter)
		return nil, ErrCancelled

	case <-waiter.expires:
		t.waiters.remove(waiter)
		return nil, ErrTimeout

	case entry := <-waiter.tuple:
		if entry == nil {
			return nil, ErrTupleSpaceDeleted
		}
		t.waiters.remove(waiter)
		return []*tupleEntry{entry}, nil
	}
}

// Consume provides low-level control over consume operations Read, ReadAll and Take.
func (t *TupleSpace) Consume(req *ConsumeOptions) ([]Tuple, error) {
	entries, err := t.consume(req)
	if err != nil {
		return nil, err
	}
	tuples := make([]Tuple, len(entries))
	for i, entry := range entries {
		tuples[i] = entry.Tuple
		entry.Processed(nil)
	}
	return tuples, nil
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

// Expired returns true if the reservation is no longer valid, for any reason.
func (r *Reservation) Expired() bool {
	return r.tomb.Err() != tomb.ErrStillAlive
}

func (r *Reservation) Wait() error {
	return r.tomb.Wait()
}

func (r *Reservation) Tuple() Tuple {
	return r.entry.Tuple
}

func (r *Reservation) Complete() error {
	r.lock.Lock()
	if !r.tomb.Alive() {
		r.lock.Unlock()
		return r.tomb.Err()
	}
	r.entry.Processed(nil)
	r.tomb.Kill(nil)
	r.lock.Unlock()
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
