package tuplespace

import (
	"fmt"
	log "github.com/alecthomas/log4go"
	"strings"
	"sync"
	"time"
)

type tupleVisitor func(tuple Tuple) int

type tupleEntry struct {
	tuple   Tuple
	timeout time.Time
}

type tupleWaiter struct {
	match   Tuple
	matches chan []Tuple
	timeout time.Time
	actions int
	// true if tuples were found, false otherwise
	result chan error
	cancel chan *tupleWaiter
}

func (t *tupleWaiter) String() string {
	var timeout time.Duration
	if !t.timeout.IsZero() {
		timeout = t.timeout.Sub(time.Now())
	}
	actions := []string{}
	if t.actions&ActionTake != 0 {
		actions = append(actions, "TAKE")
	} else {
		actions = append(actions, "READ")
	}
	if t.actions&ActionOne != 0 {
		actions = append(actions, "ONE")
	} else {
		actions = append(actions, "ALL")
	}
	return fmt.Sprintf("ReadOperationHandle{%s, %v, %v}", strings.Join(actions, "|"), t.match, timeout)
}

func (r *tupleWaiter) Cancel() {
	r.cancel <- r
}

func (t *tupleWaiter) Get() chan []Tuple {
	return t.matches
}

func (t *tupleWaiter) Error() chan error {
	return t.result
}

func newWaiter(cancel chan *tupleWaiter, match Tuple, timeout time.Duration, actions int) *tupleWaiter {
	var expires time.Time
	if timeout != 0 {
		expires = time.Now().Add(timeout)
	}
	return &tupleWaiter{
		match:   match,
		matches: make(chan []Tuple, 1),
		timeout: expires,
		result:  make(chan error, 1),
		actions: actions,
		cancel:  cancel,
	}
}

type tupleSpaceImpl struct {
	name         string
	tuples       map[*tupleEntry]interface{}
	waiters      map[*tupleWaiter]interface{}
	in           chan *tupleEntry
	waitersIn    chan *tupleWaiter
	cancel       chan *tupleWaiter
	shutdown     chan bool
	id           uint64
	stats        TupleSpaceStats
	statsUpdated *sync.Cond
}

func NewTupleSpace(name string) TupleSpace {
	ts := &tupleSpaceImpl{
		name:         name,
		tuples:       make(map[*tupleEntry]interface{}),
		waiters:      make(map[*tupleWaiter]interface{}),
		in:           make(chan *tupleEntry, 16),
		waitersIn:    make(chan *tupleWaiter, 16),
		cancel:       make(chan *tupleWaiter, 16),
		shutdown:     make(chan bool, 1),
		statsUpdated: sync.NewCond(&sync.Mutex{}),
	}
	go ts.run()
	return ts
}

func (t *tupleSpaceImpl) run() {
	statTimer := time.Tick(time.Millisecond * 100)
	reportStatsTimer := time.Tick(time.Second * 2)
	purgeTimer := time.Tick(time.Millisecond * 250)
	for {
		select {
		case entry := <-t.in:
			t.processNewEntry(entry)
		case waiter := <-t.waitersIn:
			t.processNewWaiter(waiter)
		case waiter := <-t.cancel:
			t.cancelWaiter(waiter)
		case <-purgeTimer:
			// TODO: Implement timeouts using a heap rather than periodic checks.
			t.purge()
		case <-statTimer:
			t.updateStats()
		case <-reportStatsTimer:
			t.reportStats()
		case <-t.shutdown:
			return
		}
	}
}

func (t *tupleSpaceImpl) reportStats() {
	log.Info("Stats: %s", &t.stats)
}

func (t *tupleSpaceImpl) cancelWaiter(waiter *tupleWaiter) {
	log.Info("Cancelled waiter %s", waiter)
	delete(t.waiters, waiter)
	waiter.result <- CancelledReader
}

func (t *tupleSpaceImpl) updateStats() {
	t.statsUpdated.L.Lock()
	defer t.statsUpdated.L.Unlock()
	log.Finest("Updating stats")
	t.stats.Tuples = len(t.tuples)
	t.stats.Waiters = len(t.waiters)
	t.statsUpdated.Broadcast()
}

func (t *tupleSpaceImpl) processNewEntry(entry *tupleEntry) {
	for waiter := range t.waiters {
		if waiter.match.Match(entry.tuple) {
			delete(t.waiters, waiter)

			waiter.matches <- []Tuple{entry.tuple}

			if waiter.actions&ActionTake != 0 {
				return
			}
		}
	}

	t.tuples[entry] = nil
}

func (t *tupleSpaceImpl) processNewWaiter(waiter *tupleWaiter) {
	var matches []Tuple
	for entry := range t.tuples {
		if waiter.match.Match(entry.tuple) {
			matches = append(matches, entry.tuple)
			if waiter.actions&ActionTake != 0 {
				delete(t.tuples, entry)
			}
			if waiter.actions&ActionOne != 0 {
				break
			}
		}
	}

	if len(matches) > 0 {
		log.Fine("Read %s immediately returned %d matching tuples", waiter, len(matches))
		waiter.matches <- matches
	} else {
		log.Fine("Adding new waiter %s", waiter)
		t.waiters[waiter] = nil
	}
}

// Purge expired tuples and waiters.
func (t *tupleSpaceImpl) purge() {
	log.Fine("Purging tuples and waiters")
	tuples := 0
	now := time.Now()
	for entry := range t.tuples {
		if !entry.timeout.IsZero() && entry.timeout.Before(now) {
			delete(t.tuples, entry)
			tuples++
		}
	}
	waiters := 0
	for waiter := range t.waiters {
		if !waiter.timeout.IsZero() && waiter.timeout.Before(now) {
			delete(t.waiters, waiter)
			waiter.result <- ReaderTimeout
			waiters++
		}
	}
	log.Fine("Purged %d tuples and %d waiters", tuples, waiters)
}

func (t *tupleSpaceImpl) Name() string {
	return t.name
}

func (t *tupleSpaceImpl) Send(tuple Tuple, timeout time.Duration) error {
	log.Debug("Send(%v, %v)", tuple, timeout)
	var expires time.Time
	if timeout != 0 {
		expires = time.Now().Add(timeout)
	}
	entry := &tupleEntry{tuple: tuple, timeout: expires}
	t.in <- entry
	return nil
}

func (t *tupleSpaceImpl) ReadOperation(match Tuple, timeout time.Duration, actions int) ReadOperationHandle {
	waiter := newWaiter(t.cancel, match, timeout, actions)
	t.waitersIn <- waiter
	return waiter
}

func (t *tupleSpaceImpl) Read(match Tuple, timeout time.Duration) (r Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, ActionOne)
	select {
	case err = <-waiter.Error():
		return
	case matches := <-waiter.Get():
		r = matches[0]
		return
	}
}

func (t *tupleSpaceImpl) ReadAll(match Tuple, timeout time.Duration) (r []Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, 0)
	select {
	case err = <-waiter.Error():
		return
	case r = <-waiter.Get():
		return
	}
}

func (t *tupleSpaceImpl) Take(match Tuple, timeout time.Duration) (r Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, ActionOne|ActionTake)
	select {
	case err = <-waiter.Error():
		return
	case matches := <-waiter.Get():
		r = matches[0]
		return
	}
}

func (t *tupleSpaceImpl) TakeAll(match Tuple, timeout time.Duration) (r []Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, ActionTake)
	select {
	case err = <-waiter.Error():
		return
	case r = <-waiter.Get():
		return
	}
}

func (t *tupleSpaceImpl) Shutdown() error {
	t.shutdown <- true
	return nil
}

func (t *tupleSpaceImpl) Stats() TupleSpaceStats {
	t.statsUpdated.L.Lock()
	defer t.statsUpdated.L.Unlock()
	t.statsUpdated.Wait()
	return t.stats
}
