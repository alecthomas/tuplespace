// Package tuplespace provides implementations of a Tuple Space for Go.
//
// It provides both an in-process asynchronous tuplespace type (use
// NewTupleSpace) and a RESTful server.
//
// Two storage backends are currently available: leveldb and in- memory.
//
// Install and the server:
//
// 		$ go get github.com/alecthomas/tuplespace/bin/tuplespaced
// 		$ tuplespaced --log-level=info
// 		[22:26:27 EST 2013/12/05] [INFO] Starting server on http://127.0.0.1:2619/tuplespace/
//    [22:26:27 EST 2013/12/05] [INFO] Compacting database
//
// You can test the tuplespace service with a basic command-line client:
//
//		$ go get github.com/alecthomas/tuplespace/bin/tuplespace
//
// Send a bunch of tuples:
//
//		$ tuplespace --copies=1000 send '["cmd", "uname -a"]'
//
// Take all tuples:
//
//    $ time tuplespace takeall '["cmd", null]' | wc
//
// Python bindings are also available:
//
// 		$ pip install tuplespace
// 		$ python
// 		>>> import tuplespace
// 		>>> ts = tuplespace.TupleSpace()
// 		>>> ts.send(('cmd', 'uname -a'))
// 		>>> ts.take(('cmd', None))
//    ('cmd', 'uname -a')
//
package tuplespace

import (
	"fmt"
	log "github.com/alecthomas/log4go"
	"strings"
	"sync"
	"time"
)

type tupleVisitor func(tuple Tuple) int

type tupleWaiter struct {
	match   Tuple
	matches chan []Tuple
	timeout time.Time
	actions int
	// true if tuples were found, false otherwise
	result chan error
	cancel chan *tupleWaiter
}

type tupleSend struct {
	tuples  []Tuple
	timeout time.Time
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
	store        TupleStore
	waiters      map[*tupleWaiter]interface{}
	in           chan *tupleSend
	waitersIn    chan *tupleWaiter
	cancel       chan *tupleWaiter
	shutdown     chan bool
	id           uint64
	stats        TupleSpaceStats
	statsUpdated *sync.Cond
}

// NewTupleSpace creates a new tuple store using the given storage backend.
func NewTupleSpace(store TupleStore) RawTupleSpace {
	ts := &tupleSpaceImpl{
		waiters:      make(map[*tupleWaiter]interface{}),
		in:           make(chan *tupleSend, 16),
		waitersIn:    make(chan *tupleWaiter, 16),
		cancel:       make(chan *tupleWaiter, 16),
		shutdown:     make(chan bool, 1),
		statsUpdated: sync.NewCond(&sync.Mutex{}),
		store:        store,
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
		case send := <-t.in:
			t.processNewEntries(send)
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
			t.store.Shutdown()
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
	// TODO: Add stats to TupleStore interface.
	// t.stats.Tuples = len(t.tuples)
	t.stats.Waiters = len(t.waiters)
	t.statsUpdated.Broadcast()
}

func (t *tupleSpaceImpl) processNewEntries(entries *tupleSend) {
	for waiter := range t.waiters {
		matches := []Tuple{}
		for i, tuple := range entries.tuples {
			if tuple != nil && waiter.match.Match(tuple) {
				matches = append(matches, tuple)
				matches[i] = nil
				if waiter.actions&ActionTake != 0 {
					return
				}
			}
		}
		if len(matches) > 0 {
			delete(t.waiters, waiter)
			waiter.matches <- matches
		}
	}

	err := t.store.Put(entries.tuples, entries.timeout)
	if err != nil {
		panic(err.Error())
	}
}

func (t *tupleSpaceImpl) processNewWaiter(waiter *tupleWaiter) {
	stored, err := t.store.Match(waiter.match)
	if err != nil {
		panic(err.Error())
	}
	var matches []Tuple
	for _, entry := range stored {
		if waiter.match.Match(entry.Tuple) {
			matches = append(matches, entry.Tuple)
			if waiter.actions&ActionTake != 0 {
				t.store.Delete(entry.ID)
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
	log.Fine("Purging waiters")
	now := time.Now()
	waiters := 0
	for waiter := range t.waiters {
		if !waiter.timeout.IsZero() && waiter.timeout.Before(now) {
			delete(t.waiters, waiter)
			waiter.result <- ReaderTimeout
			waiters++
		}
	}
	log.Fine("Purged %d waiters", waiters)
}

func (t *tupleSpaceImpl) SendMany(tuples []Tuple, timeout time.Duration) error {
	log.Debug("Send(%s, %s)", tuples, timeout)
	var expires time.Time
	if timeout != 0 {
		expires = time.Now().Add(timeout)
	}
	entry := &tupleSend{tuples: tuples, timeout: expires}
	t.in <- entry
	return nil
}

func (t *tupleSpaceImpl) ReadOperation(match Tuple, timeout time.Duration, actions int) ReadOperationHandle {
	waiter := newWaiter(t.cancel, match, timeout, actions)
	log.Debug("ReadOperation(%s)", waiter)
	t.waitersIn <- waiter
	return waiter
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
