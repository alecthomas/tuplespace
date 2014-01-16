// Package tuplespace provides an implementations of a tuple space for Go.
//
// It provides both an in-process asynchronous tuplespace (use
// NewTupleSpace) and a RESTful server binary.
//
// Two storage backends are currently available: leveldb and in-memory.
//
// To use the in-process tuple space:
//
// 		import (
// 			"github.com/alecthomas/tuplespace"
// 			"github.com/alecthomas/tuplespace/store"
// 		)
//
// 		func main() {
//			ts, _ := tuplespace.NewTupleSpace(store.NewMemoryStore())
//			ts.Send(tuplespace.Tuple{"cmd", "uname -a"}, 0)
//			tuple, _ := ts.Take(tuplespace.Tuple{"cmd", nil}, 0)
//			println(tuple.String())
// 		}
//
// Using the client for the server is similarly simple:
//
// 		import (
// 			"github.com/alecthomas/tuplespace"
// 			"github.com/alecthomas/tuplespace/client"
// 		)
//
// 		func main() {
//			ts, _ := client.NewTupleClient("http://127.0.0.1:2619/tuplespace/")
//			ts.Send(tuplespace.Tuple{"cmd", "uname -a"}, 0)
//			tuple, _ := ts.Take(tuplespace.Tuple{"cmd", nil}, 0)
//			println(tuple.String())
// 		}
//
// Install and run the server:
//
//		$ go get github.com/alecthomas/tuplespace/bin/tuplespaced
//		$ tuplespaced --log-level=info
//		[22:26:27 EST 2013/12/05] [INFO] Starting server on http://127.0.0.1:2619/tuplespace/
//		[22:26:27 EST 2013/12/05] [INFO] Compacting database
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
//		$ pip install tuplespace
//		$ python
//		>>> import tuplespace
//		>>> ts = tuplespace.TupleSpace()
//		>>> ts.send(('cmd', 'uname -a'))
//		>>> ts.take(('cmd', None))
//		('cmd', 'uname -a')
//
package tuplespace

import (
	"fmt"
	log "github.com/alecthomas/log4go"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type tupleVisitor func(tuple Tuple) int

type tupleWaiter struct {
	match   *TupleMatcher
	matches chan []Tuple
	timeout time.Time
	actions int
	err     chan error
	cancel  chan *tupleWaiter
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
	return t.err
}

func newWaiter(cancel chan *tupleWaiter, match *TupleMatcher, timeout time.Duration, actions int) *tupleWaiter {
	var expires time.Time
	if timeout != 0 {
		expires = time.Now().Add(timeout)
	}
	return &tupleWaiter{
		match:   match,
		matches: make(chan []Tuple, 1),
		timeout: expires,
		err:     make(chan error, 1),
		actions: actions,
		cancel:  cancel,
	}
}

type tupleSpaceImpl struct {
	store        TupleStore
	waiters      map[*tupleWaiter]interface{}
	waitersLock  sync.RWMutex
	cancel       chan *tupleWaiter
	shutdown     chan bool
	id           uint64
	stats        TupleSpaceStats
	statsUpdated *sync.Cond
}

// NewRawTupleSpace creates a new tuple store using the given storage backend.
func NewRawTupleSpace(store TupleStore) RawTupleSpace {
	ts := &tupleSpaceImpl{
		waiters:      make(map[*tupleWaiter]interface{}),
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
	t.waitersLock.Lock()
	defer t.waitersLock.Unlock()
	delete(t.waiters, waiter)
	waiter.err <- CancelledReader
}

func (t *tupleSpaceImpl) updateStats() {
	t.statsUpdated.L.Lock()
	defer t.statsUpdated.L.Unlock()
	t.waitersLock.RLock()
	defer t.waitersLock.RUnlock()
	t.stats.Waiters = int64(len(t.waiters))
	t.store.UpdateStats(&t.stats)
	t.statsUpdated.Broadcast()
}

func (t *tupleSpaceImpl) processNewEntries(tuples []Tuple, timeout time.Time) error {
	t.waitersLock.Lock()
	defer t.waitersLock.Unlock()

	atomic.AddInt64(&t.stats.TuplesSeen, int64(len(tuples)))

	available := len(tuples)

	// Check for matches against waiters and delete them if they match.
	for waiter := range t.waiters {
		take := waiter.actions&ActionTake != 0
		one := waiter.actions&ActionOne != 0
		var matches []Tuple
		for i, tuple := range tuples {
			if tuple != nil && waiter.match.Match(tuple) {
				matches = append(matches, tuple)
				if take {
					tuples[i] = nil
					available--
					atomic.AddInt64(&t.stats.TuplesTaken, 1)
				} else {
					atomic.AddInt64(&t.stats.TuplesRead, 1)
				}
				if one {
					break
				}
			}
		}

		// Remove and signal the waiter that matched.
		if len(matches) > 0 {
			delete(t.waiters, waiter)
			waiter.matches <- matches
		}

		// If all tuples have been taken, there's nothing else to do.
		if available == 0 {
			return nil
		}
	}

	// Remove entries taken by waiters.
	remaining := make([]Tuple, 0, len(tuples))
	for _, tuple := range tuples {
		if tuple != nil {
			remaining = append(remaining, tuple)
		}
	}

	// Finally, store the remaining tuples.
	_, err := t.store.Put(remaining, timeout)
	return err
}

// When a new waiter arrives, we check if it matches existing tuples. If so,
// we return those tuples. If not, the waiter is added to the list of waiters
// that will match against future tuples.
func (t *tupleSpaceImpl) processNewWaiter(waiter *tupleWaiter) {
	t.waitersLock.Lock()
	defer t.waitersLock.Unlock()

	atomic.AddInt64(&t.stats.WaitersSeen, 1)
	one := waiter.actions&ActionOne != 0
	take := waiter.actions&ActionTake != 0
	limit := 0
	if one {
		limit = 1
	}
	stored, deletes, err := t.store.Match(waiter.match, limit)
	if err != nil {
		waiter.err <- err
		return
	}

	matches := make([]Tuple, 0, len(stored))
	taken := 0
	for _, entry := range stored {
		matches = append(matches, entry.Tuple)
		if take {
			taken++
			deletes = append(deletes, entry)
		}
		if one {
			break
		}
	}

	if len(deletes) > 0 {
		log.Fine("Deleting %d tuples. %d taken by %s, %d expired", len(deletes), taken, waiter, len(deletes)-taken)
		t.store.Delete(deletes)
	}

	if len(matches) > 0 {
		log.Fine("Waiter %s immediately returned %d matching tuples", waiter, len(matches))
		if len(deletes) != 0 {
			atomic.AddInt64(&t.stats.TuplesTaken, int64(len(deletes)))
		} else {
			atomic.AddInt64(&t.stats.TuplesRead, int64(len(matches)))
		}
		waiter.matches <- matches
	} else {
		log.Fine("Adding new waiter %s", waiter)
		t.waiters[waiter] = nil
	}
}

// Purge expired waiters.
func (t *tupleSpaceImpl) purge() {
	t.waitersLock.Lock()
	defer t.waitersLock.Unlock()
	now := time.Now()
	waiters := 0
	for waiter := range t.waiters {
		if !waiter.timeout.IsZero() && waiter.timeout.Before(now) {
			delete(t.waiters, waiter)
			waiter.err <- ReaderTimeout
			waiters++
		}
	}
	if waiters > 0 {
		log.Fine("Purged %d waiters", waiters)
	}
}

func (t *tupleSpaceImpl) SendMany(tuples []Tuple, timeout time.Duration) error {
	log.Debug("Send(%d, %s)", len(tuples), timeout)
	var expires time.Time
	if timeout != 0 {
		expires = time.Now().Add(timeout)
	}
	return t.processNewEntries(tuples, expires)
}

func (t *tupleSpaceImpl) ReadOperation(match *TupleMatcher, timeout time.Duration, actions int) ReadOperationHandle {
	waiter := newWaiter(t.cancel, match, timeout, actions)
	log.Debug("ReadOperation(%s)", waiter)
	t.processNewWaiter(waiter)
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
