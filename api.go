package tuplespace

import (
	"errors"
	"fmt"
	"time"
)

// A TupleEntry represents a stored tuple.
type TupleEntry struct {
	ID      uint64
	Tuple   Tuple
	Timeout time.Time
}

// IsExpired returns whether the entry has expired.
func (t *TupleEntry) IsExpired(now time.Time) bool {
	return !t.Timeout.IsZero() && now.After(t.Timeout)
}

// A TupleStore handles efficient storage and retrieval of tuples.
//
// - Implementations MUST purge expired entries.
// - Implementations MUST be concurrency safe.
type TupleStore interface {
	// Put tuples into the store.
	Put(tuple []Tuple, timeout time.Time) error
	// Match retrieves tuples from the store that match "match". This MUST NOT
	// return expired tuples.
	Match(match Tuple, limit int) ([]*TupleEntry, error)
	// Delete a set of entries in the store.
	Delete(entries []*TupleEntry) error
	// Shutdown the store.
	Shutdown()
	// Update statistics.
	UpdateStats(stats *TupleSpaceStats)
}

// TupleSpaceStats contains statistics on the tuplespace.
type TupleSpaceStats struct {
	Waiters     int64 `json:"waiters"`      // Number of current waiters.
	Tuples      int64 `json:"tuples"`       // Number of unexpired tuples.
	WaitersSeen int64 `json:"waiters_seen"` // Number of waiters seen since service start.
	TuplesSeen  int64 `json:"tuples_seen"`  // Number of tuples seen since service start.
	TuplesTaken int64 `json:"tuples_taken"` // Number of tuples taken since service start.
	TuplesRead  int64 `json:"tuples_read"`  // Number of tuples read since service start.
}

func (t TupleSpaceStats) String() string {
	return fmt.Sprintf("Waiters: %d, Tuples: %d", t.Waiters, t.Tuples)
}

const (
	// ActionTake tells the tuplespace to remove a tuple when matched.
	ActionTake = 1
	// ActionOne tells the tuplespace to stop matching after the first match.
	ActionOne = 2
)

var (
	// ReaderTimeout indicates a timeout on a read operation expired before
	// matching any tuples.
	ReaderTimeout = errors.New("reader timeout")
	// CancelledReader is returned when a reader is cancelled via ReadOperationHandle.Cancel().
	CancelledReader = errors.New("cancelled reader")
)

// ReadOperationHandle provides asynchronous access to read results.
type ReadOperationHandle interface {
	Cancel()
	Get() chan []Tuple
	Error() chan error
}

// RawTupleSpace provides the fundamental operations on a tuple space.
type RawTupleSpace interface {
	// SendMany tuples into the tuplespace, with an optional timeout.
	SendMany(tuples []Tuple, timeout time.Duration) error

	// Raw read operation. Actions should be a bitfield of Action* constants.
	//
	// Generally used like so:
	//
	// 		handle := ts.ReadOperation([]Tuple{"cmd", nil}, 0, ActionTake|ActionOne)
	// 		select {
	// 			case tuples := <-handle.Get():
	// 				...
	// 			case err := <-handle.Error():
	// 				...
	// 		}
	ReadOperation(match Tuple, timeout time.Duration, actions int) ReadOperationHandle

	// Shutdown the tuplespace.
	Shutdown() error

	// Stats returns statistics on the state of the TupleSpace.
	Stats() TupleSpaceStats
}

// TupleSpaceHelper provides convenience methods for a TupleSpace on top of
// the raw operations provided by RawTupleSpace.
type TupleSpaceHelper interface {
	// Send a single tuple into the tuplespace, with an optional timeout.
	// NOTE: SendMany() has much higher throughput than Send().
	Send(tuple Tuple, timeout time.Duration) error

	// Read a tuple from the tuplespace, with an optional timeout.
	Read(match Tuple, timeout time.Duration) (Tuple, error)

	// Read all matching tuples from the tuplespace.
	ReadAll(match Tuple, timeout time.Duration) ([]Tuple, error)

	// Take (read and remove) a tuple from the tuplespace, with an optional timeout.
	Take(match Tuple, timeout time.Duration) (Tuple, error)

	// Take (read and remove) all tuples from the tuplespace, with an optional timeout.
	TakeAll(match Tuple, timeout time.Duration) ([]Tuple, error)
}

// A TupleSpace provides a shared space for delivering and receiving tuples.
type TupleSpace interface {
	RawTupleSpace
	TupleSpaceHelper
}
