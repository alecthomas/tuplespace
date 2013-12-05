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

// A TupleStore handles efficient storage and retrieval of tuples.
// The store MUST purge expired entries.
type TupleStore interface {
	// Put a tuple into the store.
	Put(tuple Tuple, timeout time.Time) error
	// Match retrieves tuples from the store that match "match". This MUST NOT
	// return expired tuples.
	Match(match Tuple) ([]*TupleEntry, error)
	// Delete an entry in the store. "id" is TupleEntry.ID.
	Delete(id uint64) error
	// Shutdown the store.
	Shutdown()
}

// TupleSpaceStats contains statistics on the tuplespace.
type TupleSpaceStats struct {
	Waiters int `json:"waiters"`
	Tuples  int `json:"tuples"`
}

func (t *TupleSpaceStats) String() string {
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

type ReadOperationHandle interface {
	Cancel()
	Get() chan []Tuple
	Error() chan error
}

// A TupleSpace provides a shared space for delivering and receiving tuples.
type TupleSpace interface {
	// Send a tuple into the tuplespace, with an optional timeout.
	Send(tuple Tuple, timeout time.Duration) error

	// Raw read operation. Actions should be a bitfield of Action* constants.
	ReadOperation(match Tuple, timeout time.Duration, actions int) ReadOperationHandle

	// Read a tuple from the tuplespace, with an optional timeout.
	Read(match Tuple, timeout time.Duration) (Tuple, error)

	// Read all matching tuples from the tuplespace.
	ReadAll(match Tuple, timeout time.Duration) ([]Tuple, error)

	// Take (read and remove) a tuple from the tuplespace, with an optional timeout.
	Take(match Tuple, timeout time.Duration) (Tuple, error)

	// Take (read and remove) all tuples from the tuplespace, with an optional timeout.
	TakeAll(match Tuple, timeout time.Duration) ([]Tuple, error)

	// Shutdown the tuplespace.
	Shutdown() error

	// Statistics for the tuplespace.
	Stats() TupleSpaceStats
}
