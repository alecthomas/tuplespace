package tuplespace

import (
	"errors"
	"fmt"
	"time"
)

type TupleSpaceStats struct {
	Waiters int
	Tuples  int
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
	Name() string

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

	// Create a reservation on a matching tuple.
	// A reservation provides exclusive access to a tuple for a period of time
	// (the reservation timeout). During this time a reservation can be marked
	// as complete, or cancelled. If marked complete the tuple will be removed
	// from the tuplespace. If the reservation is cancelled or times out, the
	// tuple will be returned to the tuplespace.
	// Reserve(match Tuple, timeout time.Duration, reservation time.Duration) (Reservation, error)

	// Shutdown the tuplespace.
	Shutdown() error

	// Statistics for the tuplespace.
	Stats() TupleSpaceStats
}

// A tuple reservation.
// type Reservation interface {
// 	// The reserved tuple.
// 	Tuple() Tuple

// 	// Complete the reservation, removing the tuple and reservation from the server.
// 	Complete() error

// 	// Cancel the reservation, returning the associated tuple back to the tuplespace.
// 	Cancel() error
// }
