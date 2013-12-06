package tuplespace

import (
	"time"
)

type tupleSpaceHelper struct {
	RawTupleSpace
}

// NewTupleSpace creates a new tuple store using the given storage backend.
func NewTupleSpace(store TupleStore) TupleSpace {
	return NewTupleSpaceHelper(NewRawTupleSpace(store))
}

// NewTupleSpaceHelper implements convenience functions on top of a
// RawTupleSpace implementation.
func NewTupleSpaceHelper(space RawTupleSpace) TupleSpace {
	return &tupleSpaceHelper{space}
}

func (t *tupleSpaceHelper) SendMany(tuples []Tuple, timeout time.Duration) error {
	return t.SendMany(tuples, timeout)
}

func (t *tupleSpaceHelper) ReadOperation(match Tuple, timeout time.Duration, actions int) ReadOperationHandle {
	return t.ReadOperation(match, timeout, actions)
}

func (t *tupleSpaceHelper) Shutdown() error {
	return t.Shutdown()
}

func (t *tupleSpaceHelper) Send(tuple Tuple, timeout time.Duration) error {
	return t.SendMany([]Tuple{tuple}, timeout)
}

func (t *tupleSpaceHelper) Read(match Tuple, timeout time.Duration) (r Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, ActionOne)
	select {
	case err = <-waiter.Error():
		return
	case matches := <-waiter.Get():
		r = matches[0]
		return
	}
}

func (t *tupleSpaceHelper) ReadAll(match Tuple, timeout time.Duration) (r []Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, 0)
	select {
	case err = <-waiter.Error():
		return
	case r = <-waiter.Get():
		return
	}
}

func (t *tupleSpaceHelper) Take(match Tuple, timeout time.Duration) (r Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, ActionOne|ActionTake)
	select {
	case err = <-waiter.Error():
		return
	case matches := <-waiter.Get():
		r = matches[0]
		return
	}
}

func (t *tupleSpaceHelper) TakeAll(match Tuple, timeout time.Duration) (r []Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, ActionTake)
	select {
	case err = <-waiter.Error():
		return
	case r = <-waiter.Get():
		return
	}
}
