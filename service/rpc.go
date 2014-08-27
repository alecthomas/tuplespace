package service

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/tuplespace"
)

type TupleSpace struct {
	lock               sync.Mutex
	spaces             map[string]*tuplespace.TupleSpace
	reservationCounter int64
	reservations       map[int64]*tuplespace.Reservation
}

func New() *TupleSpace {
	return &TupleSpace{
		spaces:             map[string]*tuplespace.TupleSpace{},
		reservationCounter: time.Now().UnixNano(),
		reservations:       map[int64]*tuplespace.Reservation{},
	}
}

func (t *TupleSpace) get(space string) *tuplespace.TupleSpace {
	t.lock.Lock()
	defer t.lock.Unlock()
	s, ok := t.spaces[space]
	if !ok {
		s = tuplespace.New()
		t.spaces[space] = s
	}
	return s
}

type StatusRequest struct {
	Space string
}

func (t *TupleSpace) Status(req *StatusRequest, rep *tuplespace.Status) error {
	space := t.get(req.Space)
	status := space.Status()
	*rep = *status
	return nil
}

type SendRequest struct {
	Space       string
	Tuples      []tuplespace.Tuple
	Expires     time.Duration
	Acknowledge bool
}

type SendResponse struct{}

func (t *TupleSpace) Send(req *SendRequest, rep *SendResponse) error {
	space := t.get(req.Space)
	var err error
	if req.Acknowledge {
		if len(req.Tuples) != 1 {
			err = fmt.Errorf("expected exactly one tuple to ack")
		}
		err = space.SendWithAcknowledgement(req.Tuples[0], req.Expires)
	} else {
		space.SendMany(req.Tuples, req.Expires)
	}
	*rep = struct{}{}
	return err
}

type ReadRequest struct {
	Space   string
	Match   string
	Timeout time.Duration
	All     bool
}

func (t *TupleSpace) Read(req *ReadRequest, rep *[]tuplespace.Tuple) error {
	space := t.get(req.Space)
	if req.All {
		tuples, err := space.ReadAll(req.Match, req.Timeout)
		*rep = tuples
		return err
	}
	tuple, err := space.Read(req.Match, req.Timeout)
	*rep = append(*rep, tuple)
	return err
}

type TakeRequest struct {
	Space              string
	Match              string
	Timeout            time.Duration
	ReservationTimeout time.Duration
}

type TakeResponse struct {
	Reservation int64
	Tuple       tuplespace.Tuple
}

func (t *TupleSpace) Take(req *TakeRequest, rep *TakeResponse) error {
	space := t.get(req.Space)
	res, err := space.Take(req.Match, req.Timeout, req.ReservationTimeout)
	if err != nil {
		return err
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	id := atomic.AddInt64(&t.reservationCounter, 1)
	t.reservations[id] = res
	rep.Reservation = id
	rep.Tuple = res.Tuple()
	return nil
}

type EndTakeRequest struct {
	Reservation int64
	Cancel      bool
}

type EndTakeResponse struct{}

func (t *TupleSpace) EndTake(req *EndTakeRequest, rep *EndTakeResponse) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	res, ok := t.reservations[req.Reservation]
	if !ok {
		return fmt.Errorf("unknown reservation")
	}
	delete(t.reservations, req.Reservation)
	if req.Cancel {
		return res.Cancel()
	}
	return res.Complete()
}
