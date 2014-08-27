package service

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/go-logging"
	"github.com/alecthomas/rapid"
	"github.com/alecthomas/rapid/schema"

	"github.com/alecthomas/tuplespace"
)

var (
	log            = logging.MustGetLogger("tuplespace")
	spaceNameRegex = regexp.MustCompile(`[.\w]+`)
)

type TupleSpacePath struct {
	Space string `schema:"space"`
}

func (t *TupleSpacePath) Validate() error {
	if !spaceNameRegex.MatchString(t.Space) {
		return errors.New("space name must match " + spaceNameRegex.String())
	}
	return nil
}

type SendRequest struct {
	Tuples      []tuplespace.Tuple `json:"tuples"`
	Expires     time.Duration      `json:"expires,omitempty"`
	Acknowledge bool               `json:"ack"`
}

func (s *SendRequest) Validate() error {
	if len(s.Tuples) == 0 {
		return errors.New("need a tuple")
	}
	if len(s.Tuples) > 1 && s.Acknowledge {
		return errors.New("can't acknowledge more than one tuple")
	}
	return nil
}

type ReadQuery struct {
	Query   string        `schema:"q"`
	Timeout time.Duration `schema:"timeout"`
	All     bool          `schema:"all"`
}

func (c *ReadQuery) Validate() error {
	return nil
}

type TakeRequest struct {
	Query              string        `json:"q"`
	Timeout            time.Duration `json:"timeout"`
	ReservationTimeout time.Duration `json:"reservation_timeout"`
}

func (r *TakeRequest) Validate() error {
	if r.ReservationTimeout < 0 || r.ReservationTimeout > tuplespace.MaxReservationTimeout {
		return fmt.Errorf("reservation timeout outside acceptable range %s < n < %s", time.Second*0, tuplespace.MaxReservationTimeout)
	}
	return nil
}

type ReservationPath struct {
	Space       string `schema:"space"`
	Reservation int64  `schema:"reservation"`
}

type ReservationResponse struct {
	ID    int64            `json:"id"`
	Tuple tuplespace.Tuple `json:"tuple"`
}

type EndReservationRequest struct {
	Cancel bool `json:"cancel"`
}

// Definition returns the service definition for the TupleSpace RESTful service.
//
// The TupleSpace service provides namespaced tuplespaces. Spaces are created on demand.
//
// 		GET /tuplespaces -> list tuplespaces
// 		GET /tuplespaces/{space} -> status of tuplespace
//		DELETE /tuplespaces/{space} -> delete tuplespace
//
// 		POST /tuplespaces/{space}/tuples -> send tuple to tuplespace {space}
// 		GET /tuplespaces/{space}/tuples?q={expr}&timeout={timeout} -> read tuple matching {expr}
//
// 		POST /tuplespaces/{space}/tuples/reservations -> create a tuple reservation
// 		DELETE /tuplespaces/{space}/tuples/reservations/{reservation} -> complete or cancel a reservation
//
func Definition() *schema.Schema {
	d := rapid.Define("TupleSpace")

	tuplespaces := d.Resource("TupleSpaces", "/tuplespaces").Description("Manage tuple spaces.")
	tuplespaces.Route("ListSpaces", "/tuplespaces").
		Description("List tuple spaces.").
		Get().
		Response(http.StatusOK, []string{})
	tuplespaces.Route("SpaceStatus", "/tuplespaces/{space:[.\\w]+}").
		Description("Return status of tuple space.").
		Get().
		Path(&TupleSpacePath{}).
		Response(http.StatusOK, &tuplespace.Status{})
	tuplespaces.Route("DeleteSpace", "/tuplespaces/{space:[.\\w]+}").
		Description("Delete a tuple space.").
		Delete().
		Path(&TupleSpacePath{}).
		Response(http.StatusOK, nil).
		Response(http.StatusNotFound, nil)

	tuples := d.Resource("Tuples", "/tuplespaces/{space:[.\\w]+}/tuples")
	tuples.Route("Send", "/tuplespaces/{space:[.\\w]+}/tuples").
		Description("Send a tuple to the space.").
		Post().
		Request(&SendRequest{}).
		Path(&TupleSpacePath{}).
		Response(http.StatusCreated, nil).
		Response(http.StatusBadRequest, nil).
		Response(http.StatusGatewayTimeout, nil).
		Response(http.StatusInternalServerError, nil)
	tuples.Route("Read", "/tuplespaces/{space:[.\\w]+}/tuples").
		Description("Read a tuple from the space.").
		Get().
		Path(&TupleSpacePath{}).
		Query(&ReadQuery{}).
		Response(http.StatusOK, tuplespace.Tuple{}).
		Response(http.StatusGatewayTimeout, nil)

	reservations := d.Resource("Reservations", "/tuplespaces/{space:[.\\w]+}/reservations")
	reservations.Route("Take", "/tuplespaces/{space:[.\\w]+}/reservations").
		Description("Take a tuple within a reservation.").
		Post().
		Path(&TupleSpacePath{}).
		Request(&TakeRequest{}).
		Response(http.StatusCreated, &ReservationResponse{})
	reservations.Route("EndReservation", "/tuplespaces/{space:[.\\w]+}/reservations/{reservation:\\d+}").
		Description("Finish a reservation.").
		Delete().
		Path(&ReservationPath{}).
		Request(&EndReservationRequest{}).
		Response(http.StatusOK, nil)

	return d.Build()
}

func Server() (*rapid.Server, error) {
	server, err := rapid.NewServer(Definition(), newServer())
	if err != nil {
		return nil, err
	}
	server.SetLogger(log)
	return server, nil
}

type server struct {
	lock          sync.Mutex
	spaces        map[string]*tuplespace.TupleSpace
	reservations  map[int64]*tuplespace.Reservation
	reservationID int64
}

func newServer() *server {
	return &server{
		spaces:        map[string]*tuplespace.TupleSpace{},
		reservations:  map[int64]*tuplespace.Reservation{},
		reservationID: time.Now().UnixNano(),
	}
}

func (s *server) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, r := range s.reservations {
		_ = r.Cancel()
	}
	s.reservations = map[int64]*tuplespace.Reservation{}
	for _, space := range s.spaces {
		_ = space.Close()
	}
	s.spaces = map[string]*tuplespace.TupleSpace{}
	return nil
}

func (s *server) ListSpaces() ([]string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	spaces := make([]string, 0, len(s.spaces))
	for name := range s.spaces {
		spaces = append(spaces, name)
	}
	return spaces, nil
}

// Atomically retrieve an existing space, or create a new one.
func (s *server) getOrCreate(name string) *tuplespace.TupleSpace {
	s.lock.Lock()
	defer s.lock.Unlock()
	if space, ok := s.spaces[name]; ok {
		return space
	}
	space := tuplespace.New()
	s.spaces[name] = space
	return space
}

func (s *server) SpaceStatus(path *TupleSpacePath) (*tuplespace.Status, error) {
	return s.getOrCreate(path.Space).Status(), nil
}

func (s *server) DeleteSpace(path *TupleSpacePath) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	space, ok := s.spaces[path.Space]
	if !ok {
		return rapid.Error(http.StatusNotFound, "space not found")
	}
	err := space.Close()
	delete(s.spaces, path.Space)
	if err != nil {
		return rapid.Error(http.StatusInternalServerError, err.Error())
	}
	return nil
}

func (s *server) Send(path *TupleSpacePath, req *SendRequest) error {
	space := s.getOrCreate(path.Space)
	var err error
	if req.Acknowledge {
		err = space.SendWithAcknowledgement(req.Tuples[0], req.Expires)
		if err == nil {
			return nil
		}
	} else {
		for _, tuple := range req.Tuples {
			space.Send(tuple, req.Expires)
		}
	}
	if err == tuplespace.ErrTimeout {
		return rapid.Error(http.StatusGatewayTimeout, err.Error())
	} else if err != nil {
		return rapid.Error(http.StatusInternalServerError, err.Error())
	}
	return nil
}

func (s *server) Read(path *TupleSpacePath, query *ReadQuery, cancel rapid.CloseNotifierChannel) ([]tuplespace.Tuple, error) {
	space := s.getOrCreate(path.Space)
	tuples, err := space.Consume(&tuplespace.ConsumeOptions{
		Match:   query.Query,
		Timeout: query.Timeout,
		All:     query.All,
		Cancel:  cancel,
	})
	if err != nil {
		return nil, rapid.Error(http.StatusInternalServerError, err.Error())
	}
	return tuples, nil
}

func (s *server) Take(path *TupleSpacePath, req *TakeRequest, cancel rapid.CloseNotifierChannel) (*ReservationResponse, error) {
	space := s.getOrCreate(path.Space)
	reservation, err := space.TakeWithCancel(req.Query, req.Timeout, req.ReservationTimeout, cancel)
	if err != nil {
		return nil, err
	}
	resp := &ReservationResponse{
		ID:    atomic.AddInt64(&s.reservationID, 1),
		Tuple: reservation.Tuple(),
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	s.reservations[resp.ID] = reservation
	return resp, nil
}

func (s *server) EndReservation(path *ReservationPath, req *EndReservationRequest) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	reservation, ok := s.reservations[path.Reservation]
	if !ok {
		return rapid.Error(http.StatusNotFound, "reservation not found")
	}
	defer delete(s.reservations, path.Reservation)
	if req.Cancel {
		return reservation.Cancel()
	}
	return reservation.Complete()
}
