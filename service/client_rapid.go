package service

import (
	"github.com/alecthomas/rapid"
	"github.com/alecthomas/tuplespace"
)

type tupleSpaceClient struct {
	c rapid.Client
}

func dialTupleSpace(url string) (*tupleSpaceClient, error) {
	c, err := rapid.Dial(url)
	if err != nil {
		return nil, err
	}
	return &tupleSpaceClient{c}, nil
}

func newTupleSpaceClient(client rapid.Client) *tupleSpaceClient {
	return &tupleSpaceClient{client}
}

// ListSpaces - List tuple spaces.
func (a *tupleSpaceClient) ListSpaces() ([]string, error) {
	resp := []string{}
	r := rapid.Request("GET", "/tuplespaces").Build()
	err := a.c.Do(r, &resp)
	return resp, err
}

// SpaceStatus - Return status of tuple space.
func (a *tupleSpaceClient) SpaceStatus(space string) (*tuplespace.Status, error) {
	resp := &tuplespace.Status{}
	r := rapid.Request("GET", "/tuplespaces/{space}", space).Build()
	err := a.c.Do(r, resp)
	return resp, err
}

// DeleteSpace - Delete a tuple space.
func (a *tupleSpaceClient) DeleteSpace(space string) error {
	r := rapid.Request("DELETE", "/tuplespaces/{space}", space).Build()
	err := a.c.Do(r, nil)
	return err
}

// Send - Send a tuple to the space.
func (a *tupleSpaceClient) Send(space string, req *SendRequest) error {
	r := rapid.Request("POST", "/tuplespaces/{space}/tuples", space).Body(req).Build()
	err := a.c.Do(r, nil)
	return err
}

// Read - Read tuples from the space.
func (a *tupleSpaceClient) Read(space string, query *ReadQuery) ([]tuplespace.Tuple, error) {
	resp := []tuplespace.Tuple{}
	r := rapid.Request("GET", "/tuplespaces/{space}/tuples", space).Query(query).Build()
	err := a.c.Do(r, &resp)
	return resp, err
}

// Take - Take a tuple within a reservation.
func (a *tupleSpaceClient) Take(space string, req *TakeRequest) (*ReservationResponse, error) {
	resp := &ReservationResponse{}
	r := rapid.Request("POST", "/tuplespaces/{space}/reservations", space).Body(req).Build()
	err := a.c.Do(r, resp)
	return resp, err
}

// EndReservation - Finish a reservation.
func (a *tupleSpaceClient) EndReservation(space string, reservation int64, req *EndReservationRequest) error {
	r := rapid.Request("DELETE", "/tuplespaces/{space}/reservations/{reservation}", space, reservation).Body(req).Build()
	err := a.c.Do(r, nil)
	return err
}
