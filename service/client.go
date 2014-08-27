package service

import (
	"time"

	"github.com/alecthomas/tuplespace"
)

type Client struct {
	c *tupleSpaceClient
}

func Dial(url string) (*Client, error) {
	c, err := dialTupleSpace(url)
	if err != nil {
		return nil, err
	}
	return &Client{
		c: c,
	}, nil
}

func (c *Client) Space(space string) *ClientSpace {
	return &ClientSpace{c: c.c, space: space}
}

func (c *Client) Close() error {
	return nil
}

type ClientSpace struct {
	c     *tupleSpaceClient
	space string
}

func (c *ClientSpace) Close() error {
	return nil
}

func (c *ClientSpace) Status() (*tuplespace.Status, error) {
	return c.c.SpaceStatus(c.space)
}

func (c *ClientSpace) Send(tuple tuplespace.Tuple, expires time.Duration) error {
	return c.c.Send(c.space, &SendRequest{
		Tuples:  []tuplespace.Tuple{tuple},
		Expires: expires,
	})
}

func (c *ClientSpace) SendMany(tuples []tuplespace.Tuple, expires time.Duration) error {
	return c.c.Send(c.space, &SendRequest{
		Tuples:  tuples,
		Expires: expires,
	})
}

func (c *ClientSpace) SendWithAcknowledgement(tuple tuplespace.Tuple, expires time.Duration) error {
	return c.c.Send(c.space, &SendRequest{
		Tuples:      []tuplespace.Tuple{tuple},
		Expires:     expires,
		Acknowledge: true,
	})
}

func (c *ClientSpace) Read(match string, timeout time.Duration) (tuplespace.Tuple, error) {
	tuples, err := c.c.Read(c.space, &ReadQuery{
		Query:   match,
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}
	return tuples[0], nil
}

func (c *ClientSpace) ReadAll(match string, timeout time.Duration) ([]tuplespace.Tuple, error) {
	return c.c.Read(c.space, &ReadQuery{
		Query:   match,
		Timeout: timeout,
		All:     true,
	})
}

func (c *ClientSpace) Take(match string, timeout time.Duration, reservationTimeout time.Duration) (*ClientReservation, error) {
	r, err := c.c.Take(c.space, &TakeRequest{
		Query:              match,
		Timeout:            timeout,
		ReservationTimeout: reservationTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &ClientReservation{
		c:     c.c,
		space: c.space,
		id:    r.ID,
		tuple: r.Tuple,
	}, nil
}

type ClientReservation struct {
	c     *tupleSpaceClient
	space string
	id    int64
	tuple tuplespace.Tuple
}

func (c *ClientReservation) Tuple() tuplespace.Tuple {
	return c.tuple
}

func (c *ClientReservation) Complete() error {
	return c.c.EndReservation(c.space, c.id, &EndReservationRequest{})
}

func (c *ClientReservation) Cancel() error {
	return c.c.EndReservation(c.space, c.id, &EndReservationRequest{Cancel: true})
}
