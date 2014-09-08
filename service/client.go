package service

import (
	"net"
	"net/rpc"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/alecthomas/tuplespace"
)

type Client struct {
	addr    string
	session *yamux.Session
}

// Dial creats a new connection to a TupleSpace server.
func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	session, err := yamux.Client(conn, nil)
	if err != nil {
		return nil, err
	}
	return &Client{addr: conn.RemoteAddr().String(), session: session}, nil
}

func (c *Client) RemoteAddr() string {
	return c.addr
}

func (c *Client) Close() error {
	return c.session.Close()
}

// Space creates a new multiplexed connection to the given space. This is a
// low-overhead operation, but not without cost.
func (c *Client) Space(space string) (*ClientSpace, error) {
	conn, err := c.session.Open()
	if err != nil {
		return nil, err
	}
	return HijackConn(c.addr, space, conn), nil
}

type ClientSpace struct {
	addr  string
	space string
	rpc   *rpc.Client
}

// HijackConn starts a new RPC session for a tuple space, on an existing net.Conn.
func HijackConn(addr, space string, conn net.Conn) *ClientSpace {
	return &ClientSpace{addr: addr, rpc: rpc.NewClient(conn), space: space}
}

func (c *ClientSpace) RemoteAddr() string {
	return c.addr
}

func (c *ClientSpace) Name() string {
	return c.space
}

func (c *ClientSpace) Close() error {
	return c.rpc.Close()
}

func (c *ClientSpace) Status() (*tuplespace.Status, error) {
	req := &StatusRequest{
		Space: c.space,
	}
	rep := &tuplespace.Status{}
	return rep, c.rpc.Call("TupleSpace.Status", req, rep)
}

func (c *ClientSpace) send(ack bool, tuples []tuplespace.Tuple, expires time.Duration) error {
	req := &SendRequest{
		Space:       c.space,
		Tuples:      tuples,
		Expires:     expires,
		Acknowledge: ack,
	}
	rep := &SendResponse{}
	return c.rpc.Call("TupleSpace.Send", req, rep)
}

func (c *ClientSpace) Send(tuple tuplespace.Tuple, expires time.Duration) error {
	return c.send(false, []tuplespace.Tuple{tuple}, expires)
}

func (c *ClientSpace) SendMany(tuples []tuplespace.Tuple, expires time.Duration) error {
	return c.send(false, tuples, expires)
}

func (c *ClientSpace) SendWithAcknowledgement(tuple tuplespace.Tuple, expires time.Duration) error {
	return c.send(true, []tuplespace.Tuple{tuple}, expires)
}

func (c *ClientSpace) read(all bool, match string, timeout time.Duration) ([]tuplespace.Tuple, error) {
	req := &ReadRequest{
		Space:   c.space,
		Match:   match,
		Timeout: timeout,
		All:     all,
	}
	rep := []tuplespace.Tuple{}
	err := c.rpc.Call("TupleSpace.Read", req, &rep)
	return rep, err
}

func (c *ClientSpace) Read(match string, timeout time.Duration) (tuplespace.Tuple, error) {
	tuples, err := c.read(false, match, timeout)
	if err != nil {
		return nil, err
	}
	return tuples[0], nil
}

func (c *ClientSpace) ReadAll(match string, timeout time.Duration) ([]tuplespace.Tuple, error) {
	return c.read(true, match, timeout)
}

func (c *ClientSpace) Take(match string, timeout time.Duration, reservationTimeout time.Duration) (*ClientReservation, error) {
	req := &TakeRequest{
		Space:              c.space,
		Match:              match,
		Timeout:            timeout,
		ReservationTimeout: reservationTimeout,
	}
	rep := &TakeResponse{}
	err := c.rpc.Call("TupleSpace.Take", req, rep)
	if err != nil {
		return nil, err
	}
	return &ClientReservation{
		rpc:   c.rpc,
		tuple: rep.Tuple,
		id:    rep.Reservation,
	}, nil
}

type ClientReservation struct {
	rpc   *rpc.Client
	tuple tuplespace.Tuple
	id    int64
}

func (c *ClientReservation) Tuple() tuplespace.Tuple {
	return c.tuple
}

func (c *ClientReservation) Complete() error {
	return c.end(false)
}

func (c *ClientReservation) Cancel() error {
	return c.end(true)
}

func (c *ClientReservation) end(cancel bool) error {
	req := &EndTakeRequest{
		Reservation: c.id,
		Cancel:      cancel,
	}
	rep := &EndTakeResponse{}
	return c.rpc.Call("TupleSpace.EndTake", req, rep)
}
