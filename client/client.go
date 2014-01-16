// Package client is a Go client for the RESTful TupleSpace service.
package client

import (
	"bytes"
	"errors"
	"github.com/alecthomas/tuplespace"
	"github.com/vmihailenco/msgpack"
	"net/http"
	"time"
)

// TupleSpaceClient is the a Go client for the tuplespace service.
type TupleSpaceClient struct {
	url    string
	client *http.Client
}

// NewTupleSpaceClient creates a new client for the service at url.
func NewTupleSpaceClient(url string) *TupleSpaceClient {
	return &TupleSpaceClient{
		url:    url,
		client: &http.Client{},
	}
}

func (t *TupleSpaceClient) do(method string, req interface{}, resp interface{}) error {
	reqBytes, err := msgpack.Marshal(req)
	if err != nil {
		return err
	}
	hreq, err := http.NewRequest(method, t.url, bytes.NewReader(reqBytes))
	if err != nil {
		return err
	}
	hreq.Header["Accept"] = []string{"application/x-msgpack"}
	hreq.Header["Content-Type"] = []string{"application/x-msgpack"}

	hresp, err := t.client.Do(hreq)
	if hresp != nil && hresp.Body != nil {
		defer hresp.Body.Close()
	}
	if err != nil {
		return err
	}
	decoder := msgpack.NewDecoder(hresp.Body)
	if hresp.StatusCode < 200 || hresp.StatusCode > 299 {
		if hresp.StatusCode == http.StatusGatewayTimeout {
			return tuplespace.ReaderTimeout
		}
		herr := &tuplespace.ErrorResponse{}
		err := decoder.Decode(herr)
		if err != nil {
			return err
		}
		return errors.New(herr.Error)
	}

	return decoder.Decode(resp)
}

func (t *TupleSpaceClient) read(match *tuplespace.TupleMatcher, timeout time.Duration, actions int, out interface{}) error {
	method := "GET"
	if actions&tuplespace.ActionTake != 0 {
		method = "DELETE"
	}
	req := &tuplespace.ReadRequest{
		Match:   match.String(),
		Timeout: timeout,
	}
	req.All = actions&tuplespace.ActionOne == 0
	return t.do(method, req, out)
}

// Send a single tuple into the tuplespace, with an optional timeout.
// NOTE: SendMany() has much higher throughput than Send().
func (t *TupleSpaceClient) Send(tuple interface{}, timeout time.Duration) error {
	return t.SendMany([]interface{}{tuple}, timeout)
}

// SendMany tuples into the tuplespace. The "tuples" argument must be an array
// of values.
func (t *TupleSpaceClient) SendMany(tuples interface{}, timeout time.Duration) error {
	req := &tuplespace.SendRequest{
		Tuples:  tuples,
		Timeout: timeout,
	}
	resp := &tuplespace.SendResponse{}
	return t.do("POST", req, resp)
}

// Read a tuple from the tuplespace, with an optional timeout.
func (t *TupleSpaceClient) Read(match string, timeout time.Duration, tuple interface{}) error {
	m, err := tuplespace.Match(match)
	if err != nil {
		return err
	}
	out := []interface{}{tuple}
	return t.read(m, timeout, tuplespace.ActionOne, &out)
}

// ReadAll matching tuples from the tuplespace. The "tuples" argument must be
// an array of values.
func (t *TupleSpaceClient) ReadAll(match string, timeout time.Duration, tuples interface{}) error {
	m, err := tuplespace.Match(match)
	if err != nil {
		return err
	}
	return t.read(m, timeout, 0, tuples)
}

// Take (read and remove) a tuple from the tuplespace, with an optional
// timeout.
func (t *TupleSpaceClient) Take(match string, timeout time.Duration, tuple interface{}) error {
	m, err := tuplespace.Match(match)
	if err != nil {
		return err
	}
	out := []interface{}{tuple}
	return t.read(m, timeout, tuplespace.ActionOne|tuplespace.ActionTake, &out)
}

// TakeAll (read and remove) tuples from the tuplespace, with an optional
// timeout. The "tuples" argument must be an array of values.
func (t *TupleSpaceClient) TakeAll(match string, timeout time.Duration, tuples interface{}) error {
	m, err := tuplespace.Match(match)
	if err != nil {
		return err
	}
	return t.read(m, timeout, tuplespace.ActionTake, tuples)
}
