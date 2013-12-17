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

type clientReadOperationHandle struct {
	tuples chan []tuplespace.Tuple
	err    chan error
	cancel chan bool
}

func (c *clientReadOperationHandle) Cancel() {
	c.cancel <- true
}

func (c *clientReadOperationHandle) Get() chan []tuplespace.Tuple {
	return c.tuples
}

func (c *clientReadOperationHandle) Error() chan error {
	return c.err
}

type tupleSpaceClient struct {
	URL    string
	Client *http.Client
}

// NewTupleSpaceClient creates a new client for the service at url.
func NewTupleSpaceClient(url string) tuplespace.TupleSpace {
	c := &tupleSpaceClient{
		URL:    url,
		Client: &http.Client{},
	}
	return tuplespace.NewTupleSpaceHelper(c)
}

func (t *tupleSpaceClient) do(method string, req interface{}, resp interface{}) error {
	reqBytes, err := msgpack.Marshal(req)
	if err != nil {
		return err
	}
	hreq, err := http.NewRequest(method, t.URL, bytes.NewReader(reqBytes))
	if err != nil {
		return err
	}
	hreq.Header["Accept"] = []string{"application/x-msgpack"}
	hreq.Header["Content-Type"] = []string{"application/x-msgpack"}

	hresp, err := t.Client.Do(hreq)
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

func (t *tupleSpaceClient) SendMany(tuples []tuplespace.Tuple, timeout time.Duration) error {
	req := &tuplespace.SendRequest{
		Tuples:  tuples,
		Timeout: timeout,
	}
	resp := &tuplespace.SendResponse{}
	return t.do("POST", req, resp)
}

func (t *tupleSpaceClient) ReadOperation(match tuplespace.Tuple, timeout time.Duration, actions int) tuplespace.ReadOperationHandle {
	handle := &clientReadOperationHandle{
		tuples: make(chan []tuplespace.Tuple, 1),
		err:    make(chan error, 1),
		cancel: make(chan bool, 1),
	}
	method := "GET"
	if actions&tuplespace.ActionTake != 0 {
		method = "DELETE"
	}
	req := &tuplespace.ReadRequest{
		Match:   match,
		Timeout: timeout,
	}
	req.All = actions&tuplespace.ActionOne == 0
	resp := &tuplespace.ReadResponse{}
	err := t.do(method, req, resp)
	if err != nil {
		handle.err <- err
	} else {
		handle.tuples <- resp.Tuples
	}
	return handle
}

func (t *tupleSpaceClient) Shutdown() error {
	return nil
}

func (t *tupleSpaceClient) Stats() tuplespace.TupleSpaceStats {
	return tuplespace.TupleSpaceStats{}
}
