package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/alecthomas/tuplespace"
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
	url    string
	client *http.Client
}

func NewTupleSpaceClient(url string) tuplespace.TupleSpace {
	return &tupleSpaceClient{
		url:    url,
		client: &http.Client{},
	}
}

func (t *tupleSpaceClient) do(method string, req interface{}, resp interface{}) error {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return err
	}
	hreq, err := http.NewRequest(method, t.url, bytes.NewReader(reqBytes))
	if err != nil {
		return err
	}
	hreq.Header["Accept"] = []string{"application/json"}
	hreq.Header["Content-Type"] = []string{"application/json"}

	hresp, err := t.client.Do(hreq)
	if hresp != nil && hresp.Body != nil {
		defer hresp.Body.Close()
	}
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(hresp.Body)
	if hresp.StatusCode < 200 && hresp.StatusCode > 299 {
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

func (t *tupleSpaceClient) Send(tuples []tuplespace.Tuple, timeout time.Duration) error {
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
	req.All = (actions&tuplespace.ActionOne == 0)
	resp := &tuplespace.ReadResponse{}
	err := t.do(method, req, resp)
	if err != nil {
		handle.err <- err
	} else {
		handle.tuples <- resp.Tuples
	}
	return handle
}

func (t *tupleSpaceClient) Read(match tuplespace.Tuple, timeout time.Duration) (r tuplespace.Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, tuplespace.ActionOne)
	select {
	case err = <-waiter.Error():
		return
	case matches := <-waiter.Get():
		r = matches[0]
		return
	}
}

func (t *tupleSpaceClient) ReadAll(match tuplespace.Tuple, timeout time.Duration) (r []tuplespace.Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, 0)
	select {
	case err = <-waiter.Error():
		return
	case r = <-waiter.Get():
		return
	}
}

func (t *tupleSpaceClient) Take(match tuplespace.Tuple, timeout time.Duration) (r tuplespace.Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, tuplespace.ActionOne|tuplespace.ActionTake)
	select {
	case err = <-waiter.Error():
		return
	case matches := <-waiter.Get():
		r = matches[0]
		return
	}
}

func (t *tupleSpaceClient) TakeAll(match tuplespace.Tuple, timeout time.Duration) (r []tuplespace.Tuple, err error) {
	waiter := t.ReadOperation(match, timeout, tuplespace.ActionTake)
	select {
	case err = <-waiter.Error():
		return
	case r = <-waiter.Get():
		return
	}
}

func (t *tupleSpaceClient) Shutdown() error {
	return nil
}

func (t *tupleSpaceClient) Stats() tuplespace.TupleSpaceStats {
	return tuplespace.TupleSpaceStats{}
}
