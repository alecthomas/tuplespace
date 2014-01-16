package tuplespace

import (
	"time"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type SendRequest struct {
	Tuples  []Tuple       `json:"tuples"`
	Timeout time.Duration `json:"timeout"`
}

type SendResponse struct{}

// A ReadRequest sent as JSON from a client.
type ReadRequest struct {
	Match string `json:"match"`
	// Timeout for read request. Defaults to a very large period of time indeed.
	Timeout time.Duration `json:"timeout"`
	// Read all tuples.
	All bool `json:"all"`
}

type ReadResponse struct {
	Tuples []Tuple `json:"tuples"`
}
