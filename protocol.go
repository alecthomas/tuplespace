package tuplespace

import (
	"time"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type SendRequest struct {
	Tuple   Tuple         `json:"tuple" required`
	Timeout time.Duration `json:"timeout"`
}

type SendResponse struct{}

// A request sent as JSON from a client.
type ReadRequest struct {
	Match Tuple `json:"match" required`
	// Timeout for read request. Defaults to a very large period of time indeed.
	Timeout time.Duration `json:"timeout"`
	// Read all tuples.
	All bool `json:"all"`
}

type ReadResponse struct {
	Tuples []Tuple `json:"tuples"`
}
