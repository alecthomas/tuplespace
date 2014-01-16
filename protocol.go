package tuplespace

import (
	"time"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type SendRequest struct {
	Tuples  interface{}   `json:"tuples"`
	Timeout time.Duration `json:"timeout"`
}

type SendResponse struct{}

// A ReadRequest sent as JSON from a client.
type ReadRequest struct {
	Match   string        `json:"match"`   // Match expression.
	Timeout time.Duration `json:"timeout"` // Timeout for read request. Defaults to a very large period of time indeed.
	All     bool          `json:"all"`     // Read all tuples.
}

type ReadResponse struct {
	Tuples interface{} `json:"tuples"`
}
