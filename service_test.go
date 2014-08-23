package tuplespace

import (
	"testing"

	"github.com/stretchrcom/testify/assert"
)

func TestServiceDeadlock(t *testing.T) {
	path := &TupleSpacePath{SpaceID: "test"}
	s := newServer()
	errors := make(chan error, 10)
	started := make(chan bool)
	go func() {
		started <- true
		_, err := s.Take(path, &ConsumeQuery{}, nil)
		errors <- err
	}()
	<-started
	err := s.Send(path, &SendRequest{
		Tuples: []Tuple{Tuple{}},
	})
	assert.NoError(t, err)
	assert.NoError(t, <-errors)
	go func() {
		started <- true
		_, err := s.Take(path, &ConsumeQuery{}, nil)
		errors <- err
	}()
	<-started
	err = s.Send(path, &SendRequest{
		Tuples: []Tuple{Tuple{}},
	})
	assert.NoError(t, err)
	assert.NoError(t, <-errors)
}
