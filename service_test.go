package tuplespace

import (
	"fmt"
	"testing"

	"github.com/stretchrcom/testify/assert"
)

func TestServiceDeadlock(t *testing.T) {
	path := &TupleSpacePath{Space: "test"}
	s := newServer()
	errors := make(chan error, 10)
	started := make(chan bool)
	go func() {
		started <- true
		_, err := s.Take(path, &ConsumeQuery{}, nil)
		errors <- err
	}()
	<-started
	_, err := s.Send(path, &SendRequest{
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
	_, err = s.Send(path, &SendRequest{
		Tuples: []Tuple{Tuple{}},
	})
	assert.NoError(t, err)
	assert.NoError(t, <-errors)
}

func BenchmarkServiceSend(b *testing.B) {
	s := newServer()
	defer s.Close()
	for i := 0; i < b.N; i++ {
		s.Send(&TupleSpacePath{Space: "test"}, &SendRequest{Tuples: []Tuple{Tuple{"a": 10}}})
	}
}

func benchmarkReadN(b *testing.B, n int) {
	s := newServer()
	defer s.Close()
	path := &TupleSpacePath{Space: "test"}
	for i := 0; i < n; i++ {
		s.Send(path, &SendRequest{Tuples: []Tuple{Tuple{"a": i}}})
	}
	q := fmt.Sprintf("a > %d", n/2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Read(path, &ConsumeQuery{Query: q}, nil)
	}
}

func BenchmarkServiceRead10(b *testing.B) {
	benchmarkReadN(b, 10)
}

func BenchmarkServiceRead100(b *testing.B) {
	benchmarkReadN(b, 100)
}

func BenchmarkServiceRead1000(b *testing.B) {
	benchmarkReadN(b, 1000)
}

func BenchmarkServiceRead10000(b *testing.B) {
	benchmarkReadN(b, 10000)
}

func BenchmarkServiceTake(b *testing.B) {
	s := newServer()
	defer s.Close()
	path := &TupleSpacePath{Space: "test"}
	for i := 0; i < b.N; i++ {
		s.Send(path, &SendRequest{Tuples: []Tuple{Tuple{"a": i}}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := fmt.Sprintf("a == %d", i)
		s.Take(path, &ConsumeQuery{Query: q}, nil)
	}
}
