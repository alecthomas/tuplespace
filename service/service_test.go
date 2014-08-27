package service

import (
	"fmt"
	"testing"

	"github.com/stretchrcom/testify/assert"

	"github.com/alecthomas/tuplespace"
)

func TestServiceDeadlock(t *testing.T) {
	path := &TupleSpacePath{Space: "test"}
	s := newServer()
	errors := make(chan error, 10)
	started := make(chan bool)
	go func() {
		started <- true
		_, err := s.Take(path, &TakeRequest{}, nil)
		errors <- err
	}()
	<-started
	err := s.Send(path, &SendRequest{
		Tuples: []tuplespace.Tuple{tuplespace.Tuple{}},
	})
	assert.NoError(t, err)
	assert.NoError(t, <-errors)
	go func() {
		started <- true
		_, err := s.Take(path, &TakeRequest{}, nil)
		errors <- err
	}()
	<-started
	err = s.Send(path, &SendRequest{
		Tuples: []tuplespace.Tuple{tuplespace.Tuple{}},
	})
	assert.NoError(t, err)
	assert.NoError(t, <-errors)
}

func BenchmarkServiceSend(b *testing.B) {
	s := newServer()
	defer s.Close()
	for i := 0; i < b.N; i++ {
		s.Send(&TupleSpacePath{Space: "test"}, &SendRequest{Tuples: []tuplespace.Tuple{tuplespace.Tuple{"a": 10}}})
	}
}

func benchmarkReadN(b *testing.B, n int) {
	s := newServer()
	defer s.Close()
	path := &TupleSpacePath{Space: "test"}
	for i := 0; i < n; i++ {
		s.Send(path, &SendRequest{Tuples: []tuplespace.Tuple{tuplespace.Tuple{"a": i}}})
	}
	q := fmt.Sprintf("a > %d", n/2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Read(path, &ReadQuery{Query: q}, nil)
	}
	b.StopTimer()
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
		s.Send(path, &SendRequest{Tuples: []tuplespace.Tuple{tuplespace.Tuple{"a": i}}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// q := fmt.Sprintf("a == %d", i)
		r, err := s.Take(path, &TakeRequest{Query: ""}, nil)
		if err != nil {
			panic(err)
		}
		rpath := &ReservationPath{Space: path.Space, Reservation: r.ID}
		err = s.EndReservation(rpath, &EndReservationRequest{})
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}
