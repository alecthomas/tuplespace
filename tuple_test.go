package tuplespace_test

import (
	"github.com/alecthomas/tuplespace"
	"github.com/stretchrcom/testify/assert"
	"testing"
)

func TestTupleMatch(t *testing.T) {
	tuple := tuplespace.Tuple{"cmd", "uname -a"}
	match := tuplespace.Tuple{"cmd", nil}
	assert.True(t, match.Match(tuple))
}

func TestTupleDoesNotMatch(t *testing.T) {
	tuple := tuplespace.Tuple{"cmd", "uname -a"}
	match := tuplespace.Tuple{"not", nil}
	assert.False(t, match.Match(tuple))
}

func TestTupleTypeComparisonFails(t *testing.T) {
	tuple := tuplespace.Tuple{int32(10)}
	match := tuplespace.Tuple{string("foo")}
	assert.False(t, match.Match(tuple))
}

func BenchmarkTupleComparison(b *testing.B) {
	tuple := tuplespace.Tuple{10, "hello", "goodbye", 1.5}
	match := tuplespace.Tuple{10, nil, "goodbye", "love"}
	for i := 0; i < b.N; i++ {
		match.Match(tuple)
	}
}
