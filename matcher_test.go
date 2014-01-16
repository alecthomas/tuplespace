package tuplespace

import (
	"github.com/stretchrcom/testify/assert"
	"testing"
)

func TestMatcherMatchInt(t *testing.T) {
	tuple := Tuple{"I": 5}
	assert.True(t, MustMatch("I == 5").Match(tuple))
}

func TestMatcherBitOps(t *testing.T) {
	tuple := Tuple{"I": 3}
	assert.True(t, MustMatch("I & 2 == 2").Match(tuple))
	assert.True(t, MustMatch("I | 4 == 7").Match(tuple))
}

func TestMatchNil(t *testing.T) {
	tuple := Tuple{}
	assert.True(t, MustMatch(`I == nil`).Match(tuple))
	assert.False(t, MustMatch(`I != nil`).Match(tuple))
}

func TestMatchShortCircuit(t *testing.T) {
	tuple := Tuple{}
	assert.True(t, MustMatch(`true || false`).Match(tuple))
}

func TestMatchMap(t *testing.T) {
	tuple := Tuple{"Foo": map[string]interface{}{"Bar": "Waz"}}
	assert.True(t, MustMatch(`Foo.Bar == "Waz"`).Match(tuple))
}

func BenchmarkTupleMatching(b *testing.B) {
	tuple := Tuple{"I": 5}
	m := MustMatch("I == 5 || I == 3")
	for i := 0; i < b.N; i++ {
		m.Match(tuple)
	}
}

func BenchmarkEval(t *testing.B) {
	expr := MustMatch("(3 + 4 * 2 / (1 - 5) + 3) > 0")
	cx := map[string]interface{}{}
	for i := 0; i < t.N; i++ {
		expr.Match(cx)
	}
}
