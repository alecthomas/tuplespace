package tuplespace

import (
	"fmt"
	"reflect"
	"strings"
)

// A Tuple represents a stored tuple.
type Tuple []interface{}

func (t Tuple) String() string {
	tuple := []string{}
	for _, e := range t {
		tuple = append(tuple, fmt.Sprintf("%#v", e))
	}
	return fmt.Sprintf("(%s)", strings.Join(tuple, ", "))
}

func equal(a, b interface{}) (truth bool) {
	defer func() {
		if r := recover(); r != nil {
			truth = false
		}
	}()

	i, j := reflect.ValueOf(a), reflect.ValueOf(b)
	return reflect.DeepEqual(i.Convert(j.Type()).Interface(), b)
}

// Match applies the current tuple as a match template against u.
func (t Tuple) Match(u Tuple) bool {
	if len(t) != len(u) {
		return false
	}
	for i, e := range t {
		f := u[i]
		if e == nil {
			continue
		}
		// TODO: Use a more lenient comparison. Currently this fails if an int is
		// passed into the leveldb backend, because msgpack serializes as an
		// int64. If a match is subsequently passed with an int, the DeepEqual
		// comparison will fail because it expects types to be identical.
		if !equal(e, f) {
			return false
		}
	}
	return true
}

// Key returns the non-unique key for this tuple.
func (t Tuple) Key() []byte {
	return []byte(t.String())
}
