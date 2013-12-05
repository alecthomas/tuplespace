package tuplespace

import (
	"fmt"
	"reflect"
)

// A Tuple represents a stored tuple.
type Tuple []interface{}

func (t Tuple) String() string {
	return fmt.Sprintf("%#v", t)
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
		if !reflect.DeepEqual(e, f) {
			return false
		}
	}
	return true
}
