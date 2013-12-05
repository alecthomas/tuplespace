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
