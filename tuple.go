package tuplespace

// A Tuple represents a stored tuple.
type Tuple map[string]interface{}

// func (t Tuple) String() string {
// 	tuple := []string{}
// 	for k, v := range t {
// 		tuple = append(tuple, fmt.Sprintf("%v: %#v", k, v))
// 	}
// 	return fmt.Sprintf("(%s)", strings.Join(tuple, ", "))
// }
