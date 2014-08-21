package tuplespace

import (
	"github.com/alecthomas/rapid"
	"github.com/alecthomas/rapid/schema"
)

// Service definition for the TupleSpace RESTful service.
//
// The TupleSpace service provides namespaced tuplespaces.
//
// 		GET /tuplespace -> list tuplespaces
// 		POST /tuplespace -> create tuplespace
//
func Service() *schema.Schema {
	d := rapid.Define("TupleSpace")
	return d.Build()
}

func Server() (*rapid.Server, error) {
	handler := &server{
		spaces: make(map[string]*TupleSpace),
	}
	return rapid.NewServer(Service(), handler)
}

type server struct {
	spaces map[string]*TupleSpace
}
