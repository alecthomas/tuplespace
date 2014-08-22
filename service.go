package tuplespace

import (
	"errors"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/alecthomas/go-logging"

	"github.com/alecthomas/rapid"
	"github.com/alecthomas/rapid/schema"
)

var (
	log            = logging.MustGetLogger("tuplespace")
	spaceNameRegex = regexp.MustCompile(`[.\w]+`)
)

type TupleSpacePath struct {
	ID string `schema:"id"`
}

func (t *TupleSpacePath) Validate() error {
	if !spaceNameRegex.MatchString(t.ID) {
		return errors.New("space name must match " + spaceNameRegex.String())
	}
	return nil
}

type SendRequest struct {
	Tuples      []Tuple       `json:"tuples"`
	Expires     time.Duration `json:"expires,omitempty"`
	Acknowledge bool          `json:"ack"`
}

func (s *SendRequest) Validate() error {
	if len(s.Tuples) == 0 {
		return errors.New("need at least one tuple")
	}
	if s.Acknowledge && len(s.Tuples) > 1 {
		return errors.New("can only acknowledge a single tuple")
	}
	return nil
}

type ConsumeQuery struct {
	Query   string        `schema:"q"`
	Timeout time.Duration `schema:"timeout"`
}

func (c *ConsumeQuery) Validate() error {
	return nil
}

// Service definition for the TupleSpace RESTful service.
//
// The TupleSpace service provides namespaced tuplespaces. Spaces are created on demand.
//
// 		GET /tuplespaces -> list tuplespaces
// 		GET /tuplespaces/{id} -> status of tuplespace
//		DELETE /tuplespaces/{id} -> delete tuplespace
//
// 		POST /tuplespaces/{id}/tuples -> send tuple to tuplespace {id}
// 		GET /tuplespaces/{id}/tuples?q={expr}&timeout={timeout} -> read tuple matching {expr}
// 		DELETE /tuplespaces/{id}/tuples?q={expr}&timeout={timeout} -> read tuple matching {expr}
//
func Service() *schema.Schema {
	d := rapid.Define("TupleSpace")

	tuplespace := d.Resource("TupleSpaces", "/tuplespaces").Description("Manage tuple spaces.")
	tuplespace.Route("ListSpaces", "/tuplespaces").
		Description("List tuple spaces.").
		Get().
		Response(http.StatusOK, []string{})
	tuplespace.Route("SpaceStatus", "/tuplespaces/{id:[.\\w]+}").
		Description("Return status of tuple space.").
		Get().
		Path(&TupleSpacePath{}).
		Response(http.StatusOK, &Status{})
	tuplespace.Route("DeleteSpace", "/tuplespaces/{id:[.\\w]+}").
		Description("Delete a tuple space.").
		Delete().
		Path(&TupleSpacePath{}).
		Response(http.StatusOK, nil).
		Response(http.StatusNotFound, nil)

	tuples := d.Resource("Tuples", "/tuplespaces/{id:[.\\w]+}/tuples")
	tuples.Route("Send", "/tuplespaces/{id:[.\\w]+}/tuples").
		Description("Send a tuple to the space.").
		Post().
		Request(&SendRequest{}).
		Path(&TupleSpacePath{}).
		Response(http.StatusCreated, nil).
		Response(http.StatusBadRequest, nil).
		Response(http.StatusGatewayTimeout, nil).
		Response(http.StatusInternalServerError, nil)
	tuples.Route("Read", "/tuplespaces/{id:[.\\w]+}/tuples").
		Description("Read a tuple from the space.").
		Get().
		Path(&TupleSpacePath{}).
		Query(&ConsumeQuery{}).
		Response(http.StatusOK, []Tuple{}).
		Response(http.StatusGatewayTimeout, nil)
	tuples.Route("Take", "/tuplespaces/{id:[.\\w]+}/tuples").
		Description("Take a tuple from the space.").
		Delete().
		Path(&TupleSpacePath{}).
		Query(&ConsumeQuery{}).
		Response(http.StatusOK, []Tuple{}).
		Response(http.StatusGatewayTimeout, nil)
	return d.Build()
}

func Server() (*rapid.Server, error) {
	server, err := rapid.NewServer(Service(), newServer())
	if err != nil {
		return nil, err
	}
	server.SetLogger(log)
	return server, nil
}

type server struct {
	lock   sync.Mutex
	spaces map[string]*TupleSpace
}

func newServer() *server {
	return &server{
		spaces: make(map[string]*TupleSpace),
	}
}

func (s *server) ListSpaces() ([]string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	spaces := make([]string, 0, len(s.spaces))
	for name := range s.spaces {
		spaces = append(spaces, name)
	}
	return spaces, nil
}

// Atomically retrieve an existing space, or create a new one.
func (s *server) getOrCreate(name string) *TupleSpace {
	s.lock.Lock()
	defer s.lock.Unlock()
	if space, ok := s.spaces[name]; ok {
		return space
	}
	space := New()
	s.spaces[name] = space
	return space
}

func (s *server) SpaceStatus(path *TupleSpacePath) (*Status, error) {
	return s.getOrCreate(path.ID).Status(), nil
}

func (s *server) DeleteSpace(path *TupleSpacePath) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	space, ok := s.spaces[path.ID]
	if !ok {
		return rapid.Error(http.StatusNotFound, "space not found")
	}
	err := space.Close()
	delete(s.spaces, path.ID)
	if err != nil {
		return rapid.Error(http.StatusInternalServerError, err.Error())
	}
	return nil
}

func (s *server) Send(path *TupleSpacePath, req *SendRequest) error {
	space := s.getOrCreate(path.ID)
	for _, tuple := range req.Tuples {
		var err error
		if req.Acknowledge {
			err = space.SendWithAcknowledgement(tuple, req.Expires)
		} else {
			err = space.Send(tuple, req.Expires)
		}
		if err == ErrTimeout {
			return rapid.Error(http.StatusGatewayTimeout, err.Error())
		} else if err != nil {
			return rapid.Error(http.StatusInternalServerError, err.Error())
		}
	}
	return nil
}

func (s *server) Read(path *TupleSpacePath, query *ConsumeQuery, cancel rapid.CloseNotifierChannel) ([]Tuple, error) {
	space := s.getOrCreate(path.ID)
	tuple, err := space.Consume(&ConsumeRequest{
		Match:   query.Query,
		Timeout: query.Timeout,
		Cancel:  cancel,
	})
	if err != nil {
		return nil, rapid.Error(http.StatusInternalServerError, err.Error())
	}
	return []Tuple{tuple}, nil
}

func (s *server) Take(path *TupleSpacePath, query *ConsumeQuery, cancel rapid.CloseNotifierChannel) ([]Tuple, error) {
	space := s.getOrCreate(path.ID)
	tuple, err := space.Consume(&ConsumeRequest{
		Match:   query.Query,
		Timeout: query.Timeout,
		Take:    true,
		Cancel:  cancel,
	})
	if err != nil {
		return nil, rapid.Error(http.StatusInternalServerError, err.Error())
	}
	return []Tuple{tuple}, nil
}
