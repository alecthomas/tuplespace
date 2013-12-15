package server

import (
	"github.com/alecthomas/tuplespace"
	"github.com/codegangsta/martini"
	"net/http"
	"reflect"
)

// DeserializerMiddleware decodes requests into a structure using the
// appropriate deserializer (eg. JSON, MsgPack, BSON, etc.)
func DeserializerMiddleware(proto interface{}) martini.Handler {
	return func(context martini.Context, req *http.Request, resp http.ResponseWriter) {
		out := reflect.New(reflect.TypeOf(proto))
		err := Serializers.DecodeRequest(req, out.Interface())
		if err != nil {
			packet := &tuplespace.ErrorResponse{Error: err.Error()}
			Serializers.EncodeResponse(resp, http.StatusBadRequest, req.Header.Get("Content-Type"), packet)
			return
		}
		context.Map(out.Elem().Interface())
	}
}

// SerializationMiddleware binds a ResponseSerializer to the current request.
func SerializationMiddleware() martini.Handler {
	return func(context martini.Context, r *http.Request, w http.ResponseWriter) {
		context.MapTo(&responseSerializer{r: r, w: w}, (*ResponseSerializer)(nil))
	}
}

// A ResponseSerializer knows how to correctly serialize a response for a client.
type ResponseSerializer interface {
	Serialize(code int, v interface{})
	Error(code int, err error)
}

type responseSerializer struct {
	r *http.Request
	w http.ResponseWriter
}

func (r *responseSerializer) Serialize(code int, v interface{}) {
	Serializers.EncodeResponseForRequest(r.r, r.w, code, v)
}

func (r *responseSerializer) Error(code int, err error) {
	Serializers.EncodeResponseForRequest(r.r, r.w, code, &tuplespace.ErrorResponse{Error: err.Error()})
}
