package server

import (
	"encoding/json"
	"errors"
	"github.com/alecthomas/tuplespace"
	"github.com/vmihailenco/msgpack"
	"github.com/youtube/vitess/go/bson"
	"io"
	"io/ioutil"
	"net/http"
)

var (
	Serializers = SerializerMap{
		"application/json":      &JsonSerializer{},
		"application/x-msgpack": &MsgpackSerializer{},
		"application/bson":      &BsonSerializer{},
	}
	UnsupportedContentType = errors.New("unsupported content type")
	EmptyRequestBody       = errors.New("empty request body")
)

type SerializerMap map[string]Serializer

func (s SerializerMap) DecodeRequest(r *http.Request, v interface{}) error {
	if r.Body == nil {
		return EmptyRequestBody
	}
	ct := r.Header.Get("Content-Type")
	defer r.Body.Close()
	return s.Decode(ct, r.Body, v)

}

func (s SerializerMap) Decode(ct string, r io.Reader, v interface{}) error {
	if ser, ok := s[ct]; ok {
		decoder := ser.NewDecoder(r)
		return decoder.Decode(v)
	}
	return UnsupportedContentType
}

func (s SerializerMap) EncodeResponseForRequest(r *http.Request, w http.ResponseWriter, code int, response interface{}) error {
	// TODO: Handle Accept header correctly.
	ct := r.Header.Get("Content-Type")
	return s.EncodeResponse(w, code, ct, response)
}

func (s SerializerMap) EncodeResponse(w http.ResponseWriter, code int, contentType string, response interface{}) error {
	// TODO: Figure out ordering here that isn't shit.
	if ser, ok := s[contentType]; ok {
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(code)
		s.rawEncode(ser, w, response)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		ser := s["application/json"]
		err := "Invalid content type " + contentType
		s.rawEncode(ser, w, &tuplespace.ErrorResponse{
			Error: err,
		})
		return errors.New(err)
	}
	return nil
}

func (s SerializerMap) Encode(ct string, w io.Writer, v interface{}) error {
	if ser, ok := s[ct]; ok {
		return s.rawEncode(ser, w, v)
	}
	return UnsupportedContentType
}

func (s SerializerMap) rawEncode(ser Serializer, w io.Writer, v interface{}) error {
	encoder := ser.NewEncoder(w)
	return encoder.Encode(v)
}

type ContentTypeDecoder interface {
	Decode(v interface{}) error
}

type ContentTypeEncoder interface {
	Encode(v interface{}) error
}

type Serializer interface {
	NewEncoder(w io.Writer) ContentTypeEncoder
	NewDecoder(r io.Reader) ContentTypeDecoder
}

type JsonSerializer struct{}

func (j *JsonSerializer) NewEncoder(w io.Writer) ContentTypeEncoder {
	return json.NewEncoder(w)
}

func (j *JsonSerializer) NewDecoder(r io.Reader) ContentTypeDecoder {
	return json.NewDecoder(r)
}

type msgpackEncoderAdapter struct{ e *msgpack.Encoder }

func (m *msgpackEncoderAdapter) Encode(v interface{}) error { return m.e.Encode(v) }

type msgpackDecoderAdapter struct{ d *msgpack.Decoder }

func (m *msgpackDecoderAdapter) Decode(v interface{}) error { return m.d.Decode(v) }

type MsgpackSerializer struct{}

func (j *MsgpackSerializer) NewEncoder(w io.Writer) ContentTypeEncoder {
	return &msgpackEncoderAdapter{msgpack.NewEncoder(w)}
}

func (j *MsgpackSerializer) NewDecoder(r io.Reader) ContentTypeDecoder {
	return &msgpackDecoderAdapter{msgpack.NewDecoder(r)}
}

type BsonSerializer struct{}

type bsonEncoder struct {
	w io.Writer
}

func (b *bsonEncoder) Encode(v interface{}) error {
	bytes, err := bson.Marshal(v)
	if err != nil {
		return err
	}
	_, err = b.w.Write(bytes)
	return err
}

func (j *BsonSerializer) NewEncoder(w io.Writer) ContentTypeEncoder {
	return &bsonEncoder{w}
}

type bsonDecoder struct {
	r io.Reader
}

func (b *bsonDecoder) Decode(v interface{}) error {
	bytes, err := ioutil.ReadAll(b.r)
	if err != nil {
		return err
	}
	return bson.Unmarshal(bytes, v)
}

func (j *BsonSerializer) NewDecoder(r io.Reader) ContentTypeDecoder {
	return &bsonDecoder{r}
}
