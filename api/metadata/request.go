package metadata

import (
	"io"

	"github.com/evandigby/kafka/api"
	"github.com/evandigby/kafka/api/enc"
)

// Request is a metadata request
type Request struct {
	Topics             []string
	AllowTopicCreation bool
}

// RequestV0 is a metadata request
type RequestV0 struct {
	topics []string
}

// RequestV1 is a metadata request
type RequestV1 struct{ RequestV0 }

// RequestV2 is a metadata request
type RequestV2 struct{ RequestV0 }

// RequestV3 is a metadata request
type RequestV3 struct{ RequestV0 }

func (r *RequestV0) APIKey() api.Key { return api.KeyMetadata }

func (r *RequestV0) Size() int32 {
	return enc.StringArraySize(r.topics)
}

func (r *RequestV0) Write(w io.Writer) error {
	return enc.WriteStringArray(w, r.topics)
}

// RequestV4 is a metadata request
type RequestV4 struct {
	RequestV0
	allowTopicCreation bool
}

// RequestV5 is a metadata request
type RequestV5 struct{ RequestV4 }

// RequestV6 is a metadata request
type RequestV6 struct{ RequestV4 }

// RequestV7 is a metadata request
type RequestV7 struct{ RequestV4 }

func (r *RequestV4) Size() int32 {
	return r.RequestV0.Size() + enc.BoolSize()
}

func (r *RequestV4) Write(w io.Writer) error {
	err := r.RequestV0.Write(w)
	if err != nil {
		return err
	}

	return enc.WriteBool(w, r.allowTopicCreation)
}

func (r *RequestV0) Version() int16 { return 0 }
func (r *RequestV1) Version() int16 { return 1 }
func (r *RequestV2) Version() int16 { return 2 }
func (r *RequestV3) Version() int16 { return 3 }
func (r *RequestV4) Version() int16 { return 4 }
func (r *RequestV5) Version() int16 { return 5 }
func (r *RequestV6) Version() int16 { return 6 }
func (r *RequestV7) Version() int16 { return 7 }

func NewRequestV0(r *Request) RequestV0 {
	return RequestV0{
		topics: r.Topics,
	}
}

func NewRequestV4(r *Request) RequestV4 {
	return RequestV4{
		RequestV0:          NewRequestV0(r),
		allowTopicCreation: r.AllowTopicCreation,
	}
}
