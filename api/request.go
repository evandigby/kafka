package api

import (
	"encoding/binary"
	"io"

	"github.com/evandigby/kafka/api/enc"
)

type RequestFactory func(d interface{}) Request

// Request represents a kafka request
type Request interface {
	APIKey() Key
	Version() int16
	Size() int32
	Write(w io.Writer) error
}

type RequestHeader struct {
	APIKey        Key
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

func (r *RequestHeader) size() int32 {
	if r == nil {
		return 0
	}

	return 10 + int32(len(r.ClientID))
}

func (r *RequestHeader) Write(w io.Writer) error {
	if r == nil {
		return nil
	}

	err := binary.Write(w, binary.BigEndian, r.APIKey)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, r.APIVersion)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, r.CorrelationID)
	if err != nil {
		return err
	}

	return enc.WriteString(w, r.ClientID)
}

type RequestData struct {
	Header  *RequestHeader
	Request Request
}

func (r *RequestData) Write(w io.Writer) error {
	sz := r.Header.size() + r.Request.Size()
	err := binary.Write(w, binary.BigEndian, sz)
	if err != nil {
		return err
	}

	err = r.Header.Write(w)
	if err != nil {
		return err
	}

	return r.Request.Write(w)
}
