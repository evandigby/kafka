package kafka

import (
	"encoding/binary"
	"io"
)

type requestHeader struct {
	APIKey        APIKey
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

func (r *requestHeader) size() int32 {
	if r == nil {
		return 0
	}

	return 10 + int32(len(r.ClientID))
}

func (r *requestHeader) write(w io.Writer) error {
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

	return writeString(w, r.ClientID)
}

// Request represents a kafka request
type Request interface {
	APIKey() APIKey
	Version() int16
	size() int32
	write(w io.Writer) error
}

type request struct {
	h *requestHeader
	r Request
}

func (r *request) write(w io.Writer) error {
	sz := r.h.size() + r.r.size()
	err := binary.Write(w, binary.BigEndian, sz)
	if err != nil {
		return err
	}

	err = r.h.write(w)
	if err != nil {
		return err
	}

	return r.r.write(w)
}
