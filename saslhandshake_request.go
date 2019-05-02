package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

type saslHandshakeRequest struct {
	mechanism string
}

func (r *saslHandshakeRequest) size() int32 {
	return stringSize(r.mechanism)
}

func (r *saslHandshakeRequest) write(w io.Writer) error {
	return writeString(w, r.mechanism)
}

func newSASLHandshakeRequest(correlationID int32, clientID string, mechanism string) *request {
	return &request{
		h: &requestHeader{
			APIKey:        APIKeySaslHandshake,
			APIVersion:    0,
			CorrelationID: correlationID,
			ClientID:      clientID,
		},
		r: &saslHandshakeRequest{
			mechanism: mechanism,
		},
	}
}

func readSASLHandshakeResponse(r io.Reader) error {
	var kerr kafkaError
	err := binary.Read(r, binary.BigEndian, &kerr.code)
	if err != nil {
		return err
	}
	if kerr.code != 0 {
		return &kerr
	}

	var arrLen int32
	err = binary.Read(r, binary.BigEndian, &arrLen)
	if err != nil {
		return err
	}
	for i := 0; i < int(arrLen); i++ {
		mechanism, err := readString(r)
		if err != nil {
			return err
		}
		fmt.Printf("Mechanism: %q\n", mechanism)
	}

	return nil
}
