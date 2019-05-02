package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

type saslAuthenticateRequest struct {
	data []byte
}

func (r *saslAuthenticateRequest) size() int32 {
	return int32(len(r.data))
}

func (r *saslAuthenticateRequest) write(w io.Writer) error {
	_, err := w.Write(r.data)
	return err
}

func newSASLAuthenticateRequest(username, password string) *request {
	return &request{
		r: &saslAuthenticateRequest{
			data: []byte("" + "\x00" + username + "\x00" + password),
		},
	}
}

func readSASLAuthenticateResponse(r io.Reader) error {
	var kerr kafkaError
	err := binary.Read(r, binary.BigEndian, &kerr.code)
	if err != nil {
		return err
	}
	if kerr.code != 0 {
		return &kerr
	}

	errMsg, err := readNullableString(r)
	if err != nil {
		return nil
	}
	if errMsg != nil {
		fmt.Printf("error message: %v\n", errMsg)
	}

	return nil
}
