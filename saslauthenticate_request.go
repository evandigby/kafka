package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

type saslAuthenticateRequest struct {
	username string
	password string
}

func (r *saslAuthenticateRequest) size() int32 {
	return int32(len(r.username) + len(r.password) + 2)
}

func (r *saslAuthenticateRequest) write(w io.Writer) error {
	_, err := w.Write([]byte{0})
	if err != nil {
		return err
	}

	_, err = w.Write([]byte(r.username))
	if err != nil {
		return err
	}

	_, err = w.Write([]byte{0})
	if err != nil {
		return err
	}

	_, err = w.Write([]byte(r.password))
	return err
}

func newSASLAuthenticateRequest(userName, password string) *request {
	return &request{
		r: &saslAuthenticateRequest{
			username: userName,
			password: password,
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
