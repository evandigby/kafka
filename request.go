package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

type requestHeader struct {
	APIKey        int16
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

type writerSizer interface {
	size() int32
	write(w io.Writer) error
}

type request struct {
	h *requestHeader
	r writerSizer
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

func readResponse(r io.Reader, readCorrelation bool) (int32, []byte, error) {
	var size int32
	err := binary.Read(r, binary.BigEndian, &size)
	if err != nil {
		fmt.Println("first")
		return -1, nil, err
	}

	fmt.Println("SIZE:", size)

	resp := make([]byte, int(size))
	_, err = io.ReadFull(r, resp)
	if err != nil {
		fmt.Println("Second")
		return -1, nil, err
	}

	if !readCorrelation {
		return -1, resp, nil
	}

	cid := int32(binary.BigEndian.Uint32(resp[:4]))
	return cid, resp[4:], nil
}
