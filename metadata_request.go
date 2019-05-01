package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

type metadataRequest struct {
	topics             []string
	allowTopicCreation bool
}

func (r *metadataRequest) size() int32 {
	return stringArraySize(r.topics) + boolSize()
}

func (r *metadataRequest) write(w io.Writer) error {
	err := writeStringArray(w, r.topics)
	if err != nil {
		return err
	}

	return writeBool(w, r.allowTopicCreation)
}

func newMetadataRequest(correlationID int32, clientID string, topics []string, allowTopicCreation bool) *request {
	return &request{
		h: &requestHeader{
			APIKey:        APIKeyMetadata,
			APIVersion:    5,
			CorrelationID: correlationID,
			ClientID:      clientID,
		},
		r: &metadataRequest{
			topics:             topics,
			allowTopicCreation: allowTopicCreation,
		},
	}
}

func readMetadataResponse(r io.Reader) error {
	var throttleTime int32
	err := binary.Read(r, binary.BigEndian, &throttleTime)
	if err != nil {
		return err
	}

	fmt.Println("Throttle time:", throttleTime)

	// Brokers
	var arrLen int32
	err = binary.Read(r, binary.BigEndian, &arrLen)
	for i := 0; i < int(arrLen); i++ {
		var (
			nodeID int32
			port   int32
		)
		err = binary.Read(r, binary.BigEndian, &nodeID)
		if err != nil {
			return err
		}

		host, err := readString(r)
		if err != nil {
			return err
		}

		err = binary.Read(r, binary.BigEndian, &port)
		if err != nil {
			return err
		}

		rack, err := readNullableString(r)
		if err != nil {
			return err
		}

		var strRack string
		if rack != nil {
			strRack = *rack
		}

		fmt.Printf("Node: %v, Host: %q, Port: %v, Rack: %q\n", nodeID, host, port, strRack)
	}

	return nil
}
