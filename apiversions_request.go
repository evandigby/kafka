package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

type apiVersionsRequest struct{}

func (r *apiVersionsRequest) size() int32             { return 0 }
func (r *apiVersionsRequest) write(w io.Writer) error { return nil }

func newAPIVersionsRequest(correlationID int32, clientID string) *request {
	return &request{
		h: &requestHeader{
			APIKey:        APIKeyAPIVersions,
			APIVersion:    1,
			CorrelationID: correlationID,
			ClientID:      clientID,
		},
		r: &apiVersionsRequest{},
	}
}

func readAPIVersionsResponse(r io.Reader) error {
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
	for i := 0; i < int(arrLen); i++ {
		var (
			apiKey     int16
			minVersion int16
			maxVersion int16
		)
		err = binary.Read(r, binary.BigEndian, &apiKey)
		if err != nil {
			return err
		}
		err = binary.Read(r, binary.BigEndian, &minVersion)
		if err != nil {
			return err
		}
		err = binary.Read(r, binary.BigEndian, &maxVersion)
		if err != nil {
			return err
		}
		fmt.Printf("Api %q: Min: %v, Max: %v\n", apiKeys[apiKey], minVersion, maxVersion)
	}

	var throttleTime int32
	err = binary.Read(r, binary.BigEndian, throttleTime)
	if err != nil {
		return err
	}
	fmt.Println("Throttle time:", throttleTime)
	return nil
}
