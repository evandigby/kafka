package kafka

import (
	"io"
)

// SASLHandshakeRequestV0 is a V0 sasl handshake request
type SASLHandshakeRequestV0 struct {
	mechanism string
}

func (r *SASLHandshakeRequestV0) size() int32 {
	return stringSize(r.mechanism)
}

func (r *SASLHandshakeRequestV0) write(w io.Writer) error {
	return writeString(w, r.mechanism)
}

// APIKey returns the API key for the request
func (r *SASLHandshakeRequestV0) APIKey() APIKey { return APIKeySaslHandshake }

// Version returns the request version
func (r *SASLHandshakeRequestV0) Version() int16 { return 0 }

func newSASLHandshakeRequestV0(mechanism string) *SASLHandshakeRequestV0 {
	return &SASLHandshakeRequestV0{
		mechanism: mechanism,
	}
}

// SASLHandshakeV0Response represents a V0 sasl handshake response
type SASLHandshakeV0Response struct {
	Mechanisms []string
}

func readSASLHandshakeResponseV0(r io.Reader) (*SASLHandshakeV0Response, error) {
	err := ErrorFromReader(r)
	if err != nil {
		return nil, err
	}

	arrLen, err := readInt32(r)
	if err != nil {
		return nil, err
	}

	var resp SASLHandshakeV0Response
	resp.Mechanisms = make([]string, int(arrLen))
	for i := 0; i < int(arrLen); i++ {
		resp.Mechanisms[i], err = readString(r)
		if err != nil {
			return nil, err
		}
	}

	return &resp, nil
}
