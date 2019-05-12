package sasl

import (
	"io"

	"github.com/evandigby/kafka/api"
	"github.com/evandigby/kafka/api/enc"
)

// HandshakeRequestV0 is a V0 sasl handshake request
type HandshakeRequestV0 struct {
	mechanism string
}

func (r *HandshakeRequestV0) Size() int32 {
	return enc.StringSize(r.mechanism)
}

func (r *HandshakeRequestV0) Write(w io.Writer) error {
	return enc.WriteString(w, r.mechanism)
}

// APIKey returns the API key for the request
func (r *HandshakeRequestV0) APIKey() api.Key { return api.KeySaslHandshake }

// Version returns the request version
func (r *HandshakeRequestV0) Version() int16 { return 0 }

func NewHandshakeRequestV0(mechanism string) *HandshakeRequestV0 {
	return &HandshakeRequestV0{
		mechanism: mechanism,
	}
}

// HandshakeV0Response represents a V0 sasl handshake response
type HandshakeV0Response struct {
	Mechanisms []string
}

func ReadHandshakeResponseV0(r io.Reader) (*HandshakeV0Response, error) {
	err := api.ErrorFromReader(r)
	if err != nil {
		return nil, err
	}

	arrLen, err := enc.ReadInt32(r)
	if err != nil {
		return nil, err
	}

	var resp HandshakeV0Response
	resp.Mechanisms = make([]string, int(arrLen))
	for i := 0; i < int(arrLen); i++ {
		resp.Mechanisms[i], err = enc.ReadString(r)
		if err != nil {
			return nil, err
		}
	}

	return &resp, nil
}
