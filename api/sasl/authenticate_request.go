package sasl

import (
	"io"

	"github.com/evandigby/kafka/api"
)

// AuthenticateRequest represents a Authentication Request
type AuthenticateRequest struct {
	data []byte
}

func (r *AuthenticateRequest) Size() int32 {
	return int32(len(r.data))
}

func (r *AuthenticateRequest) Write(w io.Writer) error {
	_, err := w.Write(r.data)
	return err
}

// APIKey returns the API key for the request
func (r *AuthenticateRequest) APIKey() api.Key { return api.KeySaslAuthenticate }

// Version returns the request version
func (r *AuthenticateRequest) Version() int16 { return -1 }

// NewAuthenticateRequest creates a new Authentication request
func NewAuthenticateRequest(username, password string) *AuthenticateRequest {
	return &AuthenticateRequest{
		data: []byte("" + "\x00" + username + "\x00" + password),
	}
}
