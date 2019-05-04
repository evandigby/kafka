package kafka

import (
	"fmt"
	"io"
)

// SASLAuthenticateRequest represents a SASLAuthentication Request
type SASLAuthenticateRequest struct {
	data []byte
}

func (r *SASLAuthenticateRequest) size() int32 {
	return int32(len(r.data))
}

func (r *SASLAuthenticateRequest) write(w io.Writer) error {
	_, err := w.Write(r.data)
	return err
}

// APIKey returns the API key for the request
func (r *SASLAuthenticateRequest) APIKey() APIKey { return APIKeySaslAuthenticate }

// Version returns the request version
func (r *SASLAuthenticateRequest) Version() int16 { return -1 }

// NewSASLAuthenticateRequest creates a new SASLAuthentication request
func NewSASLAuthenticateRequest(username, password string) *SASLAuthenticateRequest {
	return &SASLAuthenticateRequest{
		data: []byte("" + "\x00" + username + "\x00" + password),
	}
}

func readSASLAuthenticateResponse(r io.Reader) error {
	err := ErrorFromReader(r)
	if err != nil {
		return err
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
