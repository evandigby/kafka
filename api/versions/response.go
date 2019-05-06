package versions

import (
	"fmt"
	"io"
	"time"

	"github.com/evandigby/kafka/api"
	"github.com/evandigby/kafka/api/kafkaerror"
	"github.com/evandigby/kafka/enc"
)

// Supported represents a set of supported API versions
type Supported map[api.Key]*Version

// Versions returns the min and max version supported for the API Key
func (versions Supported) Versions(key api.Key) (int16, int16, error) {
	v, ok := versions[key]
	if !ok {
		return -1, -1, NewServerUnsupportedAPIError(key, nil)
	}

	return v.MinVersion, v.MaxVersion, nil
}

// IsSupported returns whether or not the given API Key and version are supported.
func (versions Supported) IsSupported(key api.Key, version int16) bool {
	v, ok := versions[key]
	if !ok {
		return false
	}

	if version < v.MinVersion || version > v.MaxVersion {
		return false
	}

	return true
}

// Version represents the versions of an API that are supported
type Version struct {
	MinVersion int16
	MaxVersion int16
	NewRequest api.RequestFactory
}

type V1Response struct {
	Error error

	Versions Supported

	ThrottleTime time.Duration
}

func ReadV1Response(r io.Reader) (*V1Response, error) {
	resp := V1Response{
		Versions: Supported{},
	}

	resp.Error = kafkaerror.FromReader(r)
	if resp.Error != nil {
		if _, ok := resp.Error.(*kafkaerror.Error); !ok {
			return nil, resp.Error
		}
	}

	arrLen, err := enc.ReadInt32(r)
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(arrLen); i++ {
		var version Version
		apiKey, err := enc.ReadInt16(r)
		if err != nil {
			return nil, err
		}

		version.MinVersion, err = enc.ReadInt16(r)
		if err != nil {
			return nil, err
		}

		version.MaxVersion, err = enc.ReadInt16(r)
		if err != nil {
			return nil, err
		}

		version.NewRequest, err = newRequestFactory(api.Key(apiKey), version.MinVersion, version.MaxVersion)
		if err != nil {
			return nil, err
		}

		resp.Versions[api.Key(apiKey)] = &version
	}

	throttleTime, err := enc.ReadInt32(r)
	if err != nil {
		return nil, err
	}

	resp.ThrottleTime = time.Duration(throttleTime) * time.Millisecond
	return &resp, nil
}

func NewServerUnsupportedAPIError(key api.Key, supportedVersions *Version) error {
	return &ServerUnsupportedAPIError{
		API:               key,
		SupportedVersions: supportedVersions,
	}
}

// IsServerUnsupportedVersionError returns whether or not this error is an unsupported version error
func IsServerUnsupportedVersionError(err error) bool {
	_, ok := err.(*ServerUnsupportedAPIError)
	return ok
}

// ServerUnsupportedAPIError is returned when you attempt to use an API or API version that isn't supported by the server
type ServerUnsupportedAPIError struct {
	API               api.Key
	SupportedVersions *Version
}

func (e *ServerUnsupportedAPIError) Error() string {
	if e.SupportedVersions == nil {
		return fmt.Sprintf("server unsupported API %q:", e.API)
	}

	return fmt.Sprintf("server unsupported API version for %q. Supported versions are Min: %v, Max: %v", e.API, e.SupportedVersions.MinVersion, e.SupportedVersions.MaxVersion)
}

func NewClientUnsupportedAPIError(key api.Key, v int16) error {
	return &ClientUnsupportedAPIError{
		API:     key,
		Version: v,
	}
}

// IsClientUnsupportedVersionError returns whether or not this error is an unsupported version error
func IsClientUnsupportedVersionError(err error) bool {
	_, ok := err.(*ClientUnsupportedAPIError)
	return ok
}

// ClientUnsupportedAPIError is returned when you attempt to use an API or API version that isn't supported by the server
type ClientUnsupportedAPIError struct {
	API     api.Key
	Version int16
}

func (e *ClientUnsupportedAPIError) Error() string {
	if e.Version < 0 {
		return fmt.Sprintf("client unsupported API %q", e.API)
	}
	return fmt.Sprintf("client unsupported API Version for %q: %v", e.API, e.Version)
}
