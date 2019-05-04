package kafka

import (
	"encoding/binary"
	"io"
	"time"
)

// APIVersionsRequestV1 Represents a V1 API Version request
type APIVersionsRequestV1 struct{}

func (r *APIVersionsRequestV1) size() int32             { return 0 }
func (r *APIVersionsRequestV1) write(w io.Writer) error { return nil }

// APIKey returns the API key for the request
func (r *APIVersionsRequestV1) APIKey() APIKey { return APIKeyAPIVersions }

// Version returns the request version
func (r *APIVersionsRequestV1) Version() int16 { return 1 }

// NewAPIVersionsRequestV1 creates a new V1 API Versions Request
func NewAPIVersionsRequestV1() *APIVersionsRequestV1 {
	return &APIVersionsRequestV1{}
}

// SupportedVersions represents a set of supported API versions
type SupportedVersions map[APIKey]APIVersion

// IsSupported returns whether or not the given API Key and version are supported. Returns an UnsupportedVersionError if they're not supported.
func (versions SupportedVersions) IsSupported(key APIKey, version int16) error {
	v, ok := versions[key]
	if !ok {
		return newServerUnsupportedAPIError(key, nil)
	}

	if version < v.MinVersion || version > v.MaxVersion {
		return newServerUnsupportedAPIError(key, &v)
	}

	return nil
}

// APIVersion represents the versions of an API that are supported
type APIVersion struct {
	MinVersion int16
	MaxVersion int16
}
type apiV1Response struct {
	err error

	versions SupportedVersions

	throttleTime time.Duration
}

func readAPIVersionsResponseV1(r io.Reader) (*apiV1Response, error) {
	err := ErrorFromReader(r)
	if err != nil {
		return nil, err
	}

	resp := apiV1Response{
		versions: SupportedVersions{},
	}

	var arrLen int32
	err = binary.Read(r, binary.BigEndian, &arrLen)
	for i := 0; i < int(arrLen); i++ {
		var version APIVersion
		apiKey, err := readInt16(r)
		if err != nil {
			return nil, err
		}

		version.MinVersion, err = readInt16(r)
		if err != nil {
			return nil, err
		}

		version.MaxVersion, err = readInt16(r)
		if err != nil {
			return nil, err
		}

		resp.versions[APIKey(apiKey)] = version
	}

	throttleTime, err := readInt32(r)
	if err != nil {
		return nil, err
	}

	resp.throttleTime = time.Duration(throttleTime) * time.Millisecond
	return &resp, nil
}
