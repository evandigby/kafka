package versions

import (
	"fmt"
	"io"
	"time"

	"github.com/evandigby/kafka/api"
	"github.com/evandigby/kafka/api/enc"
)

// Supported represents a set of supported API versions
type Supported map[api.Key]*Version

// Versions returns the min and max version supported for the API Key
func (versions Supported) Versions(key api.Key) (int16, int16, error) {
	v, ok := versions[key]
	if !ok {
		return -1, -1, fmt.Errorf("%v: %w", api.KeyMetadata, ErrorUnsupportedAPI)
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

	resp.Error = api.ErrorFromReader(r)
	if resp.Error != nil && !api.IsKafkaError(resp.Error) {
		return nil, resp.Error
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
