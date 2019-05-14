package versions

import (
	"errors"
	"fmt"

	"github.com/evandigby/kafka/api"
	"github.com/evandigby/kafka/api/metadata"
)

// ErrorUnsupportedAPI is returned whenever you attempt to use an unsupported API version
var ErrorUnsupportedAPI = errors.New("unsupported API")

// Request returns a request for the maximum supported version of the API Key
func (versions Supported) Request(key api.Key, d interface{}) (api.Request, error) {
	v, ok := versions[key]
	if !ok {
		return nil, fmt.Errorf("%v: %w", key, ErrorUnsupportedAPI)
	}

	if v.NewRequest == nil {
		return nil, fmt.Errorf("%v: %w", key, ErrorUnsupportedAPI)
	}
	return v.NewRequest(d), nil
}

func newRequestFactory(apiKey api.Key, minVersion, maxVersion int16) (api.RequestFactory, error) {
	switch apiKey {
	case api.KeyMetadata:
		return newMetadataRequestForVersion(minVersion, maxVersion)
	}

	return nil, nil // fmt.Errorf("%v: %w", apiKey, ErrorUnsupportedAPI)
}

func newMetadataRequestForVersion(minVersion, maxVersion int16) (api.RequestFactory, error) {
	switch maxVersion {
	case 0:
		return func(d interface{}) api.Request {
			r := metadata.NewRequestV0(d.(*metadata.Request))
			return &r
		}, nil
	case 1:
		return func(d interface{}) api.Request {
			return &metadata.RequestV1{RequestV0: metadata.NewRequestV0(d.(*metadata.Request))}
		}, nil
	case 2:
		return func(d interface{}) api.Request {
			return &metadata.RequestV2{RequestV0: metadata.NewRequestV0(d.(*metadata.Request))}
		}, nil
	case 3:
		return func(d interface{}) api.Request {
			return &metadata.RequestV3{RequestV0: metadata.NewRequestV0(d.(*metadata.Request))}
		}, nil
	case 4:
		return func(d interface{}) api.Request {
			r := metadata.NewRequestV4(d.(*metadata.Request))
			return &r
		}, nil
	case 5:
		return func(d interface{}) api.Request {
			return &metadata.RequestV5{RequestV4: metadata.NewRequestV4(d.(*metadata.Request))}
		}, nil
	case 6:
		return func(d interface{}) api.Request {
			return &metadata.RequestV6{RequestV4: metadata.NewRequestV4(d.(*metadata.Request))}
		}, nil
	case 7:
		return func(d interface{}) api.Request {
			return &metadata.RequestV7{RequestV4: metadata.NewRequestV4(d.(*metadata.Request))}
		}, nil
	}

	if minVersion <= 7 {
		return func(d interface{}) api.Request {
			return &metadata.RequestV7{RequestV4: metadata.NewRequestV4(d.(*metadata.Request))}
		}, nil
	}

	return nil, fmt.Errorf("metadata request for version range: %v to %v: %w", minVersion, maxVersion, ErrorUnsupportedAPI)
}
