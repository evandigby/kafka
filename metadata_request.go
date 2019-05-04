package kafka

import (
	"io"
)

// metadataRequestV5 a V5 metadata request
type metadataRequestV5 struct {
	topics             []string
	allowTopicCreation bool
}

func (r *metadataRequestV5) size() int32 {
	return stringArraySize(r.topics) + boolSize()
}

func (r *metadataRequestV5) write(w io.Writer) error {
	err := writeStringArray(w, r.topics)
	if err != nil {
		return err
	}

	return writeBool(w, r.allowTopicCreation)
}

// APIKey returns the API key for the request
func (r *metadataRequestV5) APIKey() APIKey { return APIKeyMetadata }

// Version returns the request version
func (r *metadataRequestV5) Version() int16 { return 5 }

// newMetadataRequestV5 creates a new V5 metadata request
func newMetadataRequestV5(topics []string, allowTopicCreation bool) *metadataRequestV5 {
	return &metadataRequestV5{
		topics:             topics,
		allowTopicCreation: allowTopicCreation,
	}
}
