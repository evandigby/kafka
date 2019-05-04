package kafka

import (
	"io"
)

// MetadataRequestV5 a V5 metadata request
type MetadataRequestV5 struct {
	topics             []string
	allowTopicCreation bool
}

func (r *MetadataRequestV5) size() int32 {
	return stringArraySize(r.topics) + boolSize()
}

func (r *MetadataRequestV5) write(w io.Writer) error {
	err := writeStringArray(w, r.topics)
	if err != nil {
		return err
	}

	return writeBool(w, r.allowTopicCreation)
}

// APIKey returns the API key for the request
func (r *MetadataRequestV5) APIKey() APIKey { return APIKeyMetadata }

// Version returns the request version
func (r *MetadataRequestV5) Version() int16 { return 5 }

// NewMetadataRequestV5 creates a new V5 metadata request
func NewMetadataRequestV5(topics []string, allowTopicCreation bool) *MetadataRequestV5 {
	return &MetadataRequestV5{
		topics:             topics,
		allowTopicCreation: allowTopicCreation,
	}
}
