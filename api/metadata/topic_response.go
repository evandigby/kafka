package metadata

import (
	"io"

	"github.com/evandigby/kafka/api"
	"github.com/evandigby/kafka/api/enc"
)

// TopicResponse is used to examine the topic response from any API version
type TopicResponse interface {
	Error() error
	Topic() string
	IsInternal() bool
	IsInternalSupported() bool
	Partitions() []PartitionResponse
}

// TopicResponseV0 represents topic  provided in a response
type TopicResponseV0 struct {
	err        error
	topic      string
	partitions []PartitionResponse
}

// Error returns the Error for the response
func (t *TopicResponseV0) Error() error { return t.err }

// Topic returns the Topic for the response
func (t *TopicResponseV0) Topic() string { return t.topic }

// IsInternal returns the IsInternal for the response
func (t *TopicResponseV0) IsInternal() bool { return false }

// IsInternalSupported returns whether or not IsInternal is supported in this response
func (t *TopicResponseV0) IsInternalSupported() bool { return false }

// Partitions returns the Partitions for the response
func (t *TopicResponseV0) Partitions() []PartitionResponse { return t.partitions }

func (t *TopicResponseV0) Read(r io.Reader, v int16) error {
	t.err = api.ErrorFromReader(r)
	if t.err != nil && !api.IsKafkaError(t.err) {
		return t.err
	}

	var err error

	t.topic, err = enc.ReadString(r)
	if err != nil {
		return err
	}

	t.partitions, err = readPartitions(r, v)
	if err != nil {
		return err
	}

	return nil
}

// TopicResponseV1 represents topic  provided in a response
type TopicResponseV1 struct {
	TopicResponseV0
	isInternal bool
}

// IsInternal returns the IsInternal for the response
func (t *TopicResponseV1) IsInternal() bool { return t.isInternal }

// IsInternalSupported returns whether or not IsInternal is supported in this response
func (t *TopicResponseV1) IsInternalSupported() bool { return true }

func (t *TopicResponseV1) Read(r io.Reader, v int16) error {
	t.err = api.ErrorFromReader(r)
	if t.err != nil && !api.IsKafkaError(t.err) {
		return t.err
	}

	var err error

	t.topic, err = enc.ReadString(r)
	if err != nil {
		return err
	}

	t.isInternal, err = enc.ReadBool(r)
	if err != nil {
		return err
	}

	t.partitions, err = readPartitions(r, v)
	if err != nil {
		return err
	}

	return nil
}

func readTopics(r io.Reader, v int16) ([]TopicResponse, error) {
	switch v {
	case 0:
		return readTopicsV0(r, v)
	case 1, 2, 3, 4, 5, 6, 7:
		return readTopicsV1(r, v)
	default:
		panic("shouldn't happen")
	}
}

func readTopicsV0(r io.Reader, v int16) ([]TopicResponse, error) {
	var topics []TopicResponse

	err := enc.Array(r, v,
		func(l int) { topics = make([]TopicResponse, l) },
		func(i int) enc.ElementReader {
			val := &TopicResponseV0{}
			topics[i] = val
			return val
		},
	)
	return topics, err
}

func readTopicsV1(r io.Reader, v int16) ([]TopicResponse, error) {
	var topics []TopicResponse

	err := enc.Array(r, v,
		func(l int) { topics = make([]TopicResponse, l) },
		func(i int) enc.ElementReader {
			val := &TopicResponseV1{}
			topics[i] = val
			return val
		},
	)
	return topics, err
}
