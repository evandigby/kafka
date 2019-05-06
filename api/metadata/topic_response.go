package metadata

import (
	"io"

	"github.com/evandigby/kafka/api/kafkaerror"
	"github.com/evandigby/kafka/enc"
)

// TopicResponseV0 represents topic  provided in a response
type TopicResponseV0 struct {
	Error     error
	Topic     string
	Partition []PartitionResponseV0
}

func (t *TopicResponseV0) Read(r io.Reader) (err error) {
	t.Error = kafkaerror.FromReader(r)
	if t.Error != nil {
		if _, ok := t.Error.(*kafkaerror.Error); !ok {
			return err
		}
	}

	t.Topic, err = enc.ReadString(r)
	if err != nil {
		return err
	}

	partArrLen, err := enc.ReadInt32(r)
	if err != nil {
		return err
	}

	t.Partition = make([]PartitionResponseV0, int(partArrLen))
	for j := 0; j < int(partArrLen); j++ {
		err = t.Partition[j].Read(r)
		if err != nil {
			return err
		}
	}

	return nil
}

// TopicResponseV1 represents topic  provided in a response
type TopicResponseV1 struct {
	TopicResponseV0
}

// TopicResponseV2 represents topic  provided in a response
type TopicResponseV2 struct{ TopicResponseV0 }

// TopicResponseV3 represents topic  provided in a response
type TopicResponseV3 struct{ TopicResponseV0 }

// TopicResponseV4 represents topic  provided in a response
type TopicResponseV4 struct{ TopicResponseV0 }

// TopicResponseV5 represents topic  provided in a response
type TopicResponseV5 struct{ TopicResponseV0 }

// TopicResponseV6 represents topic  provided in a response
type TopicResponseV6 struct{ TopicResponseV0 }

// TopicResponseV7 represents topic  provided in a response
type TopicResponseV7 struct{ TopicResponseV0 }
