package kafka

import (
	"io"
	"time"
)

// For all response interfaces, features not supported by the API will return an "UnnsupportedAPIRequest" error

/*
// MetadataResponse represents a generic metadata response.
type MetadataResponse interface {
	ThrottleTime() (time.Duration, error)
	Brokers() ([]BrokerResponse, error)
	ClusterID() (*string, error)
	ControllerID() (int32, error)
	TopicMetadata() ([]TopicMetadataResponse, error)
}

// BrokerResponse represents broker provided in a response
type BrokerResponse interface {
	NodeID() (int32, error)
	Host() (string, error)
	Port() (string, error)
	Rack() (*string, error)
}

// TopicMetadataResponse represents topic metadata provided in a response
type TopicMetadataResponse interface {
	Error() error
	Topic() (string, error)
	IsInternal() (bool, error)
	PartitionMetadata() ([]PartitionMetadataResponse, error)
}

// PartitionMetadataResponse represents partition metadata provided in a response
type PartitionMetadataResponse interface {
	Error() error
	Partition() (int32, error)
	Leader() (int32, error)
	LeaderEpoch() (int32, error)
	Replicas() ([]int32, error)
	ISR() ([]int32, error)
	OfflineReplicas() ([]int32, error)
}
*/

// BrokerMetadataResponse broker information returned in a metadata call
type BrokerMetadataResponse struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   *string
}

// TopicMetadataResponse represents topic metadata provided in a response
type TopicMetadataResponse struct {
	Error             error
	Topic             string
	IsInternal        bool
	PartitionMetadata []PartitionMetadataResponse
}

// PartitionMetadataResponse represents partition metadata provided in a response
type PartitionMetadataResponse struct {
	Error           error
	Partition       int32
	Leader          int32
	LeaderEpoch     int32
	Replicas        []int32
	ISR             []int32
	OfflineReplicas []int32
}

// MetadataResponseV5 V5 metadata response
type MetadataResponseV5 struct {
	ThrottleTime  time.Duration
	Brokers       []BrokerMetadataResponse
	ClusterID     *string
	ControllerID  int32
	TopicMetadata []TopicMetadataResponse
}

func readMetadataResponse(v int16, r io.Reader) (interface{}, error) {
	switch v {
	case 5:
		return readMetadataResponseV5(r)
	default:
		return nil, newClientUnsupportedAPIError(APIKeyMetadata, v)
	}
}

func readMetadataResponseV5(r io.Reader) (*MetadataResponseV5, error) {
	var resp MetadataResponseV5

	throttleTime, err := readInt32(r)
	if err != nil {
		return nil, err
	}

	resp.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	arrLen, err := readInt32(r)
	if err != nil {
		return nil, err
	}

	resp.Brokers = make([]BrokerMetadataResponse, int(arrLen))
	for i := 0; i < int(arrLen); i++ {
		resp.Brokers[i].NodeID, err = readInt32(r)
		if err != nil {
			return nil, err
		}

		resp.Brokers[i].Host, err = readString(r)
		if err != nil {
			return nil, err
		}

		resp.Brokers[i].Port, err = readInt32(r)
		if err != nil {
			return nil, err
		}

		resp.Brokers[i].Rack, err = readNullableString(r)
		if err != nil {
			return nil, err
		}
	}

	resp.ClusterID, err = readNullableString(r)
	if err != nil {
		return nil, err
	}

	resp.ControllerID, err = readInt32(r)
	if err != nil {
		return nil, err
	}

	arrLen, err = readInt32(r)
	if err != nil {
		return nil, err
	}

	resp.TopicMetadata = make([]TopicMetadataResponse, int(arrLen))
	for i := 0; i < int(arrLen); i++ {
		resp.TopicMetadata[i].Error = ErrorFromReader(r)
		if resp.TopicMetadata[i].Error != nil {
			if _, ok := resp.TopicMetadata[i].Error.(*Error); !ok {
				return nil, err
			}
		}

		resp.TopicMetadata[i].Topic, err = readString(r)
		if err != nil {
			return nil, err
		}

		resp.TopicMetadata[i].IsInternal, err = readBool(r)
		if err != nil {
			return nil, err
		}

		partArrLen, err := readInt32(r)
		if err != nil {
			return nil, err
		}

		resp.TopicMetadata[i].PartitionMetadata = make([]PartitionMetadataResponse, int(partArrLen))
		for j := 0; j < int(partArrLen); j++ {
			resp.TopicMetadata[i].PartitionMetadata[j].Error = ErrorFromReader(r)
			if resp.TopicMetadata[i].PartitionMetadata[j].Error != nil {
				if _, ok := resp.TopicMetadata[i].PartitionMetadata[j].Error.(*Error); !ok {
					return nil, err
				}
			}

			resp.TopicMetadata[i].PartitionMetadata[j].Partition, err = readInt32(r)
			if err != nil {
				return nil, err
			}

			resp.TopicMetadata[i].PartitionMetadata[j].Leader, err = readInt32(r)
			if err != nil {
				return nil, err
			}

			repArrLen, err := readInt32(r)
			if err != nil {
				return nil, err
			}
			resp.TopicMetadata[i].PartitionMetadata[j].Replicas = make([]int32, int(repArrLen))
			for x := 0; x < int(repArrLen); x++ {
				resp.TopicMetadata[i].PartitionMetadata[j].Replicas[x], err = readInt32(r)
				if err != nil {
					return nil, err
				}
			}

			isrArrLen, err := readInt32(r)
			if err != nil {
				return nil, err
			}
			resp.TopicMetadata[i].PartitionMetadata[j].ISR = make([]int32, int(isrArrLen))
			for x := 0; x < int(isrArrLen); x++ {
				resp.TopicMetadata[i].PartitionMetadata[j].ISR[x], err = readInt32(r)
				if err != nil {
					return nil, err
				}
			}

			orArrLen, err := readInt32(r)
			if err != nil {
				return nil, err
			}
			resp.TopicMetadata[i].PartitionMetadata[j].ISR = make([]int32, int(orArrLen))
			for x := 0; x < int(orArrLen); x++ {
				resp.TopicMetadata[i].PartitionMetadata[j].OfflineReplicas[x], err = readInt32(r)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return &resp, nil
}
