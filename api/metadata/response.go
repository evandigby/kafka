package metadata

import (
	"io"
	"time"

	"github.com/evandigby/kafka/enc"
)

// ResponseV0 V0  response
type ResponseV0 struct {
	Brokers []BrokerResponseV0
	Topic   []TopicResponseV0
}

// ResponseV1 V1  response
type ResponseV1 struct {
	Brokers      []BrokerResponseV1
	ControllerID int32
	Topic        []TopicResponseV1
}

// ResponseV2 V2  response
type ResponseV2 struct {
	Brokers      []BrokerResponseV2
	ClusterID    *string
	ControllerID int32
	Topic        []TopicResponseV2
}

// ResponseV5 V5  response
type ResponseV5 struct {
	ThrottleTime time.Duration
	Brokers      []BrokerResponseV1
	ClusterID    *string
	ControllerID int32
	Topic        []TopicResponseV1
}

// ResponseV7 V7  response
type ResponseV7 struct {
	ResponseV5
}

func ReadResponse(v int16, r io.Reader) (interface{}, error) {
	switch v {
	case 0:
		return readResponseV0(r)
	case 1:
		return readResponseV1(r)
	// case 5:
	// 	return readResponseV5(r)
	// case 7:
	// 	v5, err := readResponseV5(r)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return ResponseV7{ResponseV5: *v5}, nil
	default:
		panic("shouldn't happen")
	}
}

func readResponseV0(r io.Reader) (*ResponseV0, error) {
	var resp ResponseV0

	arrLen, err := enc.ReadInt32(r)
	if err != nil {
		return nil, err
	}

	resp.Brokers = make([]BrokerResponseV0, int(arrLen))
	for i := 0; i < int(arrLen); i++ {
		err = resp.Brokers[i].Read(r)
		if err != nil {
			return nil, err
		}
	}

	arrLen, err = enc.ReadInt32(r)
	if err != nil {
		return nil, err
	}

	resp.Topic = make([]TopicResponseV0, int(arrLen))
	for i := 0; i < int(arrLen); i++ {
		err = resp.Topic[i].Read(r)
		if err != nil {
			return nil, err
		}
	}

	return &resp, nil
}

func readResponseV1(r io.Reader) (*ResponseV1, error) {
	var resp ResponseV1

	arrLen, err := enc.ReadInt32(r)
	if err != nil {
		return nil, err
	}

	resp.Brokers = make([]BrokerResponseV1, int(arrLen))
	for i := 0; i < int(arrLen); i++ {
		err = resp.Brokers[i].Read(r)
		if err != nil {
			return nil, err
		}
	}

	resp.ControllerID, err = enc.ReadInt32(r)
	if err != nil {
		return nil, err
	}

	arrLen, err = enc.ReadInt32(r)
	if err != nil {
		return nil, err
	}

	resp.Topic = make([]TopicResponseV1, int(arrLen))
	for i := 0; i < int(arrLen); i++ {
		err = resp.Topic[i].Read(r)
		if err != nil {
			return nil, err
		}
	}

	return &resp, nil
}

// func readResponseV5(r io.Reader) (*ResponseV5, error) {
// 	var resp ResponseV5

// 	throttleTime, err := enc.ReadInt32(r)
// 	if err != nil {
// 		return nil, err
// 	}

// 	resp.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

// 	arrLen, err := enc.ReadInt32(r)
// 	if err != nil {
// 		return nil, err
// 	}

// 	resp.Brokers = make([]BrokerResponse, int(arrLen))
// 	for i := 0; i < int(arrLen); i++ {
// 		resp.Brokers[i].NodeID, err = enc.ReadInt32(r)
// 		if err != nil {
// 			return nil, err
// 		}

// 		resp.Brokers[i].Host, err = readString(r)
// 		if err != nil {
// 			return nil, err
// 		}

// 		resp.Brokers[i].Port, err = enc.ReadInt32(r)
// 		if err != nil {
// 			return nil, err
// 		}

// 		resp.Brokers[i].Rack, err = readNullableString(r)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}

// 	resp.ClusterID, err = readNullableString(r)
// 	if err != nil {
// 		return nil, err
// 	}

// 	resp.ControllerID, err = enc.ReadInt32(r)
// 	if err != nil {
// 		return nil, err
// 	}

// 	arrLen, err = enc.ReadInt32(r)
// 	if err != nil {
// 		return nil, err
// 	}

// 	resp.Topic = make([]TopicResponse, int(arrLen))
// 	for i := 0; i < int(arrLen); i++ {
// 		t.Error = ErrorFromReader(r)
// 		if t.Error != nil {
// 			if _, ok := t.Error.(*Error); !ok {
// 				return nil, err
// 			}
// 		}

// 		t.Topic, err = readString(r)
// 		if err != nil {
// 			return nil, err
// 		}

// 		t.IsInternal, err = readBool(r)
// 		if err != nil {
// 			return nil, err
// 		}

// 		partArrLen, err := enc.ReadInt32(r)
// 		if err != nil {
// 			return nil, err
// 		}

// 		t.Partition = make([]PartitionResponse, int(partArrLen))
// 		for j := 0; j < int(partArrLen); j++ {
// 			p.Error = ErrorFromReader(r)
// 			if p.Error != nil {
// 				if _, ok := p.Error.(*Error); !ok {
// 					return nil, err
// 				}
// 			}

// 			p.Partition, err = enc.ReadInt32(r)
// 			if err != nil {
// 				return nil, err
// 			}

// 			p.Leader, err = enc.ReadInt32(r)
// 			if err != nil {
// 				return nil, err
// 			}

// 			repArrLen, err := enc.ReadInt32(r)
// 			if err != nil {
// 				return nil, err
// 			}
// 			p.Replicas = make([]int32, int(repArrLen))
// 			for x := 0; x < int(repArrLen); x++ {
// 				p.Replicas[x], err = enc.ReadInt32(r)
// 				if err != nil {
// 					return nil, err
// 				}
// 			}

// 			isrArrLen, err := enc.ReadInt32(r)
// 			if err != nil {
// 				return nil, err
// 			}
// 			p.ISR = make([]int32, int(isrArrLen))
// 			for x := 0; x < int(isrArrLen); x++ {
// 				p.ISR[x], err = enc.ReadInt32(r)
// 				if err != nil {
// 					return nil, err
// 				}
// 			}

// 			orArrLen, err := enc.ReadInt32(r)
// 			if err != nil {
// 				return nil, err
// 			}
// 			p.OfflineReplicas = make([]int32, int(orArrLen))
// 			for x := 0; x < int(orArrLen); x++ {
// 				p.OfflineReplicas[x], err = enc.ReadInt32(r)
// 				if err != nil {
// 					return nil, err
// 				}
// 			}
// 		}
// 	}

// 	return &resp, nil
// }
