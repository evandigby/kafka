package metadata

import (
	"io"
	"time"

	"github.com/evandigby/kafka/enc"
)

// Response is used to examine a metadata response by any API version
type Response interface {
	ThrottleTime() time.Duration
	ThrottleTimeSupported() bool
	Brokers() []BrokerResponse
	ClusterID() *string
	ClusterIDSupported() bool
	ControllerID() int32
	ControllerIDSupported() bool
	Topics() []TopicResponse
}

// ResponseV0 V0  response
type ResponseV0 struct {
	brokers []BrokerResponse
	topics  []TopicResponse
}

// ThrottleTime returns the Throttle Time for the response
func (r *ResponseV0) ThrottleTime() time.Duration { return 0 }

// ThrottleTimeSupported returns whether or not ThrottleTime is supported in this version
func (r *ResponseV0) ThrottleTimeSupported() bool { return false }

// Brokers returns the Brokers for the response
func (r *ResponseV0) Brokers() []BrokerResponse { return r.brokers }

// ClusterID returns the ClusterID for the response
func (r *ResponseV0) ClusterID() *string { return nil }

// ClusterIDSupported returns whether or not ClusterID is supported in this version
func (r *ResponseV0) ClusterIDSupported() bool { return false }

// ControllerID returns the ControllerID for the response
func (r *ResponseV0) ControllerID() int32 { return 0 }

// ControllerIDSupported returns whether or not ControllerID is supported in this version
func (r *ResponseV0) ControllerIDSupported() bool { return false }

// Topics returns the Topics for the response
func (r *ResponseV0) Topics() []TopicResponse { return r.topics }

// ResponseV1 V1  response
type ResponseV1 struct {
	ResponseV0
	controllerID int32
}

// ControllerID returns the ControllerID for the response
func (r *ResponseV1) ControllerID() int32 { return r.controllerID }

// ControllerIDSupported returns whether or not ControllerID is supported in this version
func (r *ResponseV1) ControllerIDSupported() bool { return true }

// ResponseV2 V2  response
type ResponseV2 struct {
	ResponseV1
	clusterID *string
}

// ClusterID returns the ClusterID for the response
func (r *ResponseV2) ClusterID() *string { return r.clusterID }

// ClusterIDSupported returns whether or not ClusterID is supported in this version
func (r *ResponseV2) ClusterIDSupported() bool { return true }

// ResponseV3 V3  response
type ResponseV3 struct {
	throttleTime time.Duration
	ResponseV2
}

// ThrottleTime returns the Throttle Time for the response
func (r *ResponseV3) ThrottleTime() time.Duration { return r.throttleTime }

// ThrottleTimeSupported returns whether or not ThrottleTime is supported in this version
func (r *ResponseV3) ThrottleTimeSupported() bool { return true }

// ReadResponse reads the correct metadata response for the specified version
func ReadResponse(r io.Reader, v int16) (Response, error) {
	switch v {
	case 0:
		return readResponseV0(r, v)
	case 1:
		return readResponseV1(r, v)
	case 2:
		return readResponseV2(r, v)
	case 3, 4, 5, 6, 7:
		return readResponseV3(r, v)
	default:
		panic("shouldn't happen")
	}
}

func readResponseV0(r io.Reader, v int16) (*ResponseV0, error) {
	var (
		resp ResponseV0
		err  error
	)

	resp.brokers, err = readBrokers(r, 0)
	if err != nil {
		return nil, err
	}

	resp.topics, err = readTopics(r, v)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func readResponseV1(r io.Reader, v int16) (*ResponseV1, error) {
	var (
		resp ResponseV1
		err  error
	)

	resp.brokers, err = readBrokers(r, 1)
	if err != nil {
		return nil, err
	}

	resp.controllerID, err = enc.ReadInt32(r)
	if err != nil {
		return nil, err
	}

	resp.topics, err = readTopics(r, v)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func readResponseV2(r io.Reader, v int16) (*ResponseV2, error) {
	var (
		resp ResponseV2
		err  error
	)

	resp.brokers, err = readBrokers(r, 1)
	if err != nil {
		return nil, err
	}

	resp.clusterID, err = enc.ReadNullableString(r)
	if err != nil {
		return nil, err
	}

	resp.controllerID, err = enc.ReadInt32(r)
	if err != nil {
		return nil, err
	}

	resp.topics, err = readTopics(r, v)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func readResponseV3(r io.Reader, v int16) (*ResponseV3, error) {
	var (
		resp ResponseV3
		err  error
	)

	resp.throttleTime, err = enc.ReadDuration(r)
	if err != nil {
		return nil, err
	}

	v2, err := readResponseV2(r, v)
	if err != nil {
		return nil, err
	}

	resp.ResponseV2 = *v2
	return &resp, nil
}
