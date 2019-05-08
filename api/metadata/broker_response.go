package metadata

import (
	"io"

	"github.com/evandigby/kafka/enc"
)

// BrokerResponse is used to examine the broker response from any API version
type BrokerResponse interface {
	NodeID() int32
	Host() string
	Port() int32
	Rack() *string
	IsRackSupported() bool
}

// BrokerResponseV0 broker information returned in a  call
type BrokerResponseV0 struct {
	nodeID int32
	host   string
	port   int32
}

// NodeID returns the NodeID for the broker response
func (b *BrokerResponseV0) NodeID() int32 { return b.nodeID }

// Host returns the Host for the broker response
func (b *BrokerResponseV0) Host() string { return b.host }

// Port returns the Port for the broker response
func (b *BrokerResponseV0) Port() int32 { return b.port }

// Rack returns the Rack for the broker response
func (b *BrokerResponseV0) Rack() *string { return nil }

// IsRackSupported returns whether or not Rack is supported in this version
func (b *BrokerResponseV0) IsRackSupported() bool { return false }

func (b *BrokerResponseV0) Read(r io.Reader, v int16) (err error) {
	b.nodeID, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	b.host, err = enc.ReadString(r)
	if err != nil {
		return err
	}

	b.port, err = enc.ReadInt32(r)
	return err
}

// BrokerResponseV1 broker information returned in a  call
type BrokerResponseV1 struct {
	BrokerResponseV0
	rack *string
}

// Rack returns the Rack for the broker response
func (b *BrokerResponseV1) Rack() *string { return b.rack }

// IsRackSupported returns whether or not Rack is supported in this version
func (b *BrokerResponseV1) IsRackSupported() bool { return false }

func (b *BrokerResponseV1) Read(r io.Reader, v int16) error {
	err := b.BrokerResponseV0.Read(r, v)
	if err != nil {
		return err
	}

	b.rack, err = enc.ReadNullableString(r)
	return err
}

func readBrokers(r io.Reader, v int16) ([]BrokerResponse, error) {
	switch v {
	case 0:
		return readBrokersV0(r, v)
	case 1, 2, 3, 4, 5, 6, 7:
		return readBrokersV1(r, v)
	default:
		panic("shouldn't happen")
	}
}

func readBrokersV0(r io.Reader, v int16) ([]BrokerResponse, error) {
	var brokers []BrokerResponse

	err := enc.Array(r, v,
		func(l int) { brokers = make([]BrokerResponse, l) },
		func(i int) enc.ElementReader {
			val := &BrokerResponseV0{}
			brokers[i] = val
			return val
		},
	)
	return brokers, err
}

func readBrokersV1(r io.Reader, v int16) ([]BrokerResponse, error) {
	var brokers []BrokerResponse

	err := enc.Array(r, v,
		func(l int) { brokers = make([]BrokerResponse, l) },
		func(i int) enc.ElementReader {
			val := &BrokerResponseV1{}
			brokers[i] = val
			return val
		},
	)
	return brokers, err
}
