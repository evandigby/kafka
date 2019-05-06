package metadata

import (
	"io"

	"github.com/evandigby/kafka/enc"
)

// BrokerResponseV0 broker information returned in a  call
type BrokerResponseV0 struct {
	NodeID int32
	Host   string
	Port   int32
}

func (b *BrokerResponseV0) Read(r io.Reader) (err error) {
	b.NodeID, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	b.Host, err = enc.ReadString(r)
	if err != nil {
		return err
	}

	b.Port, err = enc.ReadInt32(r)
	return err
}

// BrokerResponseV1 broker information returned in a  call
type BrokerResponseV1 struct {
	BrokerResponseV0
	Rack *string
}

// BrokerResponseV2 broker information returned in a  call
type BrokerResponseV2 struct{ BrokerResponseV1 }

// BrokerResponseV3 broker information returned in a  call
type BrokerResponseV3 struct{ BrokerResponseV1 }

// BrokerResponseV4 broker information returned in a  call
type BrokerResponseV4 struct{ BrokerResponseV1 }

// BrokerResponseV5 broker information returned in a  call
type BrokerResponseV5 struct{ BrokerResponseV1 }

// BrokerResponseV6 broker information returned in a  call
type BrokerResponseV6 struct{ BrokerResponseV1 }

// BrokerResponseV7 broker information returned in a  call
type BrokerResponseV7 struct{ BrokerResponseV1 }

func (b *BrokerResponseV1) Read(r io.Reader) (err error) {
	err = b.BrokerResponseV0.Read(r)
	if err != nil {
		return err
	}

	b.Rack, err = enc.ReadNullableString(r)
	return err
}
