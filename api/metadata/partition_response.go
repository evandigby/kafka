package metadata

import (
	"io"

	"github.com/evandigby/kafka/api/kafkaerror"
	"github.com/evandigby/kafka/enc"
)

// PartitionResponseV0 represents partition  provided in a response
type PartitionResponseV0 struct {
	Error     error
	Partition int32
	Leader    int32
	Replicas  []int32
	ISR       []int32
}

func (p *PartitionResponseV0) Read(r io.Reader) (err error) {
	p.Error = kafkaerror.FromReader(r)
	if p.Error != nil {
		if _, ok := p.Error.(*kafkaerror.Error); !ok {
			return p.Error
		}
	}

	p.Partition, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	p.Leader, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	repArrLen, err := enc.ReadInt32(r)
	if err != nil {
		return err
	}

	p.Replicas = make([]int32, int(repArrLen))
	for x := 0; x < int(repArrLen); x++ {
		p.Replicas[x], err = enc.ReadInt32(r)
		if err != nil {
			return err
		}
	}

	isrArrLen, err := enc.ReadInt32(r)
	if err != nil {
		return err
	}
	p.ISR = make([]int32, int(isrArrLen))
	for x := 0; x < int(isrArrLen); x++ {
		p.ISR[x], err = enc.ReadInt32(r)
		if err != nil {
			return err
		}
	}
	return nil
}

// PartitionResponseV1 represents partition  provided in a response
type PartitionResponseV1 struct{ PartitionResponseV0 }

// PartitionResponseV2 represents partition  provided in a response
type PartitionResponseV2 struct{ PartitionResponseV0 }

// PartitionResponseV3 represents partition  provided in a response
type PartitionResponseV3 struct{ PartitionResponseV0 }

// PartitionResponseV4 represents partition  provided in a response
type PartitionResponseV4 struct{ PartitionResponseV0 }

// PartitionResponseV5 represents partition  provided in a response
type PartitionResponseV5 struct {
	Error           error
	Partition       int32
	Leader          int32
	LeaderEpoch     int32
	Replicas        []int32
	ISR             []int32
	OfflineReplicas []int32
}

func (p *PartitionResponseV5) Read(r io.Reader) (err error) {
	p.Error = kafkaerror.FromReader(r)
	if p.Error != nil {
		if _, ok := p.Error.(*kafkaerror.Error); !ok {
			return p.Error
		}
	}

	p.Partition, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	p.Leader, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	p.LeaderEpoch, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	repArrLen, err := enc.ReadInt32(r)
	if err != nil {
		return err
	}

	p.Replicas = make([]int32, int(repArrLen))
	for x := 0; x < int(repArrLen); x++ {
		p.Replicas[x], err = enc.ReadInt32(r)
		if err != nil {
			return err
		}
	}

	isrArrLen, err := enc.ReadInt32(r)
	if err != nil {
		return err
	}
	p.ISR = make([]int32, int(isrArrLen))
	for x := 0; x < int(isrArrLen); x++ {
		p.ISR[x], err = enc.ReadInt32(r)
		if err != nil {
			return err
		}
	}

	orArrLen, err := enc.ReadInt32(r)
	if err != nil {
		return err
	}
	p.OfflineReplicas = make([]int32, int(orArrLen))
	for x := 0; x < int(isrArrLen); x++ {
		p.OfflineReplicas[x], err = enc.ReadInt32(r)
		if err != nil {
			return err
		}
	}
	return nil
}

// PartitionResponseV6 represents partition  provided in a response
type PartitionResponseV6 struct{ PartitionResponseV5 }

// PartitionResponseV7 represents partition  provided in a response
type PartitionResponseV7 struct{ PartitionResponseV5 }
