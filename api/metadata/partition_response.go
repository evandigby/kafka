package metadata

import (
	"io"

	"github.com/evandigby/kafka/api"
	"github.com/evandigby/kafka/api/enc"
)

// PartitionResponse is used to examine the partition response from any API version
type PartitionResponse interface {
	Error() error
	Partition() int32
	Leader() int32
	LeaderEpoch() int32
	IsLeaderEpochSupported() bool
	Replicas() []int32
	ISR() []int32
	OfflineReplicas() []int32
	IsOfflineReplicasSupported() bool
}

// PartitionResponseV0 represents partition  provided in a response
type PartitionResponseV0 struct {
	err       error
	partition int32
	leader    int32
	replicas  []int32
	isr       []int32
}

// Error returns the Error for the response
func (p *PartitionResponseV0) Error() error { return p.err }

// Partition returns the Partition for the response
func (p *PartitionResponseV0) Partition() int32 { return p.partition }

// Leader returns the Leader for the response
func (p *PartitionResponseV0) Leader() int32 { return p.leader }

// LeaderEpoch returns the LeaderEpoch for the response
func (p *PartitionResponseV0) LeaderEpoch() int32 { return 0 }

// IsLeaderEpochSupported returns whether or not LeaderEpoch is supported in this version
func (p *PartitionResponseV0) IsLeaderEpochSupported() bool { return false }

// Replicas returns the Replicas for the response
func (p *PartitionResponseV0) Replicas() []int32 { return p.replicas }

// ISR returns the ISR for the response
func (p *PartitionResponseV0) ISR() []int32 { return p.isr }

// OfflineReplicas returns the OfflineReplicas for the response
func (p *PartitionResponseV0) OfflineReplicas() []int32 { return nil }

// IsOfflineReplicasSupported returns whether or not OfflineReplicas is supported in this version
func (p *PartitionResponseV0) IsOfflineReplicasSupported() bool { return false }

// Read reads the partition response from the reader
func (p *PartitionResponseV0) Read(r io.Reader, v int16) error {
	p.err = api.ErrorFromReader(r)
	if p.err != nil && !api.IsKafkaError(p.err) {
		return p.err
	}

	var err error

	p.partition, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	p.leader, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	p.replicas, err = enc.ReadInt32Array(r)
	if err != nil {
		return err
	}

	p.isr, err = enc.ReadInt32Array(r)
	if err != nil {
		return err
	}
	return nil
}

// PartitionResponseV5 represents partition  provided in a response
type PartitionResponseV5 struct {
	PartitionResponseV0
	offlineReplicas []int32
}

// OfflineReplicas returns the OfflineReplicas for the response
func (p *PartitionResponseV5) OfflineReplicas() []int32 { return p.offlineReplicas }

// IsOfflineReplicasSupported returns whether or not OfflineReplicas is supported in this version
func (p *PartitionResponseV5) IsOfflineReplicasSupported() bool { return true }

// Read reads the partition response from the reader
func (p *PartitionResponseV5) Read(r io.Reader, v int16) error {
	err := p.PartitionResponseV0.Read(r, v)
	if err != nil {
		return err
	}

	p.offlineReplicas, err = enc.ReadInt32Array(r)
	if err != nil {
		return err
	}

	return nil
}

// PartitionResponseV7 represents partition  provided in a response
type PartitionResponseV7 struct {
	PartitionResponseV5
	leaderEpoch int32
}

// LeaderEpoch returns the LeaderEpoch for the response
func (p *PartitionResponseV7) LeaderEpoch() int32 { return p.leaderEpoch }

// IsLeaderEpochSupported returns whether or not LeaderEpoch is supported in this version
func (p *PartitionResponseV7) IsLeaderEpochSupported() bool { return true }

// Read reads the partition response from the reader
func (p *PartitionResponseV7) Read(r io.Reader, v int16) error {
	p.err = api.ErrorFromReader(r)
	if p.err != nil && !api.IsKafkaError(p.err) {
		return p.err
	}

	var err error

	p.partition, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	p.leader, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	p.leaderEpoch, err = enc.ReadInt32(r)
	if err != nil {
		return err
	}

	p.replicas, err = enc.ReadInt32Array(r)
	if err != nil {
		return err
	}

	p.isr, err = enc.ReadInt32Array(r)
	if err != nil {
		return err
	}

	p.offlineReplicas, err = enc.ReadInt32Array(r)
	if err != nil {
		return err
	}

	return nil
}

func readPartitions(r io.Reader, v int16) ([]PartitionResponse, error) {
	switch v {
	case 0, 1, 2, 3, 4:
		return readPartitionsV0(r, v)
	case 5, 6:
		return readPartitionsV5(r, v)
	case 7:
		return readPartitionsV7(r, v)
	default:
		panic("shouldn't happen")
	}
}

func readPartitionsV0(r io.Reader, v int16) ([]PartitionResponse, error) {
	var partitions []PartitionResponse

	err := enc.Array(r, v,
		func(l int) { partitions = make([]PartitionResponse, l) },
		func(i int) enc.ElementReader {
			val := &PartitionResponseV0{}
			partitions[i] = val
			return val
		},
	)
	return partitions, err
}

func readPartitionsV5(r io.Reader, v int16) ([]PartitionResponse, error) {
	var partitions []PartitionResponse

	err := enc.Array(r, v,
		func(l int) { partitions = make([]PartitionResponse, l) },
		func(i int) enc.ElementReader {
			val := &PartitionResponseV5{}
			partitions[i] = val
			return val
		},
	)
	return partitions, err
}

func readPartitionsV7(r io.Reader, v int16) ([]PartitionResponse, error) {
	var partitions []PartitionResponse

	err := enc.Array(r, v,
		func(l int) { partitions = make([]PartitionResponse, l) },
		func(i int) enc.ElementReader {
			val := &PartitionResponseV7{}
			partitions[i] = val
			return val
		},
	)
	return partitions, err
}
