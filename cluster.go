package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/evandigby/kafka/api/metadata"
)

// Cluster represents a Kafka cluster
type Cluster struct {
	clientID string
	sasl     SASLConfig

	brokers                  map[int32]*Broker
	brokersMux               sync.RWMutex
	topicPartitionLeaders    map[int32]map[string]*Broker
	topicPartitionLeadersMux sync.RWMutex
}

// NewCluster creates a new Kafka cluster
func NewCluster(bootstrap []string, config ClusterConfig) (*Cluster, error) {
	c := &Cluster{
		clientID: config.ClientID,
		sasl:     config.SASL,
	}

	err := c.bootstrap(bootstrap)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Cluster) bootstrap(servers []string) error {
	for {
		for _, s := range servers {
			var (
				mresp metadata.Response
				merr  error
			)

			metadataReceived := make(chan struct{})

			brokerConfig := BrokerConfig{
				ClientID: c.clientID,
				SASL:     c.sasl,
				Response: ResponseConfig{
					OnMetadataResponse: func(version int16, resp metadata.Response) {
						defer close(metadataReceived)
						mresp = resp
					},
					OnResponseError: func(err error) {
						defer close(metadataReceived)
						merr = err
					},
				},
			}

			b, err := NewBroker(s, brokerConfig)
			if err != nil {
				fmt.Println("Error creating broker:", err)
				continue
			}

			err = b.RequestMetadata(context.Background(), []string{}, false)
			if err != nil {
				fmt.Println("Error requesting metadata:", err)
				continue
			}

			<-metadataReceived
			if merr != nil {
				fmt.Println("Error in metadata response:", merr)
				continue
			}

			c.updateBrokers(mresp)
		}
	}
}

func (c *Cluster) updateBrokers(meta metadata.Response) {
	c.brokersMux.Lock()
	defer c.brokersMux.Unlock()

	brokers := map[int32]*Broker{}
	for _, b := range meta.Brokers() {
		var err error
		brokers[b.NodeID()], err = NewBroker(fmt.Sprintf("%v:%v", b.Host(), b.Port()), c BrokerConfig)
	}
}

func (c *Cluster) updateTopicPartitionLeaders(meta metadata.Response) {
	c.topicPartitionLeadersMux.Lock()
	defer c.topicPartitionLeadersMux.Unlock()
}
