package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/evandigby/kafka/api/metadata"
)

// ErrLeaderNotAvailable is returned when the leader for a topic/partition is not available
var ErrLeaderNotAvailable = errors.New("leader not available")

// Cluster represents a Kafka cluster
type Cluster struct {
	clientID string
	sasl     SASLConfig

	bootstrapServers []string

	lastMeta                 metadata.Response
	brokers                  map[int32]*Broker
	brokersMux               sync.RWMutex
	topicPartitionLeaders    map[string]map[int32]*Broker
	topicPartitionLeadersMux sync.RWMutex
}

// NewCluster creates a new Kafka cluster
func NewCluster(bootstrap []string, config ClusterConfig) (*Cluster, error) {
	c := &Cluster{
		bootstrapServers: bootstrap,
		clientID:         config.ClientID,
		sasl:             config.SASL,
	}

	err := c.bootstrap()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Cluster) bootstrap() error {
	for {
		for _, s := range c.bootstrapServers {
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

			err = c.updateBrokers(mresp, brokerConfig)
			if err != nil {
				fmt.Println("error updating brokers:", err)
				continue
			}

			err = c.updateTopicPartitionLeaders(mresp)
			if err != nil {
				fmt.Println("error updating topic partition leaders:", err)
				continue
			}

			return nil
		}
	}
}

func (c *Cluster) updateBrokers(meta metadata.Response, config BrokerConfig) error {
	type brokerConnect struct {
		nodeID int32
		broker *Broker
		err    error
	}

	brokerChan := make(chan brokerConnect)

	var wg sync.WaitGroup
	wg.Add(len(meta.Brokers()))
	go func() {
		defer close(brokerChan)
		wg.Wait()
	}()

	for _, b := range meta.Brokers() {
		go func(id int32, host string) {
			defer wg.Done()
			broker, err := NewBroker(host, config)
			brokerChan <- brokerConnect{
				broker: broker,
				err:    err,
				nodeID: id,
			}
		}(b.NodeID(), fmt.Sprintf("%v:%v", b.Host(), b.Port()))
	}

	var errs []error
	brokers := map[int32]*Broker{}
	for b := range brokerChan {
		if b.err != nil {
			errs = append(errs, b.err)
			continue
		}

		brokers[b.nodeID] = b.broker
	}

	if len(errs) > 0 {
		return errs[0] // TODO: find a better thing to return
	}

	c.brokersMux.Lock()
	defer c.brokersMux.Unlock()
	oldBrokers := c.brokers
	c.brokers = brokers

	if oldBrokers != nil {
		go func() {
			for _, b := range oldBrokers {
				b.Close()
			}
		}()
	}
	return nil
}

func (c *Cluster) updateTopicPartitionLeaders(meta metadata.Response) error {
	topicPartitionLeaders := map[string]map[int32]*Broker{}

	c.topicPartitionLeadersMux.Lock()
	defer c.topicPartitionLeadersMux.Unlock()

	c.brokersMux.RLock()
	defer c.brokersMux.RUnlock()
	for _, t := range meta.Topics() {
		topicInfo := map[int32]*Broker{}
		for _, p := range t.Partitions() {
			leader, ok := c.brokers[p.Leader()]
			if !ok {
				return fmt.Errorf("topic %v, partition %v: %w", t.Topic(), p.Partition(), ErrLeaderNotAvailable)
			}
			topicInfo[p.Partition()] = leader
		}

		topicPartitionLeaders[t.Topic()] = topicInfo
	}
	return nil
}

// Close closes the connection to all connected brokers
func (c *Cluster) Close() {
	c.brokersMux.Lock()
	defer c.brokersMux.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(c.brokers))
	for _, b := range c.brokers {
		go func(b *Broker) {
			defer wg.Done()
			b.Close()
		}(b)
	}
	wg.Wait()
}
