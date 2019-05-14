package kafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/evandigby/kafka/api/metadata"
)

var (
	// ErrLeaderNotAvailable is returned when the leader for a topic/partition is not available
	ErrLeaderNotAvailable = errors.New("leader not available")

	// ErrUnableToBootstrap is returned when we are not able to bootstrap
	ErrUnableToBootstrap = errors.New("unable to bootstrap")

	// ErrMaxAttempts is returned when a retry reaches its maximum attempts
	ErrMaxAttempts = errors.New("maximum attempts reached")
)

// Cluster represents a Kafka cluster
type Cluster struct {
	clientID string
	sasl     SASLConfig

	bootstrapServers  []string
	topics            []string
	autoCreateTopics  bool
	maxBootstrapRetry int
	bootstrapBackoff  time.Duration

	atomicBrokers               atomic.Value // map[int32]*Broker
	atomicTopicPartitionLeaders atomic.Value // map[string]map[int32]*Broker

	// this mutex should only be used to prevent bootstrapping while closing
	bootstrapping sync.Mutex

	closed chan struct{}

	onBootstrapError OnBootstrapError
}

// NewCluster creates a new Kafka cluster
func NewCluster(ctx context.Context, bootstrap []string, config ClusterConfig) (*Cluster, error) {
	config = clusterConfigDefaults(config)

	c := &Cluster{
		bootstrapServers:  bootstrap,
		bootstrapBackoff:  config.BootstrapRetryBackoff,
		maxBootstrapRetry: config.MaxBootstrapRetries,
		clientID:          config.ClientID,
		sasl:              config.SASL,
		topics:            config.Topics,
		autoCreateTopics:  config.AutoCreateTopics,
		closed:            make(chan struct{}),

		onBootstrapError: config.Events.OnBootstrapError,
	}

	err := c.bootstrap(ctx)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Cluster) brokers() map[int32]*brokerConnect {
	v, ok := c.atomicBrokers.Load().(map[int32]*brokerConnect)
	if !ok {
		return map[int32]*brokerConnect{}
	}

	return v
}

func (c *Cluster) topicPartitionLeaders() map[string]map[int32]*brokerConnect {
	v, ok := c.atomicBrokers.Load().(map[string]map[int32]*brokerConnect)
	if !ok {
		return map[string]map[int32]*brokerConnect{}
	}

	return v
}

func (c *Cluster) bootstrapFromServer(ctx context.Context, server string) error {
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

	b, err := NewBroker(ctx, server, brokerConfig)
	if err != nil {
		return err
	}

	err = b.RequestMetadata(ctx, c.topics, c.autoCreateTopics)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-metadataReceived:
	}

	if merr != nil {
		return err
	}

	c.updateBrokers(ctx, mresp, brokerConfig)

	err = c.updateTopicPartitionLeaders(mresp)
	if err != nil {
		return err
	}

	return nil
}

func (c *Cluster) bootstrap(ctx context.Context) error {
	c.bootstrapping.Lock()
	defer c.bootstrapping.Unlock()

	return retry(ctx, c.maxBootstrapRetry, c.bootstrapBackoff,
		func(ctx context.Context, attempt int) error {
			for _, s := range c.bootstrapServers {
				err := c.bootstrapFromServer(ctx, s)
				if err != nil {
					c.onBootstrapError(err, s, attempt)
					continue
				}

				return nil
			}

			return ErrUnableToBootstrap
		})
}

type brokerConnect struct {
	nodeID int32
	broker *Broker
	err    error
}

func connectBrokers(ctx context.Context, maxAttempts int, backoff time.Duration, brokers []metadata.BrokerResponse, config BrokerConfig) <-chan *brokerConnect {
	brokerChan := make(chan *brokerConnect)

	var wg sync.WaitGroup
	wg.Add(len(brokers))
	go func() {
		defer close(brokerChan)
		wg.Wait()
	}()

	for _, b := range brokers {
		go func(id int32, host string) {
			defer wg.Done()
			err := retry(ctx, maxAttempts, backoff,
				func(ctx context.Context, attempt int) error {
					broker, err := NewBroker(ctx, host, config)
					if err != nil {
						return err
					}

					brokerChan <- &brokerConnect{
						broker: broker,
						err:    err,
						nodeID: id,
					}

					return nil
				})

			if err != nil {
				brokerChan <- &brokerConnect{
					broker: nil,
					err:    err,
					nodeID: id,
				}
			}
		}(b.NodeID(), fmt.Sprintf("%v:%v", b.Host(), b.Port()))
	}

	return brokerChan
}

func disconnectBrokers(brokers map[int32]*brokerConnect) {
	var wg sync.WaitGroup

	wg.Add(len(brokers))
	for _, b := range brokers {
		go func(b *brokerConnect) {
			defer wg.Done()
			if b.broker != nil {
				b.broker.Close()
			}
		}(b)
	}

	wg.Wait()
}

func (c *Cluster) updateBrokers(ctx context.Context, meta metadata.Response, config BrokerConfig) {
	brokerChan := connectBrokers(ctx, c.maxBootstrapRetry, c.bootstrapBackoff, meta.Brokers(), config)

	brokers := map[int32]*brokerConnect{}
	for b := range brokerChan {
		brokers[b.nodeID] = b
	}

	oldBrokers := c.brokers()

	c.atomicBrokers.Store(brokers)

	go disconnectBrokers(oldBrokers)
}

func (c *Cluster) updateTopicPartitionLeaders(meta metadata.Response) error {
	topicPartitionLeaders := map[string]map[int32]*brokerConnect{}

	brokers := c.brokers()

	for _, t := range meta.Topics() {
		topicInfo := map[int32]*brokerConnect{}
		for _, p := range t.Partitions() {
			leader, ok := brokers[p.Leader()]
			if !ok {
				return fmt.Errorf("topic %v, partition %v: %w", t.Topic(), p.Partition(), ErrLeaderNotAvailable)
			}
			topicInfo[p.Partition()] = leader
		}

		topicPartitionLeaders[t.Topic()] = topicInfo
	}

	c.atomicTopicPartitionLeaders.Store(topicPartitionLeaders)

	return nil
}

// Close closes the connection to all connected brokers
func (c *Cluster) Close() {
	c.bootstrapping.Lock()
	defer c.bootstrapping.Unlock()

	close(c.closed)

	disconnectBrokers(c.brokers())
}

func retry(
	ctx context.Context,
	maxRetry int,
	backoff time.Duration,

	f func(ctx context.Context, attempt int) error,
) error {
	for i := 0; i < maxRetry; i++ {
		err := f(ctx, i)
		if err == nil {
			return nil
		}

		backoff := math.Pow(2, float64(i)) - 1

		withJitter := (rand.Float64() * backoff * float64(backoff))

		t := time.NewTimer(time.Duration(withJitter))

		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}

	return fmt.Errorf("%v: %w", maxRetry, ErrMaxAttempts)
}
