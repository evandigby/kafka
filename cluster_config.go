package kafka

import "time"

// ClusterConfig represents configuration for a Kafka cluster
type ClusterConfig struct {
	ClientID              string
	MaxBootstrapRetries   int
	BootstrapRetryBackoff time.Duration
	SASL                  SASLConfig
	Events                ClusterEvents
	Topics                []string
	AutoCreateTopics      bool
}

type (
	// OnBootstrapError is raised when a cluster has trouble bootstrapping
	OnBootstrapError func(err error, server string, attempts int)
)

// ClusterEvents are events that are called during cluster bootstrapping
type ClusterEvents struct {
	OnBootstrapError OnBootstrapError
}

func clusterEventsDefaults(c ClusterEvents) ClusterEvents {
	if c.OnBootstrapError == nil {
		c.OnBootstrapError = func(err error, server string, attempts int) {}
	}

	return c
}

func clusterConfigDefaults(c ClusterConfig) ClusterConfig {
	if c.MaxBootstrapRetries < 1 {
		c.MaxBootstrapRetries = 1
	}

	if c.BootstrapRetryBackoff < 1 {
		c.BootstrapRetryBackoff = time.Millisecond * 100
	}

	c.Events = clusterEventsDefaults(c.Events)

	return c
}
