package kafka

// ClusterConfig represents configuration for a Kafka cluster
type ClusterConfig struct {
	ClientID string
	SASL     SASLConfig
}
