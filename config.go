package kafka

import "time"

// SASLPlainConfig represents configuration required for SASLPlain mechanism
type SASLPlainConfig struct {
	UserName string
	Password string
}

// SASLConfig represents kafka SASL configuration
type SASLConfig struct {
	SASLVersion int16
	Enabled     bool
	Mechaism    string
	Plain       SASLPlainConfig
}

// OnResponse is called when a response is recieved
type OnResponse func(key APIKey, version int16, resp interface{})

// OnResponseError is called when there is an error recieving a response
type OnResponseError func(err error)

// BrokerConfig specifies configuration options for Kafka brokers
type BrokerConfig struct {
	ClientID         string
	RequestQueueSize int
	SASL             SASLConfig
	ReadTimeout      time.Duration
	SendTimeout      time.Duration

	OnResponse      OnResponse
	OnResponseError OnResponseError
}

func configDefaults(c BrokerConfig) BrokerConfig {
	if c.OnResponseError == nil {
		c.OnResponseError = func(err error) {}
	}

	if c.RequestQueueSize < 0 {
		c.RequestQueueSize = 0
	}

	if c.ReadTimeout <= 0 {
		c.ReadTimeout = time.Second * 5
	}

	if c.SendTimeout <= 0 {
		c.SendTimeout = time.Second * 5
	}

	return c
}
