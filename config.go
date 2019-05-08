package kafka

import (
	"time"

	"github.com/evandigby/kafka/api"
	"github.com/evandigby/kafka/api/metadata"
)

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

type (
	// OnMetadataResponse is called when a metadata response is recieved
	OnMetadataResponse func(version int16, resp metadata.Response)
	// OnResponse is called when a response is recieved
	OnResponse func(key api.Key, version int16, resp interface{})
	// OnResponseError is called when there is an error recieving a response
	OnResponseError func(err error)
)

// ResponseConfig contains all of the responses that can be handled
type ResponseConfig struct {
	OnMetadataResponse OnMetadataResponse
	OnResponse         OnResponse
	OnResponseError    OnResponseError
}

// BrokerConfig specifies configuration options for Kafka brokers
type BrokerConfig struct {
	ClientID         string
	RequestQueueSize int
	SASL             SASLConfig
	ReadTimeout      time.Duration
	SendTimeout      time.Duration
	Response         ResponseConfig
}

func defaultResposneConfig(c ResponseConfig) ResponseConfig {
	onResp := c.OnResponse
	if onResp == nil {
		onResp = func(key api.Key, version int16, resp interface{}) {}
	}

	c.OnResponse = func(key api.Key, version int16, resp interface{}) {
		onResp(key, version, resp)

		switch key {
		case api.KeyMetadata:
			c.OnMetadataResponse(version, resp.(metadata.Response))
		}
	}

	if c.OnResponseError == nil {
		c.OnResponseError = func(err error) {}
	}
	return c
}

func configDefaults(c BrokerConfig) BrokerConfig {
	c.Response = defaultResposneConfig(c.Response)

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
