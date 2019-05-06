package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/evandigby/kafka"
	"github.com/evandigby/kafka/api"
)

func main() {
	host := os.Getenv("KAFKA_BOOTSTRAP")
	userName := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "   ")

	received := make(chan struct{})

	broker, err := kafka.NewBroker(host, kafka.BrokerConfig{
		ClientID: "testClient",
		SASL: kafka.SASLConfig{
			Enabled:  true,
			Mechaism: "PLAIN",
			Plain: kafka.SASLPlainConfig{
				UserName: userName,
				Password: password,
			},
		},
		OnResponse: func(key api.Key, version int16, resp interface{}) {
			fmt.Println("Key", key)
			fmt.Println("Version", version)
			enc.Encode(resp)
			close(received)
		},
		OnResponseError: func(err error) {
			fmt.Println("Response error:", err)
			close(received)
		},
	})
	if err != nil {
		fmt.Println("Connect error", err)
		return
	}

	defer broker.Close()

	err = broker.RequestMetadata(context.Background(), []string{}, false)
	if err != nil {
		fmt.Println("Send metadata error", err)
		return
	}

	<-received
}
