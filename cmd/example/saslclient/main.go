package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/evandigby/kafka"
)

func main() {
	host := os.Getenv("KAFKA_BOOTSTRAP")
	userName := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "   ")

	received := make(chan struct{})

	broker, err := kafka.NewBroker(host, kafka.BrokerConfig{
		SASL: kafka.SASLConfig{
			Enabled:  true,
			Mechaism: "PLAIN",
			Plain: kafka.SASLPlainConfig{
				UserName: userName,
				Password: password,
			},
		},
		OnResponse: func(key kafka.APIKey, version int16, resp interface{}) {
			enc.Encode(resp)
			close(received)
		},
	})
	if err != nil {
		fmt.Println("Connect error", err)
		return
	}

	defer broker.Close()

	err = broker.SendRequest(context.Background(), kafka.NewMetadataRequestV5([]string{}, false))
	if err != nil {
		fmt.Println("Send metadata error", err)
		return
	}

	<-received
}
