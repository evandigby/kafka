package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/evandigby/kafka"
	"github.com/evandigby/kafka/api"
	"github.com/evandigby/kafka/api/metadata"
)

func main() {
	host := os.Getenv("KAFKA_BOOTSTRAP")
	userName := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")
	topics := os.Getenv("KAFKA_TOPICS")

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "   ")

	received := make(chan struct{})

	broker, err := kafka.NewBroker(context.Background(), host, kafka.BrokerConfig{
		ClientID: "testClient",
		SASL: kafka.SASLConfig{
			Enabled:  true,
			Mechaism: "PLAIN",
			Plain: kafka.SASLPlainConfig{
				UserName: userName,
				Password: password,
			},
		},
		Response: kafka.ResponseConfig{
			OnResponse: func(key api.Key, version int16, resp interface{}) {
				fmt.Println("Key", key)
				fmt.Println("Version", version)
			},
			OnMetadataResponse: func(version int16, resp metadata.Response) {
				defer close(received)
				fmt.Println("ThrottleTime:", resp.ThrottleTime())
				fmt.Println("ClusterID:", resp.ClusterID())
				fmt.Println("ControllerID:", resp.ControllerID())

				for _, b := range resp.Brokers() {
					fmt.Println("NodeID:", b.NodeID())
					fmt.Println("Host:", b.Host())
					fmt.Println("Port:", b.Port())
					fmt.Println("Rack:", b.Rack())
				}

				for _, t := range resp.Topics() {
					fmt.Println(t.Error())
					fmt.Println(t.Topic())

					for _, p := range t.Partitions() {
						fmt.Println(p.Error())
						fmt.Println(p.Partition())
						fmt.Println(p.Leader())
						fmt.Println(p.LeaderEpoch())
						fmt.Println(p.Replicas())
						fmt.Println(p.ISR())
						fmt.Println(p.OfflineReplicas())
					}
				}
			},
			OnResponseError: func(err error) {
				fmt.Println("Response error:", err)
				close(received)
			},
		},
	})
	if err != nil {
		fmt.Println("Connect error", err)
		return
	}

	defer broker.Close()

	err = broker.RequestMetadata(context.Background(), strings.Split(topics, ","), false)
	if err != nil {
		fmt.Println("Send metadata error", err)
		return
	}

	<-received
}
