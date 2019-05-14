package main

import (
	"context"
	"fmt"
	"os"

	"github.com/evandigby/kafka"
)

func main() {
	host := os.Getenv("KAFKA_BOOTSTRAP")
	userName := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")

	cluster, err := kafka.NewCluster(context.Background(), []string{host}, kafka.ClusterConfig{
		ClientID: "testClient",
		SASL: kafka.SASLConfig{
			Enabled:  true,
			Mechaism: "PLAIN",
			Plain: kafka.SASLPlainConfig{
				UserName: userName,
				Password: password,
			},
		},
		Events: kafka.ClusterEvents{
			OnBootstrapError: func(err error, server string, attempts int) {
				fmt.Printf("Error connecting to %q (%d attempts): %v\n", server, attempts, err)
			},
		},
	})
	if err != nil {
		fmt.Println("Connect error", err)
		return
	}

	defer cluster.Close()
}
