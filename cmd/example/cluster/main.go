package main

import (
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

	cluster, err := kafka.NewCluster([]string{host}, kafka.ClusterConfig{
		ClientID: "testClient",
		SASL: kafka.SASLConfig{
			Enabled:  true,
			Mechaism: "PLAIN",
			Plain: kafka.SASLPlainConfig{
				UserName: userName,
				Password: password,
			},
		},
	})
	if err != nil {
		fmt.Println("Connect error", err)
		return
	}

	defer cluster.Close()
}
