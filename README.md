# Kafka Playground for Azure Event Hubs

## tl;dr

[Azure Event Hubs Kafka](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview) doesn't work when using V1 SaslHandshake.

This code demonstrates a **working** authentication flow for [Azure Event Hubs Kafka](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview).

## Investigation

While attempting to connect to Azure Event Hubs' Kafka endpoint I encountered issues with all native-Go client implementations I tried:
- [Sarama](https://github.com/Shopify/sarama)
- [Segmentio Kafka Go](https://github.com/segmentio/kafka-go)

However, the one used in their examples works ([Confluent Kafka Go](https://github.com/confluentinc/confluent-kafka-go)), which is a light wrapper for [librdkafka](https://github.com/edenhill/librdkafka). The [console-producer that comes with kafka](https://kafka.apache.org/quickstart#quickstart_send) also seems to work.

Assuming that librdkafka and the console producer would be the most compliant clients I got to digging.

I'm not terribly confident with C/Java but to the best of my knowledge both librdkafka and the kafka console producer all use [V0 SaslAuthenticate](https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate) messages regardless of the reported capability or version of the Kafka client.

All of the native Go clients attempt to use [V1 SaslAuthenticate](https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate) messages; however, it doesn't work in Azure Event Hubs (nor does it seem to work in the confluent hosted Kafka offering--interesting).

## Diagnosis

Azure Event Hubs reports that it supports [V1 SaslHandshake](https://kafka.apache.org/protocol.html#The_Messages_SaslHandshake); however, when attempting to actually use it it abruptly closes the connection (the correct behavior for failed authentication according to the [Kafka Protocol spec](ttps://kafka.apache.org/protocol.html)).

## Solution

The solution is to always use V0 SaslHandshake/Auth until Azure Event Hubs fixes the V1 auth flow.

### Sarama Solution

The following commit forces the Sarama client to use V0 auth, temporarily fixing the problem:

- https://github.com/evandigby/sarama/commit/6509b6f9a616196089617e28f70ea2e21e8406ae



