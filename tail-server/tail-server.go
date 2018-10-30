package main

import (
	"flag"
	"fmt"

	"github.com/sciffer/tail/tail-server/tailserver"
)

var (
	version string

	brokerList = flag.String("brokers", "broker1:9092,broker2:9092,sbroker3:9092", "The comma separated list of brokers in the Kafka cluster")
	topic      = flag.String("topic", "tail", "the topic to consume")
	group      = flag.String("group", "tail", "The kafka group of the tail server cluster. within the same group ctail servers will shard the messages between themselves.")
	verbose    = flag.Bool("verbose", false, "Whether to turn on sarama logging")
	bufferSize = flag.Int("buffer-size", 256, "The buffer size of the message channel.")
	uri        = flag.String("uri", "/events", "The events URI prefix.")
	listen     = flag.String("listen", ":8080", "Endpoint to open for event streams.")
)

func main() {
	fmt.Printf("Tail-server %s\n", version)
	flag.Parse()

	server := ctailserver.NewCtailServer(*verbose, *bufferSize)
	server.InitConsumer(*brokerList, *topic, *group)
	server.StartHTTP(*uri, *listen)
	server.StartConsuming()
}
