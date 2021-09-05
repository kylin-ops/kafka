package kafka

import (
	"fmt"
	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	Address string
}

func TopicsList() {
	conn, err := kafka.Dial("tcp", "192.168.131.180:9092")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err)
	}
	for _, p := range partitions {
		fmt.Println(p.Topic)
	}
}
