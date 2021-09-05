package kafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

func ConsumerGroup(addrs []string, topic, groupId string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  addrs,
		GroupID:  groupId,
		Topic:    topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer r.Close()
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}
