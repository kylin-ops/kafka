package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

func main() {
	w := &kafka.Writer{
		Addr:     kafka.TCP("192.168.131.180:9092"),
		Topic:    "first",
		Balancer: &kafka.Hash{},
	}
	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
	fmt.Println(err)
}
