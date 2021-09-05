package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d key:%s value:%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	config := mocks.NewTestConfig()
	config.Version = sarama.V2_8_0_0 // specify appropriate version
	config.Consumer.Return.Errors = true

	group, err := sarama.NewConsumerGroup([]string{"192.168.131.180:9092"}, "first-1", config)
	if err != nil {
		panic(err)
	}
	defer group.Close()
	err = group.Consume(context.Background(), []string{"first"}, exampleConsumerGroupHandler{})
	fmt.Println(err)
}
