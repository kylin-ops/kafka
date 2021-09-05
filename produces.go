package kafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

type Balance string
type Ack int

const (
	BalancerHash       Balance = "hash"
	BalancerLeastBytes Balance = "leastBytes"
	BalancerRoundRobin Balance = "roundRobin"
	BalancerCRC32      Balance = "crc32"
	AckNone            Ack     = 0
	AckOne             Ack     = 1
	AckAll             Ack     = -1
)

func NewWriter(address string, balance Balance, ack Ack, topics ...string) *Produces {
	var balancer kafka.Balancer
	var requireAck kafka.RequiredAcks
	switch balance {
	case BalancerHash:
		balancer = kafka.Balancer(&kafka.RoundRobin{})
	case BalancerLeastBytes:
		balancer = kafka.Balancer(&kafka.LeastBytes{})
	case BalancerRoundRobin:
		balancer = kafka.Balancer(&kafka.RoundRobin{})
	case BalancerCRC32:
		balancer = kafka.Balancer(&kafka.CRC32Balancer{})
	}
	switch ack {
	case AckNone:
		requireAck = kafka.RequireNone
	case AckOne:
		requireAck = kafka.RequireOne
	case AckAll:
		requireAck = kafka.RequireAll
	}
	return &Produces{
		Topics: topics,
		writer: &kafka.Writer{
			Addr: kafka.TCP(address),
			// Balancer: &kafka.LeastBytes{},  // &kafka.Hash{}
			Balancer:     balancer,
			RequiredAcks: requireAck,
		},
	}
}

type Produces struct {
	Topics []string
	writer *kafka.Writer
}

// https://github.com/segmentio/kafka-go
func (p *Produces) WriteMultipleTopicsMessage(key, value string) error {
	var messages []kafka.Message
	for _, topic := range p.Topics {
		messages = append(messages, kafka.Message{
			Topic: topic,
			Key:   []byte(key),
			Value: []byte(value),
		})
	}
	fmt.Println(messages)
	return p.writer.WriteMessages(context.Background(), messages...)
}

func (p *Produces) WriteSingleTopicMessage(topic, key, value string) error {
	return p.writer.WriteMessages(context.Background(), kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(value),
	})
}

func (p *Produces) Close() {
	p.writer.Close()
}
