package main

import "github.com/kylin-ops/kafka"

//func main() {
//	r := kafka.NewReader(kafka.ReaderConfig{
//		Brokers:   []string{"192.168.131.180:9092"},
//		GroupID:   "consumer-group-id",
//		Topic:     "first",
//		MinBytes:  10e3, // 10KB
//		MaxBytes:  10e6, // 10MB
//	})
//
//	for {
//		m, err := r.ReadMessage(context.Background())
//		if err != nil {
//			break
//		}
//		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
//	}
//
//	if err := r.Close(); err != nil {
//		log.Fatal("failed to close reader:", err)
//	}
//}

func main() {
	kafka.ConsumerGroup([]string{"192.168.131.180:9092"}, "first", "this_my_groupid")
}
