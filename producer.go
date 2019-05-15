package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/globalsign/mgo"
	"github.com/rwynn/gtm"
)

func main() {
	url := "mongodb://localhost:27017"

	session, err := mgo.Dial(url)
	if err != nil {
		panic(err)
	}

	fmt.Println(session)

	session.SetMode(mgo.Monotonic, true)

	ctx := gtm.Start(session, &gtm.Options{
		WorkerCount: 1,
	})

	broker := "localhost:9092"
	topic := "test-topic"

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"enable.idempotence": true,
	})

	if err != nil {
		fmt.Println("Error creating producer", err)
		os.Exit(1)
	}

	doneChan := make(chan bool)

	go func() {
		defer close(doneChan)
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				return

			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	for {
		select {
		case err := <-ctx.ErrC:
			fmt.Println(err)

		case op := <-ctx.OpC:
			if op.IsInsert() {
				value := fmt.Sprintf("%s", op.Data)
				fmt.Println(value)
				p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(value)}
			}
		}
	}

	// _ = <-doneChan

	// p.Close()
}
