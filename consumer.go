package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	broker := "localhost:9092"
	topic := []string{"test-topic"}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"auto.offset.reset":               "earliest",
		"group.id":                        "test-consumer",
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
	})

	if err != nil {
		fmt.Println("error in creating consumer", err)
		os.Exit(1)
	}

	err = c.SubscribeTopics(topic, nil)

	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Println("caught signal", sig)
			run = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()
			case *kafka.Message:
				fmt.Println("Message",
					e.TopicPartition, string(e.Value))
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}

	c.Close()
}
