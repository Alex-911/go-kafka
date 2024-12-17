package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func ConnectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func main() {
	topic := "comments"

	worker, err := ConnectConsumer([]string{"localhost:29092"})

	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)

	if err != nil {
		panic(err)
	}

	log.Printf("Consumer Started")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				log.Printf("Received message Count: %d | Topic (%s) | Message (%s)\n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigChan:
				log.Println("Interruption Detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	log.Println("Procesed", msgCount, "messages")
	if err := worker.Close(); err != nil {
		panic(err)
	}
}
