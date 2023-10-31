package consumer

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
)

func (c *KafkaConsumer) processBatch(messages *[]MessageDetails, timer *time.Timer, batchSize, timeoutMs int, isBatchReady *bool, handler BatchMessageProcessor) error {
	// fmt.Println("Inside Process Batch")
	select {
	case <-timer.C:
		if len(*messages) > 0 {
			if err := handler(*messages); err != nil {
				return fmt.Errorf("error processing batch of messages: %v", err)
			}
			*messages = make([]MessageDetails, 0, batchSize)
		}
		timer.Reset(time.Duration(timeoutMs) * time.Millisecond)
	default:
	}
	return nil
}

func (c *KafkaConsumer) consumeMessage(messages *[]MessageDetails, batchSize int, isBatchReady *bool, handler BatchMessageProcessor) error {

	var event kafka.Event

	if consumerManager != nil && consumerManager.pollTimeoutMs != 0 {
		event = c.consumer.Poll(consumerManager.pollTimeoutMs)
	} else {
		event = c.consumer.Poll(10)
	}

	if event == nil { // we need to continue listening even after timeoutMs in Poll
		return nil
	}

	switch e := event.(type) {
	case *kafka.Message:

		data := MessageDetails{
			Topic:     *e.TopicPartition.Topic,
			Partition: e.TopicPartition.Partition,
			Message:   e.Value,
			Offset:    e.TopicPartition.Offset.String(),
			Key:       e.Key,
		}

		*messages = append(*messages, data)

		if len(*messages) >= int(batchSize) {
			*isBatchReady = true
			handler(*messages)
			*messages = make([]MessageDetails, 0, batchSize)
		}
	case *kafka.Error:
		return fmt.Errorf("consumer error: %v, Error Code: %v", e.Error(), e.Code())
	}
	return nil
}

func (c *KafkaConsumer) ConsumeMessagesInBatches(topics []string, batchSize, timeoutMs uint, handler BatchMessageProcessor) error {
	err := c.consumer.SubscribeTopics(topics, nil)
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	if err != nil {
		return err
	}

	messages := make([]MessageDetails, 0, batchSize)
	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	isBatchReady := false

	for {
		select {
		case <-signalChannel:
			c.Close()
			os.Exit(0)
		default:
			if err := c.consumeMessage(&messages, int(batchSize), &isBatchReady, handler); err != nil {
				return err
			}
		}

		if err := c.processBatch(&messages, timer, int(batchSize), int(timeoutMs), &isBatchReady, handler); err != nil {
			return err
		}
	}
}

func (c *KafkaConsumer) ConsumeBatchProtoMessage(topics []string, msg proto.Message, batchOpt *BatchConsumerOpt, callback BatchMessageProcessor) error {

	return c.ConsumeMessagesInBatches(topics, batchOpt.BatchSize, batchOpt.TimeoutMs, func(data []MessageDetails) error {
		parsedData := make([]MessageDetails, 0)
		for _, x := range data {
			err := c.deser.DeserializeInto(topics[0], x.Message, msg)
			if err != nil {
				return fmt.Errorf("error deserializing data: %w", err)
			}
			x.ProtoMsg = msg.(proto.Message)
			parsedData = append(parsedData, x)
		}

		if err := callback(parsedData); err != nil {
			return fmt.Errorf("error processing Proto message: %w", err)
		}
		return nil
	})
}
