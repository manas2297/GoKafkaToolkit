package consumer

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/proto"
)

type ConsumerManager struct {
	consumers     map[string][]*KafkaConsumer
	mutex         sync.Mutex
	paramStore    map[string]interface{}
	pollTimeoutMs int
}

var consumerManager *ConsumerManager

func consumerHelper(consumerCount int, topics []string, topicParams *ConsumerParams, topicHandlers MessageProcessor) ([]*KafkaConsumer, error) {
	consumers := make([]*KafkaConsumer, consumerCount)

	for i := 0; i < consumerCount; i++ {
		topic := topics
		paramsCopy := *topicParams
		consumer, err := NewKafkaConsumer(&paramsCopy)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer for topic %s: %w", topic, err)
		}
		consumers[i] = consumer
	}
	return consumers, nil
}

func (m *ConsumerManager) storeTopicParamHandlers(topics []string, tph TopicParamsHandlers) {
	for _, topic := range topics {
		m.paramStore[topic] = tph
	}
}

func (m *ConsumerManager) StartConsumers(topics ...string) {
	m.mutex.Lock()

	defer m.mutex.Unlock()

	if len(topics) == 0 {
		for topic := range m.paramStore {
			topics = append(topics, topic)
		}
	}
	var wg sync.WaitGroup

	for _, tp := range topics {
		tph, ok := m.paramStore[tp].(TopicParamsHandlers)
		if !ok {
			fmt.Printf("No handlers found for topic: %s\n", tp)
			continue
		}
		if consumers, ok := m.consumers[tp]; ok {
			for _, consumer := range consumers {
				wg.Add(1)
				go func(c *KafkaConsumer, tpc string, tphandler TopicParamsHandlers) {
					fmt.Println("Consumer Started for: ", tpc)
					var err error
					if tph.ProtoMsg != nil {
						err = c.ConsumeProtoMessage([]string{tpc}, tphandler.ProtoMsg, tph.Handler)
					} else if tph.AvroMsg != nil {
						err = c.ConsumeAvroMessage([]string{tpc}, tphandler.AvroMsg, tph.Handler)
					} else {
						err = c.ConsumeMessages([]string{tpc}, tph.Handler)
					}
					if err != nil {
						fmt.Printf("Error consuming messages for topic %s: %v\n", tpc, err)
					}
				}(consumer, tp, tph)
			}
		} else {
			fmt.Println("No consumers found for topic: %w\n", tp)
		}
	}
	wg.Wait()
}

func (m *ConsumerManager) ShutDown() {
	for _, consumers := range m.consumers {
		for _, consumer := range consumers {
			consumer.Close()
		}
	}
	os.Exit(0)
}

func (m *ConsumerManager) CreateConsumers(topicParamsHandlers []TopicParamsHandlers) error {
	m.mutex.Lock()

	defer m.mutex.Unlock()

	m.paramStore = make(map[string]interface{})
	for _, tph := range topicParamsHandlers {
		topicParams := tph.Params
		topicHandlers := tph.Handler
		setConsumerParams(topicParams)
		topicPartitions, err := getTopicPartitions(tph.Topics)
		if err != nil {
			return fmt.Errorf("failed to get topic partitions: %w", err)
		}
		topics := tph.Topics

		m.storeTopicParamHandlers(topics, tph)

		if len(topics) > 1 {
			consumers, err := consumerHelper(topicParams.ConsumerCount, topics, topicParams, topicHandlers)
			if err != nil {
				return err
			}
			for _, topic := range tph.Topics {
				m.consumers[topic] = consumers
			}
			return nil
		}

		noOfConsumers := len(topicPartitions)
		if topicParams.ConsumerCount <= len(topicPartitions) {
			noOfConsumers = topicParams.ConsumerCount
		}
		consumers, err := consumerHelper(noOfConsumers, topics, topicParams, topicHandlers)
		if err != nil {
			return fmt.Errorf("Error while creating consumers %v", err)
		}
		for _, topic := range tph.Topics {
			m.consumers[topic] = consumers
		}
	}

	return nil
}

func NewConsumerManager(timeoutMs int) *ConsumerManager {
	consumerManager = &ConsumerManager{
		consumers:     make(map[string][]*KafkaConsumer),
		pollTimeoutMs: timeoutMs,
	}

	return consumerManager
}

func getTopicPartitions(topics []string) ([]kafka.TopicPartition, error) {
	config, err := getAdminConfigFromParams(consumerParams)
	if err != nil {
		return nil, err
	}

	adminClient, err := kafka.NewAdminClient(config)

	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka admin client: %w", err)
	}
	defer adminClient.Close()

	var topicPartitions []kafka.TopicPartition

	for _, topic := range topics {
		metadata, err := adminClient.GetMetadata(&topic, false, 5000)
		if err != nil {
			return nil, fmt.Errorf("failed to get metadata for topic '%s': %w", topic, err)
		}

		for _, tp := range metadata.Topics[topic].Partitions {
			topicPartitions = append(topicPartitions, kafka.TopicPartition{
				Topic:     &topic,
				Partition: tp.ID,
			})
		}
	}

	return topicPartitions, nil
}

func (c *KafkaConsumer) PartitionCount(topic string) int {
	partitions, err := getTopicPartitions([]string{topic})
	if err != nil {
		fmt.Println("Can not get topic partitions")
	}
	return len(partitions)
}

func NewKafkaConsumer(params *ConsumerParams) (*KafkaConsumer, error) {
	config, err := getConfigFromParams(params)
	if err != nil {
		return nil, err
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, fmt.Errorf("Failed to create Kafka consumer: %w", err)
	}

	var schema_config *schemaregistry.Config

	if !params.SecurityParams {
		schema_config = schemaregistry.NewConfig(params.SchemaRegistryUrl)
	} else {
		schema_config = schemaregistry.NewConfigWithAuthentication(params.SchemaRegistryUrl, params.RegistryUsername, params.RegistryPassword)
	}

	client, err := schemaregistry.NewClient(schema_config)

	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		return nil, err
	}

	deser, err := protobuf.NewDeserializer(client, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}

	avroDeser, err := avro.NewGenericDeserializer(client, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}

	return &KafkaConsumer{
		consumer:       consumer,
		schemaRegistry: &client,
		deser:          deser,
		avroDeser:      avroDeser,
	}, nil
}

func (c *KafkaConsumer) AlterConsumerGroupOffset(topic string, partition int32, offset int64) error {
	topicPartition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: int32(partition),
		Offset:    kafka.Offset(offset),
	}

	offsets := []kafka.TopicPartition{topicPartition}

	err := c.consumer.Assign(offsets)
	if err != nil {
		return fmt.Errorf("failed to store offsets: %w", err)
	}

	return nil
}

func (c *KafkaConsumer) ConsumeMessages(topic []string, handler func(MessageDetails) error) error {
	err := c.consumer.SubscribeTopics(topic, nil)
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	if err != nil {
		return err
	}

	for {
		select {
		case <-signalChannel:
			c.Close()
			os.Exit(0)
		default:
			var event kafka.Event

			if consumerManager != nil && consumerManager.pollTimeoutMs != 0 {
				event = c.consumer.Poll(consumerManager.pollTimeoutMs)
			} else {
				event = c.consumer.Poll(100)
			}

			if event == nil { // we need to continue listening even after timeoutMs in Poll
				continue
			}

			switch e := event.(type) {
			case *kafka.Message:
				data := MessageDetails{
					Topic:     *e.TopicPartition.Topic,
					Partition: e.TopicPartition.Partition,
					Message:   e.Value,
					Offset:    e.TopicPartition.Offset.String(),
					Key:       e.Key,
					Headers:   e.Headers,
				}
				err = handler(data)
				if err != nil {
					return fmt.Errorf("error processing message: %v", err)
				}
			case *kafka.Error:
				return fmt.Errorf("consumer error: %v, Error Code: %v", e.Error(), e.Code())
			}
		}
	}
}

func (c *KafkaConsumer) ConsumeAvroMessage(topics []string, msg interface{}, handler func(MessageDetails) error) error {
	return c.ConsumeMessages(topics, func(data MessageDetails) error {
		c.avroDeser.DeserializeInto(topics[0], data.Message, msg)
		consumedData := MessageDetails{
			Topic:     data.Topic,
			AvroMsg:   msg,
			Offset:    data.Offset,
			Partition: data.Partition,
			Key:       data.Key,
			Headers:   data.Headers,
		}
		if err := handler(consumedData); err != nil {
			return fmt.Errorf("error processing Proto message: %w", err)
		}
		return nil
	})
}

func (c *KafkaConsumer) ConsumeGenericMessage(topics []string, callback MessageProcessor) error {
	return c.ConsumeMessages(topics, func(data MessageDetails) error {
		consumedData := MessageDetails{
			Topic:     data.Topic,
			Message:   data.Message,
			Offset:    data.Offset,
			Partition: data.Partition,
			Key:       data.Key,
			Headers:   data.Headers,
		}
		if err := callback(consumedData); err != nil {
			return fmt.Errorf("error processing generic message: %w", err)
		}
		return nil
	})
}

func (c *KafkaConsumer) ConsumeProtoMessage(topics []string, msg proto.Message, callback MessageProcessor) error {
	return c.ConsumeMessages(topics, func(data MessageDetails) error {

		err := c.deser.DeserializeInto(topics[0], data.Message, msg)

		if err != nil {
			return fmt.Errorf("error deserializing data: %w", err)
		}
		consumedData := MessageDetails{
			Topic:     data.Topic,
			ProtoMsg:  msg,
			Offset:    data.Offset,
			Partition: data.Partition,
			Key:       data.Key,
			Headers:   data.Headers,
		}
		if err := callback(consumedData); err != nil {
			return fmt.Errorf("error processing Proto message: %w", err)
		}
		return nil
	})
}

func (c *KafkaConsumer) PingKafkaConsumer(timeoutMs int) (*kafka.Metadata, error) {
	m, err := c.consumer.GetMetadata(nil, true, timeoutMs)

	if err != nil {
		return nil, fmt.Errorf("PingKafkaConsure Eror: %v", err)
	}
	return m, nil
}

func (c *KafkaConsumer) PauseKafkaConsumer(topic string, partition int32) error {
	topicPartition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
	}
	tp := []kafka.TopicPartition{topicPartition}
	err := c.consumer.Pause(tp)
	if err != nil {
		return fmt.Errorf("PauseKafkaConsumer Eror: %v", err)
	}
	return nil
}

func (c *KafkaConsumer) ResumeKafkaConsumer(topic string, partition int32) error {
	topicPartition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
	}
	tp := []kafka.TopicPartition{topicPartition}
	err := c.consumer.Resume(tp)
	if err != nil {
		return fmt.Errorf("ResumeKafkaConsumer Eror: %v", err)
	}
	return nil
}

func (c *KafkaConsumer) Close() {
	fmt.Print("Closing Consumer")
	c.consumer.Close()
}
