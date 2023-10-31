package admin

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/manas2297/GoKafkaToolkit/constants"
)

type KafkaAdminClient struct {
	admin *kafka.AdminClient
}
type KafkaSchemaRegistryClient struct {
	producer *kafka.Producer
}

func NewKafkaAdminClient(bootstrapServers string) (*KafkaAdminClient, error) {
	config := &kafka.ConfigMap{
		constants.CONFIG_BOOTSTRAP_SERVERS: bootstrapServers,
	}

	admin, err := kafka.NewAdminClient(config)

	if err != nil {
		return nil, fmt.Errorf("Failed to create kafka admin client: %v", err)
	}

	return &KafkaAdminClient{
		admin: admin,
	}, nil
}

func (c *KafkaAdminClient) Close() {
	c.admin.Close()
}

func (c *KafkaAdminClient) CreateTopic(topic string, partitions int, replicationFactor int) error {
	topicSpecifications := kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}

	results, err := c.admin.CreateTopics(context.Background(), []kafka.TopicSpecification{topicSpecifications}, kafka.SetAdminOperationTimeout(5000))

	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			return fmt.Errorf("failed to create topic: %v", result.Error)
		}
	}

	return nil

}

// TODO : Implement Schema Registry
