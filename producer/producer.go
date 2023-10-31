package producer

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/manas2297/GoKafkaToolkit/constants"
	"google.golang.org/protobuf/proto"
)

type KafkaProducer struct {
	producer       *kafka.Producer
	schemaRegistry *schemaregistry.Client
	serializer     *protobuf.Serializer
	avroSer        *avro.GenericSerializer
}
type ProducerParams struct {
	BootstrapServers  string
	SchemaRegistryUrl string
	SecurityParams    bool
	SecurityProtocol  string
	SaslMechanism     string
	SaslUsername      string
	SaslPassword      string
	RegistryUsername  string
	RegistryPassword  string
}

type ProducerOptions struct {
	GenericMessage interface{}
	Message        proto.Message
	AvroMessage    interface{}
	Opaque         interface{}
	Key            []byte
	Headers        []kafka.Header
}

type ProducedMessageDetails struct {
	Topic     string
	Offset    string
	Partition int32
}

func setDefaultConfig(params *ProducerParams) {
	username := os.Getenv(constants.CONFLUENT_TOPIC_API_KEY)
	password := os.Getenv(constants.CONFLUENT_TOPIC_API_SECRET)
	regUsername := os.Getenv(constants.SCHEMA_REGISTRY_USERNAME)
	regPassword := os.Getenv(constants.SCHEMA_REGISTRY_PASSWORD)

	if !params.SecurityParams && username != "" && password != "" {
		params.SecurityParams = true
		params.SecurityProtocol = constants.SECURITY_PROTOCOL_SASL_SSL
		params.SaslMechanism = constants.SECURITY_MECHANISM
		params.SaslUsername = username
		params.SaslPassword = password
		params.RegistryUsername = regUsername
		params.RegistryPassword = regPassword
	}
}

func NewKafkaProducer(params *ProducerParams) (*KafkaProducer, error) {
	config := &kafka.ConfigMap{
		constants.CONFIG_BOOTSTRAP_SERVERS: params.BootstrapServers,
	}

	setDefaultConfig(params)

	if params.SecurityParams {
		err := config.SetKey(constants.CONFIG_SECURITY_PROTOCOL, params.SecurityProtocol)
		if err != nil {
			return nil, err
		}
		err = config.SetKey(constants.CONFIG_SASL_MECHANISM, params.SaslMechanism)
		if err != nil {
			return nil, err
		}
		err = config.SetKey(constants.CONFIG_SASL_USERNAME, params.SaslUsername)
		if err != nil {
			return nil, err
		}
		err = config.SetKey(constants.CONFIG_SASL_PASSWORD, params.SaslPassword)
		if err != nil {
			return nil, err
		}
	}

	producer, err := kafka.NewProducer(config)

	if err != nil {
		return nil, fmt.Errorf("Failed to create Kafka producer: %w", err)
	}

	var schema_config *schemaregistry.Config

	if !params.SecurityParams {
		schema_config = schemaregistry.NewConfig(params.SchemaRegistryUrl)
	} else {
		schema_config = schemaregistry.NewConfigWithAuthentication(params.SchemaRegistryUrl, params.RegistryUsername, params.RegistryPassword)
	}

	client, err := schemaregistry.NewClient(schema_config)

	if err != nil {
		return nil, fmt.Errorf("Failed to create schema registry")
	}

	ser, err := protobuf.NewSerializer(client, serde.ValueSerde, protobuf.NewSerializerConfig())
	avroSer, err := avro.NewGenericSerializer(client, serde.ValueSerde, avro.NewSerializerConfig())

	if err != nil {
		return nil, err
	}

	return &KafkaProducer{
		producer:       producer,
		schemaRegistry: &client,
		serializer:     ser,
		avroSer:        avroSer,
	}, nil
}

func (p *KafkaProducer) ProduceGeneralMessage(topic string, options ...interface{}) (*ProducedMessageDetails, error) {
	var payload []byte
	var key []byte
	var headers []kafka.Header
	var marshalError error

	if len(options) > 0 {
		for _, opt := range options {
			if pOpt, ok := opt.(*ProducerOptions); ok {
				payload, marshalError = json.Marshal(pOpt.GenericMessage)
				if marshalError != nil {
					return nil, marshalError
				}
				headers = pOpt.Headers
			}
		}
	}
	deliveryChannel := make(chan kafka.Event)

	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Key:            key,
		Headers:        headers,
	}

	err := p.producer.Produce(kafkaMessage, deliveryChannel)

	if err != nil {
		return nil, fmt.Errorf("Failed to produce message: %w", err)
	}

	e := <-deliveryChannel
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return nil, fmt.Errorf("Message delivery failed: %w", m.TopicPartition.Error)
	}

	defer close(deliveryChannel)
	return &ProducedMessageDetails{
		Topic:     *m.TopicPartition.Topic,
		Offset:    m.TopicPartition.Offset.String(),
		Partition: m.TopicPartition.Partition,
	}, nil
}

func (p *KafkaProducer) ProduceAvroMessage(topic string, options ...interface{}) (*ProducedMessageDetails, error) {
	var payload []byte
	var marshalError error
	var headers []kafka.Header
	if len(options) > 0 {
		for _, opt := range options {
			if pOpt, ok := opt.(*ProducerOptions); ok {
				payload, marshalError = p.avroSer.Serialize(topic, pOpt.AvroMessage)
				if marshalError != nil {
					return nil, marshalError
				}
				headers = pOpt.Headers
			}
		}
	}
	deliveryChannel := make(chan kafka.Event)

	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Headers:        headers,
	}

	err := p.producer.Produce(kafkaMessage, deliveryChannel)

	if err != nil {
		return nil, fmt.Errorf("Failed to produce message: %w", err)
	}

	e := <-deliveryChannel
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return nil, fmt.Errorf("Message delivery failed: %w", m.TopicPartition.Error)
	}

	defer close(deliveryChannel)
	return &ProducedMessageDetails{
		Topic:     *m.TopicPartition.Topic,
		Offset:    m.TopicPartition.Offset.String(),
		Partition: m.TopicPartition.Partition,
	}, nil
}

func (p *KafkaProducer) ProduceProtoMessage(topic string, options ...interface{}) (*ProducedMessageDetails, error) {

	deliveryChannel := make(chan kafka.Event)
	var payload []byte
	var key []byte
	var marshalError error
	var headers []kafka.Header
	if len(options) > 0 {
		for _, opt := range options {
			if pOpt, ok := opt.(*ProducerOptions); ok {
				payload, marshalError = p.serializer.Serialize(topic, pOpt.Message)
				key = pOpt.Key
				if marshalError != nil {
					return nil, marshalError
				}
				headers = pOpt.Headers
			}
		}
	}

	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Key:            key,
		Headers:        headers,
	}

	err := p.producer.Produce(kafkaMessage, deliveryChannel)
	if err != nil {
		return nil, fmt.Errorf("Failed to produce message: %w", err)
	}

	e := <-deliveryChannel
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return nil, fmt.Errorf("Message delivery failed: %w", m.TopicPartition.Error)
	}

	defer close(deliveryChannel)
	return &ProducedMessageDetails{
		Topic:     *m.TopicPartition.Topic,
		Offset:    m.TopicPartition.Offset.String(),
		Partition: m.TopicPartition.Partition,
	}, nil
}

func (p *KafkaProducer) Close() {
	p.producer.Close()
}
