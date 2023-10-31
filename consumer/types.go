package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/proto"
)

// KafkaConsumer is a struct representing a Kafka consumer.
type KafkaConsumer struct {
	consumer       *kafka.Consumer
	schemaRegistry *schemaregistry.Client
	deser          *protobuf.Deserializer
	avroDeser      *avro.GenericDeserializer
}

// ConsumerParams represents the parameters for creating a Kafka consumer.
type ConsumerParams struct {
	BootstrapServers  string
	GroupId           string
	SchemaRegistryUrl string
	Offset            string
	ConsumerCount     int
	AutoCommit        bool
	SecurityParams    bool
	SecurityProtocol  string
	SaslMechanism     string
	SaslUsername      string
	SaslPassword      string
	SessionTimeout    int
	RegistryUsername  string
	RegistryPassword  string
}

// MessageDetails represents the details of a Kafka message.
type MessageDetails struct {
	Topic     string
	Offset    string
	Partition int32
	Message   []byte
	ProtoMsg  proto.Message
	AvroMsg   interface{}
	Key       []byte
	Headers   []kafka.Header
}

// MessageProcessor is a function type for processing Kafka messages.
type MessageProcessor func(msg MessageDetails) error
type BatchMessageProcessor func(msg []MessageDetails) error

type TopicParamsHandlers struct {
	Topics   []string
	Params   *ConsumerParams
	Handler  MessageProcessor
	ProtoMsg proto.Message
	AvroMsg  interface{}
}

type BatchConsumerOpt struct {
	BatchSize uint
	TimeoutMs uint
}
