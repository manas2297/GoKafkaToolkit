package consumer

import (
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/manas2297/GoKafkaToolkit/constants"
)

var consumerParams *ConsumerParams

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return defaultValue
	}
	return value
}

func getAdminConfigFromParams(params *ConsumerParams) (*kafka.ConfigMap, error) {
	config := &kafka.ConfigMap{
		constants.CONFIG_BOOTSTRAP_SERVERS: params.BootstrapServers,
	}
	err := updateConfigFromSecurityParams(config, params)
	return config, err
}

func getConfigFromParams(params *ConsumerParams) (*kafka.ConfigMap, error) {
	config := &kafka.ConfigMap{
		constants.CONFIG_BOOTSTRAP_SERVERS:  params.BootstrapServers,
		constants.CONFIG_GROUP_ID:           params.GroupId,
		constants.CONFIG_AUTO_OFFSET_RESET:  params.Offset,
		constants.CONFIG_ENABLE_AUTO_COMMIT: params.AutoCommit,
	}

	err := updateConfigFromSecurityParams(config, params)
	return config, err
}

func updateConfigFromSecurityParams(config *kafka.ConfigMap, params *ConsumerParams) error {
	if !params.SecurityParams {
		return nil
	}

	err := config.SetKey(constants.CONFIG_SECURITY_PROTOCOL, params.SecurityProtocol)
	if err != nil {
		return err
	}

	err = config.SetKey(constants.CONFIG_SASL_MECHANISM, params.SaslMechanism)
	if err != nil {
		return err
	}

	err = config.SetKey(constants.CONFIG_SASL_USERNAME, params.SaslUsername)
	if err != nil {
		return err
	}

	err = config.SetKey(constants.CONFIG_SASL_PASSWORD, params.SaslPassword)
	if err != nil {
		return err
	}

	return nil
}

func setDefaultConsumerParams(params *ConsumerParams) {
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

func setConsumerParams(params *ConsumerParams) {
	setDefaultConsumerParams(params)
	if params.BootstrapServers == "" {
		params.BootstrapServers = os.Getenv(constants.KAFKA_BOOTSTRAP_SERVERS)
	}

	if params.SchemaRegistryUrl == "" {
		params.SchemaRegistryUrl = os.Getenv(constants.SCHEMA_REGISTRY_URL)
	}

	consumerParams = params
}
