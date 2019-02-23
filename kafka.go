package main

import (
	"log"

	"github.com/Shopify/sarama"
)

// Logging messages
const (
	TopicFound              = "Found: %s, with the configuration %v\n"
	EntryPropertyNotDefined = "The configuration property: %s on the topic %s, is not defined in the configuration entry\n"
	EntryPropertyDeleted    = "The configuration property: %s on the topic %s, is marked for deletion\n"
	AlteredConfiguration    = "The configuration for the topic %s, has been modified %+v\n"
)

// NewKafkaAdmin creates a new KafkaAdmin
func NewKafkaAdmin(brokers []string, version sarama.KafkaVersion) (*KafkaAdmin, error) {
	config := sarama.NewConfig()
	config.Version = version

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return nil, err
	}

	client := KafkaAdmin{
		client: admin,
	}

	return &client, nil
}

// KafkaAdmin represents a Kafka admin client.
// This client is used to alter or validate configurations.
type KafkaAdmin struct {
	KafkaVersion sarama.KafkaVersion
	Brokers      []string
	client       sarama.ClusterAdmin
}

// CreateTopic creates a new Kafka topic with the given topic replication factor
// and number of partitions.
func (kafka *KafkaAdmin) CreateTopic(topic Topic) error {
	details := sarama.TopicDetail{
		NumPartitions:     topic.NumPartitions,
		ReplicationFactor: topic.ReplicationFactor,
	}

	// Validate the topic configuration
	valid := kafka.client.CreateTopic(topic.Name, &details, true)
	if valid != nil {
		return valid
	}

	err := kafka.client.CreateTopic(topic.Name, &details, false)
	if err != nil {
		return err
	}

	return nil
}

// EnforceConfiguration enforces the given configuration for undefined existing properties
func (kafka *KafkaAdmin) EnforceConfiguration(topic Topic, config map[string]*string, strict bool) (map[string]*string, error) {
	description, err := kafka.client.DescribeConfig(sarama.ConfigResource{
		Type: sarama.TopicResource,
		Name: topic.Name,
	})

	if err != nil {
		return config, err
	}

	for _, entry := range description {
		if entry.Default || entry.ReadOnly {
			continue
		}

		_, has := config[entry.Name]
		if has {
			continue
		}

		log.Printf(EntryPropertyNotDefined, entry.Name, topic.Name)

		if strict {
			log.Printf(EntryPropertyDeleted, entry.Name, topic.Name)
			continue
		}

		config[entry.Name] = &entry.Value
	}

	return config, nil
}

// AlterConfiguration alters the configuration of the given Topic.
// If strict mode is enable are all topic configurations
// that are not defined in the given configuration deleted.
// No validation is preformed before a configuration alteration
// and it is not checked if the given topic already exists on the Kafka cluster.
func (kafka *KafkaAdmin) AlterConfiguration(topic Topic, config map[string]*string, strict bool) error {
	var err error
	config, err = kafka.EnforceConfiguration(topic, config, strict)
	if err != nil {
		return err
	}

	err = kafka.client.AlterConfig(sarama.TopicResource, topic.Name, config, false)
	if err != nil {
		return err
	}

	log.Printf(AlteredConfiguration, topic.Name, config)
	return nil
}

// ValidateConfiguration validates the given configuration.
// A error is returned if the configuration is invalid.
func (kafka *KafkaAdmin) ValidateConfiguration(topic Topic, config map[string]*string, strict bool) error {
	var err error
	config, err = kafka.EnforceConfiguration(topic, config, strict)
	if err != nil {
		return err
	}

	err = kafka.client.AlterConfig(sarama.TopicResource, topic.Name, config, true)
	if err != nil {
		return err
	}

	return nil
}

// ListTopics lists all available Kafka topics and returns a map of available topics
// with their configuration entries, number of partitions and replication factor.
func (kafka *KafkaAdmin) ListTopics() (map[string]Topic, error) {
	available, err := kafka.client.ListTopics()
	if err != nil {
		return nil, err
	}

	topics := make(map[string]Topic, len(available))

	for name, topic := range available {
		topics[name] = Topic{
			Name:              name,
			ConfigEntries:     topic.ConfigEntries,
			NumPartitions:     topic.NumPartitions,
			ReplicationFactor: topic.ReplicationFactor,
		}

		// For readability parse the map pointers to strings
		c := make(map[string]string, len(topic.ConfigEntries))
		for k, v := range topic.ConfigEntries {
			c[k] = *v
		}

		log.Printf(TopicFound, name, c)
	}

	return topics, nil
}
