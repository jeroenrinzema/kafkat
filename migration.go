package main

import (
	"github.com/Shopify/sarama"
)

// Migration represents a Kafka topic migration
type Migration struct {
	Brokers          []string
	AvailableTopics  []sarama.TopicDetail
	ConfiguredTopics []sarama.TopicDetail
	ClusterAdmin     sarama.ClusterAdmin
	Version          sarama.KafkaVersion
	ValidateRun      bool
	Strict           bool

	marked []string
}

// AlterConfiguration alters the configuration for the given Kafka topic
func (migration *Migration) AlterConfiguration(topic string, config map[string]*string) error {
	if migration.Strict {
		description, err := migration.ClusterAdmin.DescribeConfig(sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: topic,
		})

		if err != nil {
			return err
		}

		for _, entry := range description {
			if entry.Default || entry.ReadOnly {
				continue
			}

			_, has := config[entry.Name]
			if has {
				continue
			}

			config[entry.Name] = nil
		}
	}

	err := migration.ClusterAdmin.AlterConfig(sarama.TopicResource, topic, config, migration.ValidateRun)
	if err != nil {
		return err
	}

	return nil
}

// Cleanup deletes all the topics marked for deletion
func (migration *Migration) Cleanup() error {
	return nil
}
