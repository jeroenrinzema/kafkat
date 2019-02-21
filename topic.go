package main

import (
	"github.com/Shopify/sarama"
)

// Topic represents a Kafka topic
type Topic struct {
	client            sarama.ClusterAdmin
	Name              string
	ConfigEntries     map[string]*string
	ReplicationFactor int16
	NumPartitions     int32
	Delete            bool
}

// AlterConfiguration alters the configuration of the migration Topic.
// If strict mode is enable are all topic configurations that are not defined in the given configuration deleted.
func (topic *Topic) AlterConfiguration(config map[string]*string, strict bool) error {
	if strict {
		description, err := topic.client.DescribeConfig(sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: topic.Name,
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

	err := topic.client.AlterConfig(sarama.TopicResource, topic.Name, config, false)
	if err != nil {
		return err
	}

	return nil
}

// ValidateConfiguration validates the given configuration
func (topic *Topic) ValidateConfiguration(config map[string]*string) error {
	err := topic.client.AlterConfig(sarama.TopicResource, topic.Name, config, true)
	if err != nil {
		return err
	}

	return nil
}
