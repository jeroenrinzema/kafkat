package main

// Topic represents a Kafka topic
type Topic struct {
	Name              string
	ConfigEntries     map[string]*string
	ReplicationFactor int16
	NumPartitions     int32
	Delete            bool
}
