package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	yaml "gopkg.in/yaml.v2"

	"github.com/Shopify/sarama"
)

// Logging messages
const (
	EntryValid       = "Topic configuration is valid: %s\n"
	EntryInvalid     = "Topic configuration is invalid: %s, %s\n"
	EntrySuccessfull = "Successfully altered: %s\n"
	EntryFailed      = "Unable to alter: %s, %s\n"
)

// Entry value keys
const (
	EntryKeyTopicName            = "name"
	EntryKeyTopicPartitionSize   = "partition"
	EntryKeyTopicReplicationSize = "replication"
)

// Scan scans the given path for topic configuration files and constructs a new migration.
func Scan(path string, strict, validate bool) (*Migration, error) {
	migration := NewMigration()

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		name := file.Name()
		ext := filepath.Ext(name)
		typ := mime.TypeByExtension(ext)

		file, err := os.Open(filepath.Join(path, name))
		if err != nil {
			return nil, err
		}

		reader := bufio.NewReader(file)
		entry := Entry{}

		switch typ {
		case TypeYAML:
			dec := yaml.NewDecoder(reader)
			for dec.Decode(&entry) == nil {
				migration.Entries = append(migration.Entries, entry)
			}
		}

		file.Close()
	}

	return migration, nil
}

// EntryStatus represents a migration entry status
type EntryStatus struct {
	Success bool
	Err     error
	Topic   Topic
	Content Entry
}

// Entry represents a Kafka topic configuration entry
type Entry struct {
	Topic  map[string]string  `yaml:"topic"`
	Config map[string]*string `yaml:"config"`
}

// NewMigration constructs a new migration struct
func NewMigration() *Migration {
	migration := &Migration{
		Topics: make(map[string]Topic),
		marked: make(map[string]Topic),
	}

	return migration
}

// Migration represents a Kafka topic migration
type Migration struct {
	Entries      []Entry
	Topics       map[string]Topic
	ValidateMode bool
	StrictMode   bool

	mutex  sync.RWMutex
	client *KafkaAdmin
	marked map[string]Topic
}

// Prepare prepares the migration to preform actions on the Kafka cluster
func (migration *Migration) Prepare(brokers, version string) error {
	b := strings.Split(brokers, ",")
	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return err
	}

	client, err := NewKafkaAdmin(b, v)
	if err != nil {
		return err
	}

	topics, err := client.ListTopics()
	if err != nil {
		return err
	}

	migration.mutex.RLock()
	defer migration.mutex.RUnlock()

	migration.Topics = topics
	migration.client = client

	return nil
}

// Apply applies the set entries to the defined Kafka topics
func (migration *Migration) Apply() error {
	results := make([]*EntryStatus, len(migration.Entries))

	for _, entry := range migration.Entries {
		// Ignore empty configurations
		if len(entry.Topic) == 0 {
			continue
		}

		if len(entry.Topic[EntryKeyTopicPartitionSize]) == 0 {
			entry.Topic[EntryKeyTopicPartitionSize] = "0"
		}

		if len(entry.Topic[EntryKeyTopicReplicationSize]) == 0 {
			entry.Topic[EntryKeyTopicReplicationSize] = "0"
		}

		name := entry.Topic[EntryKeyTopicName]
		partitions, err := strconv.ParseInt(entry.Topic[EntryKeyTopicPartitionSize], 0, 32)
		if err != nil {
			continue
		}

		replications, err := strconv.ParseInt(entry.Topic[EntryKeyTopicReplicationSize], 0, 16)
		if err != nil {
			continue
		}

		if len(name) == 0 {
			continue
		}

		topic := Topic{
			Name: name,
		}

		if partitions > 0 {
			topic.NumPartitions = int32(partitions)
		}

		if replications > 0 {
			topic.ReplicationFactor = int16(replications)
		}

		status := &EntryStatus{
			Topic:   topic,
			Content: entry,
		}

		results = append(results, status)

		if migration.ValidateMode {
			err := migration.client.ValidateConfiguration(topic, entry.Config, migration.StrictMode)
			if err != nil {
				status.Err = err
				continue
			}
			status.Success = true
			continue
		}

		_, exists := migration.Topics[name]
		if !exists {
			migration.client.CreateTopic(topic)
		}

		err = migration.client.AlterConfiguration(topic, entry.Config, migration.StrictMode)
		if err != nil {
			status.Err = err
			continue
		}

		status.Success = true
	}

results:
	for _, status := range results {
		if status == nil {
			continue
		}

		switch status.Err {
		default:
			if migration.ValidateMode {
				log.Printf(EntryInvalid, status.Topic.Name, status.Err)
				continue results
			}

			log.Printf(EntryFailed, status.Topic.Name, status.Err)
		case nil:
			if migration.ValidateMode {
				log.Printf(EntryValid, status.Topic.Name)
				continue results
			}

			log.Printf(EntrySuccessfull, status.Topic.Name)
		}
	}

	return nil
}
