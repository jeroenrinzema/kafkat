package main

import (
	"bufio"
	"io/ioutil"
	"mime"
	"os"
	"path/filepath"
	"strings"

	yaml "gopkg.in/yaml.v2"

	"github.com/Shopify/sarama"
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

// Entry represents a Kafka topic configuration entry
type Entry struct {
	Topic  map[string]string `yaml:"topic"`
	Config map[string]string `yaml:"config"`
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
	KafkaVersion sarama.KafkaVersion
	Brokers      []string
	Entries      []Entry
	Topics       map[string]Topic
	Client       sarama.ClusterAdmin
	ValidateMode bool
	StrictMode   bool

	client sarama.ClusterAdmin
	marked map[string]Topic
}

// Prepare prepares the migration to preform actions on the Kafka cluster
func (migration *Migration) Prepare(brokers, version string) error {
	var err error

	migration.Brokers = strings.Split(brokers, ",")
	migration.KafkaVersion, err = sarama.ParseKafkaVersion(version)
	if err != nil {
		return err
	}

	config := sarama.NewConfig()
	config.Version = migration.KafkaVersion

	client, err := sarama.NewClusterAdmin(migration.Brokers, config)
	if err != nil {
		return err
	}

	topics, err := client.ListTopics()
	if err != nil {
		return err
	}

	for name, topic := range topics {
		migration.Topics[name] = Topic{
			Name:              name,
			ConfigEntries:     topic.ConfigEntries,
			NumPartitions:     topic.NumPartitions,
			ReplicationFactor: topic.ReplicationFactor,
		}
	}

	migration.client = client
	return nil
}

// Apply applies the set entries to the defined Kafka topics
func (migration *Migration) Apply() error {
	for _, entry := range migration.Entries {
		name := entry.Topic[EntryKeyTopicName]
		// partitions := entry.Topic[EntryKeyTopicPartitionSize]
		// replications := entry.Topic[EntryKeyTopicReplicationSize]

		if len(name) == 0 {
			// TODO: throw a warning that no topic name is set
			continue
		}
	}

	return nil
}
