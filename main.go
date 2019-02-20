package main

import (
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V1_1_0_0

	brokers := []string{
		"192.168.99.100:9092",
	}

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		panic(err)
	}

	defer admin.Close()

	topic := "authorization"
	// tdetail := sarama.TopicDetail{
	// 	NumPartitions:     1,
	// 	ReplicationFactor: 1,
	// }

	atconf := make(map[string]*string)
	d := "delete"
	atconf["cleanup.policy"] = &d

	topics, err := admin.ListTopics()
	if err != nil {
		panic(err)
	}

	_, ok := topics[topic]

	// err = admin.CreateTopic(topic, &tdetail, false)
	// if err != nil {
	// 	panic(err)
	// }

	err = admin.AlterConfig(sarama.TopicResource, topic, atconf, false)
	if err != nil {
		panic(err)
	}

	tconf, err := admin.DescribeConfig(sarama.ConfigResource{
		Type:        sarama.TopicResource,
		Name:        "ok",
		ConfigNames: []string{},
	})
	if err != nil {
		panic(err)
	}

	log.Println(len(tconf))
}

// CollectConfigurations finds all available topic configuration files.
func CollectConfigurations(dir string) *Migration {
	return nil
}
