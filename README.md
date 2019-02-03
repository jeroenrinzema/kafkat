# Kafka topics

This repository manages the initialization and configuration of Kafka topics.
Topics are created with the Kafka topics utility tool.

```bash
# Create a topic
kafka/bin/kafka-topics.sh --create \
  --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 13 \
  --topic my-topic
```
