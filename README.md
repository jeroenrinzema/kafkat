# KAFKAT - topics management as code

> 🚧 This project a "proof of concept".

KAFKAT is a simple CLI that manages a Kafka topic's replication factor, partition size and configuration as code. Multiple modes are available.

- **Validate**: validates the given config files and logs the results
- **Strict**: enforces that all configurations applied should be defined inside the config files. All configuration files and/or topics that are not defined in configration files will be marked for deletion. This mode should only be used when wanting to use KAFKAT as source for topic configuration/definition.

## Example

```yaml
topic:
  name: click-events
  partitions: 300
  replication: 2
config:
  cleanup.policy: delete
  flush.messages: 100000
```

```bash
$ kafkat -brokers=... -strict -validate
```
