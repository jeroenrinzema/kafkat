# KAFKAT - topics management as code

KAFKAT manages a topic's replication factor, partition size and topic configuration as code.

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
