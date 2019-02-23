package main

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
