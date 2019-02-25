package main

import (
	"flag"
	"fmt"
	"mime"
	"os"
	"path/filepath"
)

// Supported configuration types
const (
	TypeYAML = "text/yaml; charset=utf-8"
)

// Flag variables
var (
	TargetPath   = ""
	Brokers      = ""
	KafkaVersion = ""
	StrictMode   = false
	ValidateMode = false
)

// Reporting templates
const (
	devider         = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
	HeaderReport    = devider + "\tKafka bootstrap brokers:\t%s\n\tValidate mode:\t\t\t%t\n\tStrict mode:\t\t\t%t\n\tTarget path:\t\t\t%s\n" + devider
	MigrationReport = devider + "\tValidate mode:\t\t\t%t\n\tStrict mode:\t\t\t%t\n\tKafka bootstrap brokers:\t%s\n\tTopics found:\t\t\t%v\n\tTopics entries:\t\t\t%v\n\tTopics marked for deletion:\t%v\n" + devider
)

func init() {
	// Include the yaml/yml type extentions to the default mime package
	mime.AddExtensionType(".yaml", TypeYAML)
	mime.AddExtensionType(".yml", TypeYAML)
}

func main() {
	// Recover from panics and exit with a error
	defer func() {
		err := recover()
		if err == nil {
			os.Exit(0)
			return
		}

		fmt.Println(err)
		os.Exit(1)
	}()

	flag.StringVar(&TargetPath, "target", ".", "Target directory, by default is the current directory used")
	flag.StringVar(&Brokers, "brokers", "", "Initial Kafka broker hosts")
	flag.StringVar(&KafkaVersion, "kafka-version", "1.1.0", "Initial Kafka broker hosts")
	flag.BoolVar(&StrictMode, "strict", false, "Strict configuration mode")
	flag.BoolVar(&ValidateMode, "validate", false, "Validate mode")
	flag.Parse()

	target, err := filepath.Abs(TargetPath)
	if err != nil {
		panic(err)
	}

	fmt.Printf(HeaderReport, Brokers, ValidateMode, StrictMode, target)

	migration, err := Scan(target, StrictMode, ValidateMode)
	if err != nil {
		panic(err)
	}

	migration.StrictMode = StrictMode
	migration.ValidateMode = ValidateMode

	err = migration.Prepare(Brokers, KafkaVersion)
	if err != nil {
		panic(err)
	}

	err = migration.Apply()
	if err != nil {
		panic(err)
	}

	topics := []string{}
	entries := []string{}
	deleted := []string{}

	for _, topic := range migration.Topics {
		name := topic.Name
		if len(name) == 0 {
			continue
		}

		topics = append(topics, name)
	}

	for _, entry := range migration.Entries {
		name := entry.Topic[EntryKeyTopicName]
		if len(name) == 0 {
			continue
		}

		entries = append(entries, name)
	}

	for _, topic := range migration.marked {
		name := topic.Name
		if len(name) == 0 {
			continue
		}

		if topic.Delete {
			deleted = append(deleted, name)
		}
	}

	fmt.Printf(MigrationReport, migration.ValidateMode, migration.StrictMode, Brokers, topics, entries, deleted)
}
