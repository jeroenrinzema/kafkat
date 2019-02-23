package main

import (
	"flag"
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
	TargetPath     = ""
	Brokers        = ""
	KafkaVersion   = ""
	StrictMode     = false
	ValidationMode = false
)

func init() {
	// Include the yaml/yml type extentions to the default mime package
	mime.AddExtensionType(".yaml", TypeYAML)
	mime.AddExtensionType(".yml", TypeYAML)
}

func main() {
	target, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}

	flag.StringVar(&TargetPath, "target", target, "Target directory, by default is the current directory used")
	flag.StringVar(&Brokers, "brokers", "", "Initial Kafka broker hosts")
	flag.StringVar(&KafkaVersion, "kafka-version", "1.1.0", "Initial Kafka broker hosts")
	flag.BoolVar(&StrictMode, "strict", false, "Strict configuration mode")
	flag.BoolVar(&ValidationMode, "validation", false, "Validation mode")
	flag.Parse()

	migration, err := Scan(TargetPath, StrictMode, ValidationMode)
	if err != nil {
		panic(err)
	}

	err = migration.Prepare(Brokers, KafkaVersion)
	if err != nil {
		panic(err)
	}

	migration.StrictMode = StrictMode
	migration.ValidateMode = ValidationMode

	err = migration.Apply()
	if err != nil {
		panic(err)
	}
}
