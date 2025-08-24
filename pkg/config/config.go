package config

import "os"

type NatsConfig struct {
	Url         string
	Topic       string
	DurableName string
	StreamName  string
}

// NewNatsConfigFromEnv creates a NatsConfig from environment variables.
func NewNatsConfigFromEnv() *NatsConfig {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://localhost:4222"
	}

	topic := os.Getenv("NATS_TOPIC")
	if topic == "" {
		topic = "tasks.visit"
	}

	durableName := os.Getenv("NATS_DURABLE_NAME")
	if durableName == "" {
		durableName = "task-processor"
	}

	streamName := os.Getenv("NATS_STREAM_NAME")
	if streamName == "" {
		streamName = "TASKS"
	}

	return NewNatsConfig(url, topic, durableName, streamName)
}

// NewNatsConfig creates a new NatsConfig.
func NewNatsConfig(url, topic, durableName, streamName string) *NatsConfig {
	return &NatsConfig{
		Url:         url,
		Topic:       topic,
		DurableName: durableName,
		StreamName:  streamName,
	}
}
