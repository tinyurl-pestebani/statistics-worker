package config

import (
	"os"
	"testing"
)

func TestNewNatsConfig(t *testing.T) {
	nc := NewNatsConfig("localhost:4222", "test", "durable-name", "TASKSTEST")

	if nc == nil {
		t.Fatal("NewNatsConfig failed, it returned a nil pointer")
	}

	if nc.Url != "localhost:4222" {
		t.Fatal("NewNatsConfig failed, it returned a wrong url")
	}

	if nc.Topic != "test" {
		t.Fatal("NewNatsConfig failed, it returned a wrong topic")
	}
}

func TestNewNatsConfigFromEnv(t *testing.T) {
	url := os.Getenv("NATS_URL")
	topic := os.Getenv("NATS_TOPIC")
	conf := NewNatsConfigFromEnv()

	if conf == nil {
		t.Fatal("NewNatsConfigFromEnv failed, it returned a nil pointer")
	}

	if conf.Url != url && url != "" {
		t.Fatal("NewNatsConfigFromEnv failed, it returned a wrong url")
	}

	if conf.Topic != topic && topic != "" {
		t.Fatal("NewNatsConfigFromEnv failed, it returned a wrong topic")
	}

	url = conf.Url
	topic = conf.Topic
	durableName := conf.DurableName
	streamName := conf.StreamName

	os.Unsetenv("NATS_URL")
	os.Unsetenv("NATS_TOPIC")
	os.Unsetenv("NATS_DURABLE_NAME")
	os.Unsetenv("NATS_STREAM_NAME")

	conf = NewNatsConfigFromEnv()

	if conf == nil {
		t.Fatal("NewNatsConfigFromEnv failed, it returned a nil pointer")
	}

	if conf.Url != "nats://localhost:4222" {
		t.Fatal("NewNatsConfigFromEnv failed, it returned a wrong url")
	}

	if conf.Topic != "tasks.visit" {
		t.Fatal("NewNatsConfigFromEnv failed, it returned a wrong topic")
	}

	if conf.DurableName != "task-processor" {
		t.Fatalf("NewNatsConfigFromEnv failed, it returned a wrong durable name: %s", conf.DurableName)
	}

	os.Setenv("NATS_URL", url)
	os.Setenv("NATS_TOPIC", topic)
	os.Setenv("NATS_DURABLE_NAME", durableName)
	os.Setenv("NATS_STREAM_NAME", streamName)

}
