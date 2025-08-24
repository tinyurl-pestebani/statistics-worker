package task_consumer_layer

import (
	"errors"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/config"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/task_consumer"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/task_consumer/nats_consumer"
	"os"
)

// ErrInvalidTaskConsumer is returned when the TASK_CONSUMER_TYPE environment variable is not set to a valid value.
var ErrInvalidTaskConsumer = errors.New("invalid task consumer")

// NewTaskConsumerLayer creates a new TaskConsumer based on the TASK_CONSUMER_TYPE environment variable.
func NewTaskConsumerLayer() (task_consumer.TaskConsumer, error) {
	switch os.Getenv("TASK_CONSUMER_TYPE") {
	case "nats":
		conf := config.NewNatsConfigFromEnv()
		return nats_consumer.NewNatsConsumer(conf)
	default:
		return nil, ErrInvalidTaskConsumer
	}
}
