package task_consumer

import (
	"context"
	"errors"
	pb "github.com/tinyurl-pestebani/go-proto-pkg/pkg/pb/v1"
)

var (
	ErrNoMessages = errors.New("no messages received")
)

// TaskMessage is an interface for a message from a task queue.
type TaskMessage interface {
	GetTask() (*pb.Task, error)
	Commit() error
}

// TaskConsumer is an interface for a task consumer.
type TaskConsumer interface {
	PollMessage(ctx context.Context) (TaskMessage, error)
	Close() error
}
