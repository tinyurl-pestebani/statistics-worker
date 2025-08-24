package nats_consumer

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/tinyurl-pestebani/go-proto-pkg/pkg/pb/v1"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/config"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/task_consumer"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// NatsMessage is a wrapper around a nats.Msg that implements the task_consumer.TaskMessage interface.
type NatsMessage struct {
	msg *nats.Msg
}

// NatsConsumer is a task consumer that polls messages from a NATS JetStream stream.
// It uses a durable pull consumer to ensure messages are not lost if the service is down.
type NatsConsumer struct {
	conn *nats.Conn
	js   nats.JetStreamContext
	sub  *nats.Subscription
	// The durableName is crucial for the consumer to pick up where it left off.
	durableName string
}

// NewNatsConsumer creates a new NatsConsumer that connects to NATS JetStream.
func NewNatsConsumer(conf *config.NatsConfig) (*NatsConsumer, error) {
	if conf.Topic == "" || conf.DurableName == "" || conf.StreamName == "" {
		return nil, errors.New("NATS topic, durable name and stream name must be configured")
	}

	conn, err := nats.Connect(
		conf.Url,
		// Add some robustness for production environments
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(3),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// 1. Get the JetStream context. This is the entry point for all JetStream operations.
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:      conf.StreamName,
		Subjects:  []string{conf.Topic}, // The stream captures messages from this subject.
		Retention: nats.InterestPolicy,  // Use WorkQueue so messages are deleted after ack from any consumer.
	})
	if err != nil {
		// Check if the error is that the stream already exists, which is not a failure condition.
		var APIError *nats.APIError
		b1 := errors.As(err, &APIError)
		if !b1 || APIError.ErrorCode != nats.JSErrCodeStreamNameInUse {
			return nil, fmt.Errorf("failed to add NATS stream: %w", err)
		}
		log.Println("NATS stream already exists")
	}

	// 2. Create a PULL subscription with a DURABLE name.
	// The Durable name ensures NATS remembers the consumer's state across restarts.
	sub, err := js.PullSubscribe(conf.Topic, conf.DurableName)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create pull subscription: %w", err)
	}

	return &NatsConsumer{
		conn:        conn,
		js:          js,
		sub:         sub,
		durableName: conf.DurableName,
	}, nil
}

// GetTask unmarshal the message data into a pb.Task.
func (m *NatsMessage) GetTask() (*pb.Task, error) {
	data := m.msg.Data
	task := &pb.Task{}
	if err := proto.Unmarshal(data, task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf task: %w", err)
	}
	return task, nil
}

// Commit acknowledges the message to JetStream, marking it as processed.
// This is how JetStream knows not to redeliver the message.
func (m *NatsMessage) Commit() error {
	// Using AckSync for a synchronous confirmation from the server.
	// This is safer as it ensures the server has processed the acknowledgment.
	err := m.msg.AckSync()
	if err != nil {
		// If the context is expired, it might mean the app is shutting down.
		if errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}
	return nil
}

// PollMessage fetches a message from the JetStream pull subscription.
func (n *NatsConsumer) PollMessage(ctx context.Context) (task_consumer.TaskMessage, error) {
	// We fetch one message at a time and wait up to 3 seconds.
	msgs, err := n.sub.Fetch(1, nats.Context(ctx))
	if err != nil {
		// If no new message, return a task_consumer.ErrNoMessages
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, task_consumer.ErrNoMessages
		}

		// If the context is done, it means the poll was cancelled from outside.
		if errors.Is(err, context.Canceled) {
			return nil, err
		}

		return nil, fmt.Errorf("error fetching message from NATS: %w", err)
	}

	// We requested 1 message, so we should get 1 if the slice is not empty.
	if len(msgs) == 0 {
		return nil, nil
	}

	return &NatsMessage{
		msg: msgs[0],
	}, nil
}

// Close gracefully shuts down the consumer.
func (n *NatsConsumer) Close() error {
	var errs []error

	// 1. Drain the subscription. This will wait for any in-flight messages to be acknowledged.
	if err := n.sub.Drain(); err != nil {
		errs = append(errs, fmt.Errorf("failed to drain subscription: %w", err))
	}

	// 2. Drain the main connection. This ensures all buffered messages are sent.
	if n.conn != nil {
		if err := n.conn.Drain(); err != nil {
			errs = append(errs, fmt.Errorf("failed to drain NATS connection: %w", err))
		}
		// 3. Close the connection.
		n.conn.Close()
	}

	if len(errs) > 0 {
		return fmt.Errorf("encountered errors while closing NATS consumer: %v", errs)
	}
	return nil
}
