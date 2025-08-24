package worker

import (
	"context"
	"errors"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/task_consumer"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/task_processor"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"sync"
	"time"
)

const (
	name = "worker"
)

var (
	logger = otelslog.NewLogger(name)
)

// Worker is a worker that processes tasks from a task queue.
type Worker struct {
	taskConsumer  task_consumer.TaskConsumer
	taskProcessor *task_processor.TaskProcessor
	semaphore     chan struct{}
	wg            sync.WaitGroup
}

// NewWorker creates a new Worker.
func NewWorker(taskConsumer task_consumer.TaskConsumer, taskProcessor *task_processor.TaskProcessor) (*Worker, error) {
	// Handle at most 10 concurrent tasks
	semaphore := make(chan struct{}, 10)

	return &Worker{
		taskConsumer:  taskConsumer,
		taskProcessor: taskProcessor,
		semaphore:     semaphore,
	}, nil
}

// Run starts the worker and listens for context cancellation.
func (w *Worker) Run(ctx context.Context) error {
	for {
		// The select makes sure not getting stuck waiting for a message
		// if a shutdown has been initiated.
		select {
		case <-ctx.Done():
			// The context was cancelled, will stop polling for messages.
			logger.Info("Context cancelled, worker stopping polling.")
			w.wg.Wait() // Wait for any in-flight tasks to finish.
			return nil
		default:
			// No shutdown signal, so we poll for a message.
			// We pass the context to PollMessage so it can also react to the cancellation.
			msg, err := w.taskConsumer.PollMessage(ctx)

			if errors.Is(err, task_consumer.ErrNoMessages) {
				logger.Debug("no messages received, worker will poll again.")
				continue
			}

			if errors.Is(err, context.Canceled) {
				// This is the expected error when the context is canceled, loop will terminate.
				w.wg.Wait() // Wait for any in-flight tasks to finish.
				return nil
			}

			if err != nil {
				logger.Error("error while polling messages from task queue", "error", err)
				time.Sleep(time.Second) // Avoid fast-looping on persistent errors
				continue
			}

			w.semaphore <- struct{}{}
			w.wg.Add(1)

			go w.processTask(ctx, msg)
		}
	}
}

// processTask processes a single task.
func (w *Worker) processTask(ctx context.Context, msg task_consumer.TaskMessage) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("recovered from panic in task", "panic", r)
		}

		if err := msg.Commit(); err != nil {
			logger.Error("error while committing message to task queue", "error", err)
		}

		<-w.semaphore
		w.wg.Done()
	}()

	task, err := msg.GetTask()
	if err != nil {
		logger.Error("error while decoding task from task queue", "error", err)
		return
	}

	if err := w.taskProcessor.ProcessTask(ctx, task); err != nil {
		logger.Error("error while executing task", "error", err)
	}
}

// GracefulStop stops the worker gracefully by cleaning up the consumer.
func (w *Worker) GracefulStop() error {
	logger.Info("Executing graceful stop. Unsubscribing and closing task consumer.")
	var errs []error

	// Then, close the underlying connection.
	if err := w.taskConsumer.Close(); err != nil {
		logger.Error("Error while closing task consumer", "error", err.Error())
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
