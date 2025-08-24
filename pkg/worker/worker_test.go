package worker

import (
	"context"
	"errors"
	pb "github.com/tinyurl-pestebani/go-proto-pkg/pkg/pb/v1"
	"github.com/tinyurl-pestebani/statistics-database/pkg/db/mockdb"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/mock_task_consumer"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/task_consumer"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/task_processor"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"

	"go.uber.org/mock/gomock"
)

func TestWorker_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// 1. Create instances of the mocks
	mockConsumer := mock_task_consumer.NewMockTaskConsumer(ctrl)
	mockMessage := mock_task_consumer.NewMockTaskMessage(ctrl)

	// Create a mocked instance of the database
	mockDb, err := mockdb.NewMockDB() // Assuming you have a mock for the database
	if err != nil {
		t.Fatalf("Failed to create a mockDB: %v", err)
	}

	taskProcessor, err := task_processor.NewTaskProcessor(mockDb)

	if err != nil {
		t.Fatalf("Failed to create task processor: %v", err)
	}

	// 2. Create a new worker with the mock consumer
	worker, err := NewWorker(mockConsumer, taskProcessor)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	// Create a context that we can cancel to stop the worker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The task that we expect to receive
	expectedTask := &pb.Task{
		Task: &pb.Task_T1{
			T1: &pb.InsertRecord{
				Tag: "tag12345",
				Time: &timestamppb.Timestamp{
					Seconds: 1,
					Nanos:   12345,
				},
			},
		},
	}

	// 3. Set up expectations on your mocks
	mockConsumer.EXPECT().PollMessage(gomock.Any()).Return(mockMessage, nil)
	mockMessage.EXPECT().GetTask().Return(expectedTask, nil)
	mockMessage.EXPECT().Commit().Return(nil)

	// Add a final call to stop the worker
	mockConsumer.EXPECT().PollMessage(gomock.Any()).DoAndReturn(func(_ context.Context) (task_consumer.TaskMessage, error) {
		cancel() // Cancel the context to stop the worker
		return nil, context.Canceled
	})

	// 4. Run the worker in a separate goroutine
	go func() {
		if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Worker.Run() returned an unexpected error: %v", err)
		}
	}()

	// Give the worker a moment to process the message
	time.Sleep(100 * time.Millisecond)

	if len(mockDb.Data) == 0 {
		t.Errorf("Worker.Run() did not insert any data")
	}
}
