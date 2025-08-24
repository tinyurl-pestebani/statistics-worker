package task_processor

import (
	"context"
	"errors"
	pb "github.com/tinyurl-pestebani/go-proto-pkg/pkg/pb/v1"
	"github.com/tinyurl-pestebani/statistics-database/pkg/db"
	"time"
)

var (
	// ErrUnknownTaskType is returned when the task type is unknown.
	ErrUnknownTaskType = errors.New("unknown task type")
)

// TaskProcessor processes tasks.
type TaskProcessor struct {
	statsDB db.StatisticsDatabase
}

// NewTaskProcessor creates a new TaskProcessor.
func NewTaskProcessor(statsDB db.StatisticsDatabase) (*TaskProcessor, error) {
	return &TaskProcessor{statsDB: statsDB}, nil
}

// ProcessTask processes a task.
func (tp *TaskProcessor) ProcessTask(ctx context.Context, task *pb.Task) error {
	switch task.Task.(type) {
	case *pb.Task_T1:
		record := task.Task.(*pb.Task_T1).T1
		return tp.insertPoint(ctx, record)
	default:
		return ErrUnknownTaskType
	}
}

// insertPoint inserts a statistic point into the database.
func (tp *TaskProcessor) insertPoint(ctx context.Context, record *pb.InsertRecord) error {
	tag := record.Tag
	t := record.Time.AsTime()

	ctxChild, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return tp.statsDB.InsertStatisticPoint(ctxChild, tag, t)
}
