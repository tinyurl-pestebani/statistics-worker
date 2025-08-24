package task_processor

import (
	"context"
	pb "github.com/tinyurl-pestebani/go-proto-pkg/pkg/pb/v1"
	"github.com/tinyurl-pestebani/statistics-database/pkg/db/mockdb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
)

func TestTaskProcessor(t *testing.T) {
	mb, _ := mockdb.NewMockDB()
	tp, err := NewTaskProcessor(mb)

	if err != nil {
		t.Errorf("Error while creating taskProcessor: %s", err)
	}

	task := &pb.Task{
		Task: &pb.Task_T1{
			T1: &pb.InsertRecord{
				Tag: "abcdef12",
				Time: &timestamppb.Timestamp{
					Seconds: 1,
					Nanos:   12345,
				},
			},
		},
	}

	if err := tp.ProcessTask(context.Background(), task); err != nil {
		t.Errorf("Error while processing task: %s", err)
	}

	if len(mb.Data) != 1 {
		t.Errorf("Master map length want 1, got %d", len(mb.Data))
	}
}
