go get go.uber.org/mock/gomock
go install go.uber.org/mock/mockgen@latest

rm -rf ./pkg/mock_task_consumer
mkdir -p ./pkg/mock_task_consumer
mockgen -source=./pkg/task_consumer/task_consumer.go -destination=./pkg/mock_task_consumer/mock_task_consumer.go

export NATS_TOPIC=tasks_test.visits
go test ./...
