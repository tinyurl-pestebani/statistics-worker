# statistics-worker

This worker is responsible for ingesting data from a task queue and inserting it into the statistics database.

Data structure for the task queue is defined in the [task.proto](https://github.com/tinyurl-pestebani/proto/blob/main/v1/task.proto).

## Queue types

Nowadays, the only supported type is nats.


## Environment variables

In order to configure the worker, the following variables must be setted up:

* **NATS_URL**: The NATS server URL (default: `nats://localhost:4222`).
* **NATS_TOPIC**: The NATS topic for task queue (default: `tasks.visit`).
* **NATS_DURABLE_NAME**: The NATS durable name of the consumer. Multiple consumers per topic are allowed (default: `task-processor`).
* **NATS_STREAM_NAME**: The NATS stream name (default: `TASKS`).
* **TASK_CONSUMER_TYPE**: Type of consumer. (valid values: `nats`).

For setting OpenTelemetry, and the database, please refer to https://github.com/tinyurl-pestebani/go-otel-setup and https://github.com/tinyurl-pestebani/statistics-database packages.
