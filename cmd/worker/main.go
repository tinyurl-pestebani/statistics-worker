package main

import (
	"context"
	"github.com/tinyurl-pestebani/go-otel-setup/pkg/otel"
	"github.com/tinyurl-pestebani/statistics-database/pkg/db_layer"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/task_consumer_layer"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/task_processor"
	"github.com/tinyurl-pestebani/statistics-worker/pkg/worker"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	// 1. Create a cancellable parent context for the entire application.
	ctx, cancel := context.WithCancel(context.Background())
	serviceName := "statistics-worker"
	serviceVersion := "0.0.1"
	otelShutdown, err := otel.SetOTelSDK(ctx, serviceName, serviceVersion)
	if err != nil {
		log.Fatalf("SetOTelSDK failed: %v", err)
	}

	// Create stats DB
	statsDB, err := db_layer.NewDBLayer()
	if err != nil {
		log.Fatalf("failed to create new DB layer: %s", err)
	}

	// Create task processor
	taskProcessor, err := task_processor.NewTaskProcessor(statsDB)
	if err != nil {
		log.Fatalf("failed to create new task processor: %s", err)
	}

	// Create task consumer
	taskConsumer, err := task_consumer_layer.NewTaskConsumerLayer()
	if err != nil {
		log.Fatalf("failed to create new task consumer: %s", err)
	}

	w, err := worker.NewWorker(taskConsumer, taskProcessor)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	// 2. Use a WaitGroup to track running goroutines.
	var wg sync.WaitGroup
	wg.Add(1)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		defer wg.Done()
		log.Println("Worker starting...")
		if err := w.Run(ctx); err != nil {
			log.Printf("worker Run() returned an error: %v", err)
		}
		log.Println("Worker has stopped.")
		signalChan <- os.Interrupt
	}()

	// 3. Wait for shutdown signal
	<-signalChan

	log.Println("Shutdown signal received, initiating graceful shutdown...")

	// 4. Cancel the main context to signal all services to stop.
	cancel()

	// 5. Create a shutdown-specific context with a timeout.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// 6. Wait for the worker to finish.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Worker goroutine finished gracefully.")
	case <-shutdownCtx.Done():
		log.Println("Shutdown timed out waiting for worker goroutine.")
	}

	// 7. Perform final cleanup of resources.
	log.Println("Closing resources...")
	if err := w.GracefulStop(); err != nil {
		log.Printf("error during worker graceful stop: %v", err)
	}

	if err := statsDB.Close(); err != nil {
		log.Printf("error closing database: %v", err)
	}

	if err := otelShutdown(shutdownCtx); err != nil {
		log.Printf("failed to shut down OpenTelemetry: %v", err)
	}

	log.Println("Shutdown complete.")
}
