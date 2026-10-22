package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	propertyjobs "github.com/infrai-examples/property-job-dead-letter"
)

type propertyProcessor struct{}

func (propertyProcessor) Process(_ context.Context, job propertyjobs.PropertyJob) error {
	log.Printf("handled kind=%s job=%s property=%s", job.Kind, job.JobID, job.PropertyID)
	return nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	source := envOr("PROPERTY_JOB_QUEUE", "property-jobs")
	dead := envOr("PROPERTY_DEAD_QUEUE", "property-jobs-dead")
	client := propertyjobs.NewQueueClient(key)
	worker := propertyjobs.Worker{
		Queue: client, Processor: propertyProcessor{}, SourceQueue: source, DeadQueue: dead, MaxAttempts: 3,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	messages, err := client.Consume(ctx, source, 10, 60)
	if err != nil {
		log.Fatal(err)
	}
	for _, message := range messages {
		outcome, err := worker.Handle(ctx, message)
		if err != nil {
			log.Printf("message=%s error=%v", message.MessageID, err)
			continue
		}
		fmt.Printf("message=%s outcome=%s\n", message.MessageID, outcome)
	}
	if len(messages) == 0 {
		fmt.Println("no property jobs ready")
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
