package propertyjobs

import (
	"context"
	"encoding/json"
	"fmt"
)

type JobKind string

const (
	MaintenanceRequest JobKind = "maintenance_request"
	TenantDocument     JobKind = "tenant_document"
	InspectionReminder JobKind = "inspection_reminder"
)

type PropertyJob struct {
	JobID      string          `json:"job_id"`
	PropertyID string          `json:"property_id"`
	Kind       JobKind         `json:"kind"`
	Attempt    int             `json:"attempt"`
	Details    json.RawMessage `json:"details"`
}

type DeadLetter struct {
	Job            PropertyJob `json:"job"`
	SourceMessage  string      `json:"source_message_id"`
	FailureSummary string      `json:"failure_summary"`
}

type Queue interface {
	Publish(context.Context, string, any, string) error
	Ack(context.Context, string, string) error
}

type Processor interface {
	Process(context.Context, PropertyJob) error
}

type Worker struct {
	Queue       Queue
	Processor   Processor
	SourceQueue string
	DeadQueue   string
	MaxAttempts int
}

type Outcome string

const (
	Processed    Outcome = "processed"
	RetryQueued  Outcome = "retry_queued"
	DeadLettered Outcome = "dead_lettered"
)

func (w Worker) Handle(ctx context.Context, message Message) (Outcome, error) {
	var job PropertyJob
	if err := json.Unmarshal(message.Payload, &job); err != nil {
		return "", fmt.Errorf("decode property job: %w", err)
	}
	if err := validateJob(job); err != nil {
		return w.deadLetter(ctx, message.MessageID, job, err)
	}
	if err := w.Processor.Process(ctx, job); err != nil {
		if job.Attempt < w.MaxAttempts {
			job.Attempt++
			key := fmt.Sprintf("property-retry:%s:%d", job.JobID, job.Attempt)
			if publishErr := w.Queue.Publish(ctx, w.SourceQueue, job, key); publishErr != nil {
				return "", publishErr
			}
			if ackErr := w.Queue.Ack(ctx, w.SourceQueue, message.MessageID); ackErr != nil {
				return "", ackErr
			}
			return RetryQueued, nil
		}
		return w.deadLetter(ctx, message.MessageID, job, err)
	}
	if err := w.Queue.Ack(ctx, w.SourceQueue, message.MessageID); err != nil {
		return "", err
	}
	return Processed, nil
}

func (w Worker) deadLetter(ctx context.Context, sourceID string, job PropertyJob, cause error) (Outcome, error) {
	record := DeadLetter{Job: job, SourceMessage: sourceID, FailureSummary: cause.Error()}
	if err := w.Queue.Publish(ctx, w.DeadQueue, record, "property-dead:"+sourceID); err != nil {
		return "", err
	}
	if err := w.Queue.Ack(ctx, w.SourceQueue, sourceID); err != nil {
		return "", err
	}
	return DeadLettered, nil
}

func validateJob(job PropertyJob) error {
	if job.JobID == "" || job.PropertyID == "" {
		return fmt.Errorf("job_id and property_id are required")
	}
	switch job.Kind {
	case MaintenanceRequest, TenantDocument, InspectionReminder:
		return nil
	default:
		return fmt.Errorf("unknown property job kind %q", job.Kind)
	}
}
