package propertyjobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type fakeQueue struct {
	publishedQueue string
	published      any
	acked          string
}

func (f *fakeQueue) Publish(_ context.Context, queue string, payload any, _ string) error {
	f.publishedQueue, f.published = queue, payload
	return nil
}
func (f *fakeQueue) Ack(_ context.Context, _ string, messageID string) error {
	f.acked = messageID
	return nil
}

type processorFunc func(context.Context, PropertyJob) error

func (f processorFunc) Process(ctx context.Context, job PropertyJob) error { return f(ctx, job) }

func TestWorkerFailureDecision(t *testing.T) {
	tests := []struct {
		name         string
		job          PropertyJob
		processError error
		wantOutcome  Outcome
		wantQueue    string
	}{
		{
			name:        "maintenance request succeeds",
			job:         PropertyJob{JobID: "maint-7", PropertyID: "building-a", Kind: MaintenanceRequest, Attempt: 1},
			wantOutcome: Processed,
		},
		{
			name:         "tenant document gets another attempt",
			job:          PropertyJob{JobID: "lease-8", PropertyID: "building-a", Kind: TenantDocument, Attempt: 1},
			processError: errors.New("document store rejected the file"), wantOutcome: RetryQueued, wantQueue: "property-jobs",
		},
		{
			name:         "inspection reminder exhausts attempts",
			job:          PropertyJob{JobID: "inspect-9", PropertyID: "building-b", Kind: InspectionReminder, Attempt: 3},
			processError: errors.New("recipient address was rejected"), wantOutcome: DeadLettered, wantQueue: "property-jobs-dead",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue := &fakeQueue{}
			worker := Worker{
				Queue: queue, SourceQueue: "property-jobs", DeadQueue: "property-jobs-dead", MaxAttempts: 3,
				Processor: processorFunc(func(context.Context, PropertyJob) error { return tt.processError }),
			}
			payload, err := json.Marshal(tt.job)
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := worker.Handle(context.Background(), Message{MessageID: "msg-42", Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			if outcome != tt.wantOutcome {
				t.Fatalf("outcome = %q, want %q", outcome, tt.wantOutcome)
			}
			if queue.publishedQueue != tt.wantQueue {
				t.Fatalf("publish queue = %q, want %q", queue.publishedQueue, tt.wantQueue)
			}
			if queue.acked != "msg-42" {
				t.Fatalf("acked = %q, want msg-42", queue.acked)
			}
			if outcome == RetryQueued {
				retry := queue.published.(PropertyJob)
				if retry.Attempt != 2 {
					t.Fatalf("retry attempt = %d, want 2", retry.Attempt)
				}
			}
		})
	}
}
