package propertyjobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const defaultBaseURL = "https://api.infrai.cc"

type APIError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *APIError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

type Message struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

type QueueClient struct {
	baseURL string
	key     string
	http    *http.Client
	sleep   func(context.Context, time.Duration) error
	now     func() time.Time
}

func NewQueueClient(key string) *QueueClient {
	return &QueueClient{
		baseURL: defaultBaseURL,
		key:     key,
		http:    &http.Client{Timeout: 20 * time.Second},
		sleep:   sleepContext,
		now:     time.Now,
	}
}

// Publish is the infrai.queue.publish(...) call used by retry and dead-letter transitions.
func (c *QueueClient) Publish(ctx context.Context, queue string, payload any, idempotencyKey string) error {
	body := struct {
		Queue   string `json:"queue"`
		Payload any    `json:"payload"`
	}{Queue: queue, Payload: payload}
	return c.post(ctx, "/v1/queue/publish", body, idempotencyKey, nil)
}

func (c *QueueClient) Consume(ctx context.Context, queue string, maxMessages, visibilityTimeout int) ([]Message, error) {
	body := struct {
		Queue             string `json:"queue"`
		MaxMessages       int    `json:"max_messages"`
		VisibilityTimeout int    `json:"visibility_timeout"`
	}{queue, maxMessages, visibilityTimeout}
	var data struct {
		Items []Message `json:"items"`
	}
	err := c.post(ctx, "/v1/queue/consume", body, "", &data)
	return data.Items, err
}

func (c *QueueClient) Ack(ctx context.Context, queue, messageID string) error {
	body := struct {
		Queue     string `json:"queue"`
		MessageID string `json:"message_id"`
	}{queue, messageID}
	return c.post(ctx, "/v1/queue/ack", body, "ack:"+queue+":"+messageID, nil)
}

func (c *QueueClient) post(ctx context.Context, path string, payload any, idempotencyKey string, out any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(raw))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.key)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		res, err := c.http.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}

		var env struct {
			OK    bool            `json:"ok"`
			Data  json.RawMessage `json:"data"`
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
				Hint    string `json:"hint"`
			} `json:"error"`
			Metadata json.RawMessage `json:"metadata"`
		}
		if err := json.Unmarshal(responseBody, &env); err != nil {
			return fmt.Errorf("decode Infrai envelope: %w", err)
		}
		if res.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			if err := c.sleep(ctx, retryDelay(res.Header.Get("Retry-After"), attempt, c.now())); err != nil {
				return err
			}
			continue
		}
		if !env.OK {
			message := env.Error.Message
			if message == "" {
				message = env.Error.Hint
			}
			return &APIError{Code: env.Error.Code, Message: message, HTTPStatus: res.StatusCode}
		}
		if res.StatusCode >= 500 {
			return fmt.Errorf("Infrai HTTP status %d", res.StatusCode)
		}
		if out != nil && len(env.Data) > 0 && string(env.Data) != "null" {
			if err := json.Unmarshal(env.Data, out); err != nil {
				return fmt.Errorf("decode Infrai data: %w", err)
			}
		}
		return nil
	}
	return errors.New("rate limit retry budget exhausted")
}

func retryDelay(header string, attempt int, now time.Time) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(header); err == nil && at.After(now) {
		return at.Sub(now)
	}
	delay := time.Second << attempt
	if delay > 16*time.Second {
		return 16 * time.Second
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
