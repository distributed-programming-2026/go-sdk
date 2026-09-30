package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

func TestLoggerFieldsAndDebugLevel(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{AppID: "orders", Debug: true, Output: &output})
	logger.Debug("handled", DurationNs(1500*time.Millisecond), Duration(1500*time.Millisecond))

	record := decodeRecord(t, output.Bytes())
	if record[appIDKey] != "orders" || record["level"] != "DEBUG" {
		t.Fatalf("unexpected record: %#v", record)
	}
	if record[durationNSKey] != float64(1_500_000_000) || record[durationKey] != "1.5s" {
		t.Fatalf("unexpected durations: %#v", record)
	}
	if _, ok := record[timestampKey].(float64); !ok {
		t.Fatalf("@timestamp must be a JSON number: %#v", record[timestampKey])
	}
	if _, err := time.Parse(time.RFC3339Nano, record[timeKey].(string)); err != nil {
		t.Fatalf("invalid human-readable time: %v", err)
	}
}

func TestTarget(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{Output: &output})
	logger.Info("connected", Target("amqp"))

	record := decodeRecord(t, output.Bytes())
	if record[targetKey] != "amqp" {
		t.Fatalf("unexpected target: %#v", record[targetKey])
	}
}

func TestDebugDisabledByDefault(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{Output: &output})
	logger.Debug("hidden")
	if output.Len() != 0 {
		t.Fatalf("debug record was written: %s", output.String())
	}
}

func TestErrorSupportsJoinedAndWrappedErrors(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{Output: &output})
	first := stackError{message: "first", stack: "first.go:10"}
	second := fmt.Errorf("wrapped: %w", stackError{message: "second", stack: "second.go:20"})
	joined := errors.Join(first, second)

	logger.Error("failed", Error(joined))
	record := decodeRecord(t, output.Bytes())
	if record[errorKey] != joined.Error() {
		t.Fatalf("unexpected error: %#v", record[errorKey])
	}
	stack, ok := record[stackKey].(string)
	if !ok || !bytes.Contains([]byte(stack), []byte("first.go:10")) || !bytes.Contains([]byte(stack), []byte("second.go:20")) {
		t.Fatalf("joined stacks were not retained: %#v", record[stackKey])
	}
}

func TestAttrsAddedWithLoggerWith(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{Output: &output}).With(Error(stackError{message: "boom", stack: "worker.go:42"}))
	logger.Info("message", slog.String("key", "value"))
	record := decodeRecord(t, output.Bytes())
	if record[errorKey] != "boom" || record[stackKey] != "worker.go:42" {
		t.Fatalf("unexpected error fields: %#v", record)
	}
}

func TestGroupsDoNotMoveAutomaticFields(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{AppID: "worker", Output: &output}).WithGroup("request")
	logger.Info("message", slog.String("id", "42"))
	record := decodeRecord(t, output.Bytes())
	if record[appIDKey] != "worker" || record[timeKey] == nil || record[timestampKey] == nil {
		t.Fatalf("automatic fields must remain at root: %#v", record)
	}
	group, ok := record["request"].(map[string]any)
	if !ok || group["id"] != "42" {
		t.Fatalf("unexpected group: %#v", record["request"])
	}
}

type stackError struct {
	message string
	stack   string
}

func (e stackError) Error() string      { return e.message }
func (e stackError) StackTrace() string { return e.stack }

func decodeRecord(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode log record %q: %v", data, err)
	}
	return record
}
