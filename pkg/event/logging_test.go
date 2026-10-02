package event

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/distributed-programming-2026/go-sdk/pkg/event/internal/contextmeta"
	"github.com/distributed-programming-2026/go-sdk/pkg/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestHandlerLoggingIncludesRetryMetadata(t *testing.T) {
	var output bytes.Buffer
	logger := logging.New(logging.Config{
		AppID:  "event-test",
		Output: &output,
	})
	retryCount := int64(4)
	envelope, err := New(t.Context(), "users", "created", wrapperspb.String("payload"))
	require.NoError(t, err)
	ctx := contextmeta.WithDelivery(t.Context(), contextmeta.Delivery{
		Redelivered:      true,
		RetryCount:       &retryCount,
		RetryCountSource: "x-delivery-count",
	})
	handler := ChainHandler(
		HandlerFunc(func(context.Context, Envelope) error { return nil }),
		HandlerLogging(logger),
	)

	require.NoError(t, handler.Handle(ctx, envelope))
	var record map[string]any
	require.NoError(t, json.Unmarshal(output.Bytes(), &record))
	assert.Equal(t, "event handling completed", record["msg"])
	assert.Equal(t, true, record["redelivered"])
	assert.Equal(t, float64(4), record["retry_count"])
	assert.Equal(t, "x-delivery-count", record["retry_count_source"])
	assert.Equal(t, "processed", record["result"])
}
