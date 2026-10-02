package eventamqp

import (
	"context"
	"errors"
	"testing"

	sdkamqp "github.com/distributed-programming-2026/go-sdk/pkg/amqp"
	"github.com/distributed-programming-2026/go-sdk/pkg/event"
	"github.com/distributed-programming-2026/go-sdk/pkg/event/internal/contextmeta"
	rabbit "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestHandlerAcknowledgesSuccessAndPropagatesMetadata(t *testing.T) {
	const routingKey = "users.created"

	envelope, err := event.New(t.Context(), "users", "created", wrapperspb.String("payload"))
	require.NoError(t, err)
	body, err := envelope.Marshal()
	require.NoError(t, err)
	var received event.Envelope
	var delivery contextmeta.Delivery
	handler, err := NewHandler(event.HandlerFunc(func(ctx context.Context, envelope event.Envelope) error {
		received = envelope
		delivery = contextmeta.DeliveryFromContext(ctx)
		return nil
	}))
	require.NoError(t, err)

	disposition := handler(t.Context(), sdkamqp.Delivery{
		RoutingKey:  routingKey,
		ContentType: contentType,
		Headers:     rabbit.Table{"x-delivery-count": int64(3)},
		Body:        body,
		Redelivered: true,
	})

	assert.Equal(t, sdkamqp.Ack, disposition)
	assert.Equal(t, envelope.ID(), received.ID())
	assert.True(t, delivery.Redelivered)
	require.NotNil(t, delivery.RetryCount)
	assert.Equal(t, int64(3), *delivery.RetryCount)
	assert.Equal(t, "x-delivery-count", delivery.RetryCountSource)
}

func TestHandlerRejectsFailures(t *testing.T) {
	const routingKey = "users.created"

	envelope, err := event.New(t.Context(), "users", "created", wrapperspb.String("payload"))
	require.NoError(t, err)
	body, err := envelope.Marshal()
	require.NoError(t, err)
	handler, err := NewHandler(event.HandlerFunc(func(context.Context, event.Envelope) error {
		return errors.New("failed")
	}))
	require.NoError(t, err)

	assert.Equal(t, sdkamqp.Reject, handler(t.Context(), sdkamqp.Delivery{
		RoutingKey:  routingKey,
		ContentType: contentType,
		Body:        body,
	}))
	assert.Equal(t, sdkamqp.Reject, handler(t.Context(), sdkamqp.Delivery{
		RoutingKey:  routingKey,
		ContentType: "text/plain",
		Body:        body,
	}))
}
