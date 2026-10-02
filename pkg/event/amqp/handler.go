package eventamqp

import (
	"context"
	"errors"
	"math"

	"github.com/distributed-programming-2026/go-sdk/pkg/amqp"
	"github.com/distributed-programming-2026/go-sdk/pkg/event"
	"github.com/distributed-programming-2026/go-sdk/pkg/event/internal/contextmeta"
	rabbit "github.com/rabbitmq/amqp091-go"
)

func NewHandler(handler event.Handler) (amqp.Handler, error) {
	if handler == nil {
		return nil, errors.New("event/amqp: handler must not be nil")
	}
	return func(ctx context.Context, delivery amqp.Delivery) amqp.Disposition {
		if delivery.ContentType != contentType {
			return amqp.Reject
		}
		envelope, err := event.Unmarshal(delivery.Body)
		if err != nil {
			return amqp.Reject
		}
		if delivery.RoutingKey != routingKey(envelope) {
			return amqp.Reject
		}
		ctx = contextmeta.WithCorrelationID(ctx, envelope.CorrelationID())
		ctx = contextmeta.WithDelivery(ctx, deliveryMetadata(delivery))
		if err := handler.Handle(ctx, envelope); err != nil {
			return amqp.Reject
		}
		return amqp.Ack
	}, nil
}

func deliveryMetadata(delivery amqp.Delivery) contextmeta.Delivery {
	count, source := retryCount(delivery.Headers)
	return contextmeta.Delivery{
		Redelivered:      delivery.Redelivered,
		RetryCount:       count,
		RetryCountSource: source,
	}
}

func retryCount(headers rabbit.Table) (count *int64, source string) {
	if count, ok := integerHeader(headers["x-delivery-count"]); ok {
		return &count, "x-delivery-count"
	}
	deaths, ok := headers["x-death"].([]interface{})
	if !ok {
		return nil, ""
	}
	var total int64
	found := false
	for _, death := range deaths {
		table, ok := death.(rabbit.Table)
		if !ok {
			continue
		}
		count, ok := integerHeader(table["count"])
		if !ok {
			continue
		}
		total += count
		found = true
	}
	if !found {
		return nil, ""
	}
	return &total, "x-death"
}

func integerHeader(value any) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		if uint64(value) > math.MaxInt64 {
			return 0, false
		}
		return int64(value), true
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		if value > math.MaxInt64 {
			return 0, false
		}
		return int64(value), true
	default:
		return 0, false
	}
}
