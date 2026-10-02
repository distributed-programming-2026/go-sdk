package eventamqp

import (
	"context"
	"errors"
	"fmt"

	"github.com/distributed-programming-2026/go-sdk/pkg/amqp"
	"github.com/distributed-programming-2026/go-sdk/pkg/event"
)

const contentType = "application/vnd.distributed-programming.event+protobuf"

type dispatcher struct {
	producer amqp.Producer
}

func NewDispatcher(producer amqp.Producer) (event.Dispatcher, error) {
	if producer == nil {
		return nil, errors.New("event/amqp: producer must not be nil")
	}
	return &dispatcher{producer: producer}, nil
}

func (d *dispatcher) Dispatch(ctx context.Context, envelope event.Envelope) error {
	payload, err := envelope.Marshal()
	if err != nil {
		return fmt.Errorf("event/amqp: encode envelope: %w", err)
	}
	return d.producer.Publish(ctx, amqp.Delivery{
		RoutingKey:    routingKey(envelope),
		CorrelationID: envelope.CorrelationID(),
		ContentType:   contentType,
		Type:          envelope.Type(),
		MessageID:     envelope.ID(),
		Body:          payload,
	})
}

func routingKey(envelope event.Envelope) string {
	return envelope.Producer() + "." + envelope.Type()
}
