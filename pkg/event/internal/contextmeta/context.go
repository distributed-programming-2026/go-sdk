package contextmeta

import "context"

type correlationIDKey struct{}
type deliveryKey struct{}

type Delivery struct {
	Redelivered      bool
	RetryCount       *int64
	RetryCountSource string
}

func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, correlationID)
}

func CorrelationID(ctx context.Context) string {
	correlationID, _ := ctx.Value(correlationIDKey{}).(string)
	return correlationID
}

func WithDelivery(ctx context.Context, delivery Delivery) context.Context {
	return context.WithValue(ctx, deliveryKey{}, delivery)
}

func DeliveryFromContext(ctx context.Context) Delivery {
	delivery, _ := ctx.Value(deliveryKey{}).(Delivery)
	return delivery
}
