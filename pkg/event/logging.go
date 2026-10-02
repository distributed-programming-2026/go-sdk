package event

import (
	"context"
	"log/slog"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/event/internal/contextmeta"
	"github.com/distributed-programming-2026/go-sdk/pkg/logging"
)

func DispatcherLogging(logger *slog.Logger) DispatcherMiddleware {
	if logger == nil {
		panic("event: logger is required")
	}
	logger = logger.With(logging.Target("event.dispatcher"))
	return func(next Dispatcher) Dispatcher {
		return DispatcherFunc(func(ctx context.Context, envelope Envelope) error {
			started := time.Now()
			err := next.Dispatch(ctx, envelope)
			attrs := eventLogAttrs(envelope, time.Since(started))
			if err != nil {
				attrs = append(attrs, logging.Error(err), slog.String("result", "failed"))
				logger.ErrorContext(ctx, "event dispatch completed", attrs...)
				return err
			}
			attrs = append(attrs, slog.String("result", "success"))
			logger.InfoContext(ctx, "event dispatch completed", attrs...)
			return nil
		})
	}
}

func HandlerLogging(logger *slog.Logger) HandlerMiddleware {
	if logger == nil {
		panic("event: logger is required")
	}
	logger = logger.With(logging.Target("event.handler"))
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, envelope Envelope) error {
			started := time.Now()
			err := next.Handle(ctx, envelope)
			attrs := eventLogAttrs(envelope, time.Since(started))
			delivery := contextmeta.DeliveryFromContext(ctx)
			attrs = append(attrs, slog.Bool("redelivered", delivery.Redelivered))
			if delivery.RetryCount != nil {
				attrs = append(attrs,
					slog.Int64("retry_count", *delivery.RetryCount),
					slog.String("retry_count_source", delivery.RetryCountSource),
				)
			}
			if err != nil {
				attrs = append(attrs, logging.Error(err), slog.String("result", "rejected"))
				logger.ErrorContext(ctx, "event handling completed", attrs...)
				return err
			}
			attrs = append(attrs, slog.String("result", "processed"))
			logger.InfoContext(ctx, "event handling completed", attrs...)
			return nil
		})
	}
}

func eventLogAttrs(envelope Envelope, duration time.Duration) []any {
	attrs := []any{
		slog.String("event_id", envelope.ID()),
		slog.String("event_type", envelope.Type()),
		slog.String("producer", envelope.Producer()),
		slog.String("correlation_id", envelope.CorrelationID()),
		logging.Duration(duration),
		logging.DurationNs(duration),
	}
	return attrs
}
