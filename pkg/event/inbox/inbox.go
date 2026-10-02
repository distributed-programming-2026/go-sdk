package inbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/event"
	"github.com/distributed-programming-2026/go-sdk/pkg/event/internal/contextmeta"
	"github.com/distributed-programming-2026/go-sdk/pkg/mysql"
	"github.com/distributed-programming-2026/go-sdk/pkg/uow"
)

type Config struct {
	Consumer string
}

type Inbox struct {
	unit     uow.UnitOfWork
	consumer string
}

type handler struct {
	next     event.Handler
	unit     uow.UnitOfWork
	consumer string
}

func Decorate(
	next event.Handler,
	unit uow.UnitOfWork,
	config Config,
) (event.Handler, *Inbox, error) {
	if next == nil {
		return nil, nil, errors.New("event/inbox: next handler must not be nil")
	}
	if unit == nil {
		return nil, nil, errors.New("event/inbox: unit of work must not be nil")
	}
	if config.Consumer == "" {
		return nil, nil, errors.New("event/inbox: consumer must not be empty")
	}
	return &handler{
		next:     next,
		unit:     unit,
		consumer: config.Consumer,
	}, &Inbox{
		unit:     unit,
		consumer: config.Consumer,
	}, nil
}

func (h *handler) Handle(ctx context.Context, envelope event.Envelope) error {
	return h.unit.ExecuteWithClientContext(ctx, func(client mysql.ClientContext) error {
		result, err := client.ExecContext(
			ctx,
			"event.inbox.reserve",
			`INSERT INTO event_inbox
    (consumer, event_id, producer, event_type, occurred_at, processed_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE event_id = event_id`,
			h.consumer,
			envelope.ID(),
			envelope.Producer(),
			envelope.Type(),
			envelope.OccurredAt(),
			time.Now().UTC(),
		)
		if err != nil {
			return fmt.Errorf("event/inbox: reserve event: %w", err)
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("event/inbox: inspect reservation: %w", err)
		}
		if inserted == 0 {
			return nil
		}
		handlerContext := contextmeta.WithCorrelationID(ctx, envelope.CorrelationID())
		if err := h.next.Handle(handlerContext, envelope); err != nil {
			return fmt.Errorf("event/inbox: handle event: %w", err)
		}
		return nil
	})
}

func (i *Inbox) Cleanup(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, errors.New("event/inbox: cleanup limit must be positive")
	}
	var deleted int64
	err := i.unit.ExecuteWithClientContext(ctx, func(client mysql.ClientContext) error {
		result, err := client.ExecContext(
			ctx,
			"event.inbox.cleanup",
			`DELETE FROM event_inbox
WHERE consumer = ? AND processed_at < ?
ORDER BY processed_at
LIMIT ?`,
			i.consumer,
			before.UTC(),
			limit,
		)
		if err != nil {
			return fmt.Errorf("event/inbox: cleanup: %w", err)
		}
		deleted, err = result.RowsAffected()
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("event/inbox: cleanup: %w", err)
	}
	return deleted, nil
}
