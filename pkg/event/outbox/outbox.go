package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/event"
	"github.com/distributed-programming-2026/go-sdk/pkg/mysql"
	"github.com/distributed-programming-2026/go-sdk/pkg/uow"
)

const defaultBatchSize = 100

type Config struct {
	Transport   string
	BatchSize   int
	LockTimeout time.Duration
}

type Relay struct {
	next        event.Dispatcher
	unit        uow.LockableUnitOfWork
	transport   string
	batchSize   int
	lockTimeout time.Duration
}

type dispatcher struct {
	unit      uow.LockableUnitOfWork
	transport string
}

func Decorate(
	next event.Dispatcher,
	unit uow.LockableUnitOfWork,
	config Config,
) (event.Dispatcher, *Relay, error) {
	if next == nil {
		return nil, nil, errors.New("event/outbox: next dispatcher must not be nil")
	}
	if unit == nil {
		return nil, nil, errors.New("event/outbox: unit of work must not be nil")
	}
	if config.Transport == "" {
		return nil, nil, errors.New("event/outbox: transport must not be empty")
	}
	if config.BatchSize == 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.BatchSize < 0 {
		return nil, nil, errors.New("event/outbox: batch size must be positive")
	}
	if config.LockTimeout < 0 {
		return nil, nil, errors.New("event/outbox: lock timeout must not be negative")
	}

	return &dispatcher{
		unit:      unit,
		transport: config.Transport,
	}, &Relay{
		next:        next,
		unit:        unit,
		transport:   config.Transport,
		batchSize:   config.BatchSize,
		lockTimeout: config.LockTimeout,
	}, nil
}

func (d *dispatcher) Dispatch(ctx context.Context, envelope event.Envelope) error {
	payload, err := envelope.Marshal()
	if err != nil {
		return fmt.Errorf("event/outbox: encode envelope: %w", err)
	}
	return d.unit.ExecuteWithClientContext(ctx, nil, func(client mysql.ClientContext) error {
		_, err := client.ExecContext(
			ctx,
			"event.outbox.store",
			`INSERT INTO event_outbox
    (transport, event_id, payload, created_at)
VALUES (?, ?, ?, ?)`,
			d.transport,
			envelope.ID(),
			payload,
			time.Now().UTC(),
		)
		if err != nil {
			return fmt.Errorf("event/outbox: store event: %w", err)
		}
		return nil
	})
}

type storedEvent struct {
	ID      uint64 `db:"id"`
	Payload []byte `db:"payload"`
}

func (r *Relay) ProcessBatch(ctx context.Context) (processed int, err error) {
	var processErr error
	err = r.unit.ExecuteWithClientContext(ctx, []uow.LockOptions{{
		Name:    "event_outbox_" + r.transport,
		Timeout: r.lockTimeout,
	}}, func(client mysql.ClientContext) error {
		var storedEvents []storedEvent
		if err := client.SelectContext(
			ctx,
			"event.outbox.pending",
			&storedEvents,
			`SELECT id, payload
FROM event_outbox
WHERE transport = ? AND published_at IS NULL
ORDER BY id
LIMIT ?`,
			r.transport,
			r.batchSize,
		); err != nil {
			return fmt.Errorf("event/outbox: select pending events: %w", err)
		}

		for _, stored := range storedEvents {
			envelope, decodeErr := event.Unmarshal(stored.Payload)
			if decodeErr != nil {
				processErr = r.recordFailure(ctx, client, stored.ID, decodeErr)
				return nil
			}
			if dispatchErr := r.next.Dispatch(ctx, envelope); dispatchErr != nil {
				processErr = r.recordFailure(ctx, client, stored.ID, dispatchErr)
				return nil
			}
			if _, markErr := client.ExecContext(
				ctx,
				"event.outbox.mark-published",
				`UPDATE event_outbox
SET published_at = ?, attempts = attempts + 1, last_error = NULL
WHERE id = ? AND published_at IS NULL`,
				time.Now().UTC(),
				stored.ID,
			); markErr != nil {
				return fmt.Errorf("event/outbox: mark event %d as published: %w", stored.ID, markErr)
			}
			processed++
		}
		return nil
	})
	return processed, errors.Join(err, processErr)
}

func (r *Relay) recordFailure(
	ctx context.Context,
	client mysql.ClientContext,
	id uint64,
	cause error,
) error {
	_, updateErr := client.ExecContext(
		ctx,
		"event.outbox.record-failure",
		`UPDATE event_outbox
SET attempts = attempts + 1, last_error = ?
WHERE id = ? AND published_at IS NULL`,
		cause.Error(),
		id,
	)
	return errors.Join(fmt.Errorf("event/outbox: publish event %d: %w", id, cause), updateErr)
}

func (r *Relay) Cleanup(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, errors.New("event/outbox: cleanup limit must be positive")
	}
	var deleted int64
	err := r.unit.ExecuteWithClientContext(ctx, nil, func(client mysql.ClientContext) error {
		result, err := client.ExecContext(
			ctx,
			"event.outbox.cleanup",
			`DELETE FROM event_outbox
WHERE transport = ? AND published_at IS NOT NULL AND published_at < ?
ORDER BY id
LIMIT ?`,
			r.transport,
			before.UTC(),
			limit,
		)
		if err != nil {
			return fmt.Errorf("event/outbox: cleanup: %w", err)
		}
		deleted, err = result.RowsAffected()
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("event/outbox: cleanup: %w", err)
	}
	return deleted, nil
}
