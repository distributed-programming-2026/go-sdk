package outbox

import (
	"context"
	"errors"

	"github.com/distributed-programming-2026/go-sdk/pkg/migrator"
	"github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

func Migrations(client mysql.ClientContext) []migrator.Migration {
	if client == nil {
		panic("event/outbox: migration client must not be nil")
	}
	return []migrator.Migration{createOutboxMigration{client: client}}
}

type createOutboxMigration struct {
	client mysql.ClientContext
}

func (createOutboxMigration) Version() int64 { return 2026100201 }

func (createOutboxMigration) Description() string { return "create event outbox table" }

func (m createOutboxMigration) Up(ctx context.Context) error {
	if ctx == nil {
		return errors.New("event/outbox: migration context must not be nil")
	}
	_, err := m.client.ExecContext(ctx, "event.outbox.migration.create", `CREATE TABLE IF NOT EXISTS event_outbox (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    transport VARCHAR(128) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    payload LONGBLOB NOT NULL,
    created_at DATETIME(6) NOT NULL,
    published_at DATETIME(6) NULL,
    attempts BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_event_outbox_transport_event (transport, event_id),
    KEY ix_event_outbox_pending (transport, published_at, id),
    KEY ix_event_outbox_cleanup (transport, published_at)
) ENGINE=InnoDB CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci`)
	return err
}
