package inbox

import (
	"context"
	"errors"

	"github.com/distributed-programming-2026/go-sdk/pkg/migrator"
	"github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

func Migrations(client mysql.ClientContext) []migrator.Migration {
	if client == nil {
		panic("event/inbox: migration client must not be nil")
	}
	return []migrator.Migration{createInboxMigration{client: client}}
}

type createInboxMigration struct {
	client mysql.ClientContext
}

func (createInboxMigration) Version() int64 { return 2026100201 }

func (createInboxMigration) Description() string { return "create event inbox table" }

func (m createInboxMigration) Up(ctx context.Context) error {
	if ctx == nil {
		return errors.New("event/inbox: migration context must not be nil")
	}
	_, err := m.client.ExecContext(ctx, "event.inbox.migration.create", `CREATE TABLE IF NOT EXISTS event_inbox (
    consumer VARCHAR(128) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    producer VARCHAR(128) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    processed_at DATETIME(6) NOT NULL,
    PRIMARY KEY (consumer, event_id),
    KEY ix_event_inbox_cleanup (consumer, processed_at)
) ENGINE=InnoDB CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci`)
	return err
}
