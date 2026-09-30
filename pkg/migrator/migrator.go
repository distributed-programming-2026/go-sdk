// Package migrator applies ordered, target-scoped MySQL migrations.
package migrator

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/logging"
	sdkmysql "github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

const (
	DefaultTableName   = "migrations"
	defaultLockTimeout = 5 * time.Second
)

var (
	ErrNoMigrations     = errors.New("migrations must not be empty")
	ErrDuplicateVersion = errors.New("duplicate migration version")
	ErrOutOfOrder       = errors.New("migration is older than the last applied migration")
)

// Migration is one forward-only migration. Up should return only after all of
// its changes are durable. A panic from Up is deliberately not recovered.
type Migration interface {
	Version() int64
	Description() string
	Up(ctx context.Context) error
}

// Option customizes a Migrator.
type Option func(*config) error

type config struct {
	tableName   string
	lockTimeout time.Duration
}

// WithTableName stores migration history in a separate table. It is useful
// when infrastructure and application migrations must not share a registry.
func WithTableName(tableName string) Option {
	return func(config *config) error {
		if _, err := quoteIdentifier(tableName); err != nil {
			return err
		}
		config.tableName = tableName
		return nil
	}
}

// WithLockTimeout controls how long Migrate waits for another migrator.
func WithLockTimeout(timeout time.Duration) Option {
	return func(config *config) error {
		if timeout < 0 {
			return fmt.Errorf("lock timeout must not be negative: %s", timeout)
		}
		config.lockTimeout = timeout
		return nil
	}
}

// Migrator serializes and records migrations in MySQL.
type Migrator struct {
	storage     migrationStorage
	locker      sdkmysql.Locker
	logger      *slog.Logger
	tableName   string
	lockTimeout time.Duration
}

// New constructs a migrator backed by client. The logger and client are
// required; invalid configuration is reported as an error.
func New(client sdkmysql.TransactionalClient, logger *slog.Logger, options ...Option) (*Migrator, error) {
	if client == nil {
		return nil, errors.New("mysql client must not be nil")
	}
	if logger == nil {
		return nil, errors.New("logger must not be nil")
	}

	config := config{tableName: DefaultTableName, lockTimeout: defaultLockTimeout}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("migrator option must not be nil")
		}
		if err := option(&config); err != nil {
			return nil, fmt.Errorf("configure migrator: %w", err)
		}
	}
	storage, err := newStorage(config.tableName, client)
	if err != nil {
		return nil, err
	}
	return &Migrator{
		storage:     storage,
		locker:      sdkmysql.NewLocker(sdkmysql.NewConnectionPool(client)),
		logger:      logger.With(logging.Target("migrator")),
		tableName:   config.tableName,
		lockTimeout: config.lockTimeout,
	}, nil
}

// NewMigrator is an explicit spelling of New.
func NewMigrator(client sdkmysql.TransactionalClient, logger *slog.Logger, options ...Option) (*Migrator, error) {
	return New(client, logger, options...)
}

// Migrate applies migrations for target in ascending version order. Migration
// ordering and version uniqueness are independent for every target.
func (m *Migrator) Migrate(ctx context.Context, target string, migrations ...Migration) error {
	if ctx == nil {
		return errors.New("context must not be nil")
	}
	if target == "" {
		return errors.New("migration target must not be empty")
	}
	ordered, err := validateAndSort(migrations)
	if err != nil {
		return err
	}

	return m.locker.ExecuteWithLock(ctx, m.lockName(target), m.lockTimeout, func() error {
		if err := m.storage.Init(ctx); err != nil {
			return fmt.Errorf("initialize migration storage: %w", err)
		}
		lastVersion, err := m.storage.LastVersion(ctx, target)
		if err != nil {
			return fmt.Errorf("read last migration version for target %q: %w", target, err)
		}

		for _, migration := range ordered {
			applied, err := m.storage.Applied(ctx, target, migration.Version())
			if err != nil {
				return fmt.Errorf("check migration %q/%d: %w", target, migration.Version(), err)
			}
			if applied {
				m.logger.DebugContext(ctx, "migration already applied",
					slog.String("migration_target", target), slog.Int64("version", migration.Version()))
				continue
			}
			if lastVersion.valid && migration.Version() < lastVersion.value {
				return fmt.Errorf("%w: target %q version %d, last applied version %d",
					ErrOutOfOrder, target, migration.Version(), lastVersion.value)
			}
			if err := migration.Up(ctx); err != nil {
				return fmt.Errorf("apply migration %q/%d (%s): %w",
					target, migration.Version(), migration.Description(), err)
			}
			if err := m.storage.Store(ctx, target, migration); err != nil {
				return fmt.Errorf("store migration %q/%d: %w", target, migration.Version(), err)
			}
			lastVersion = migrationVersion{value: migration.Version(), valid: true}
			m.logger.InfoContext(ctx, "migration applied",
				slog.String("migration_target", target),
				slog.Int64("version", migration.Version()),
				slog.String("description", migration.Description()))
		}
		return nil
	})
}

func (m *Migrator) lockName(target string) string {
	// MySQL lock names are limited to 64 characters. A digest avoids collisions
	// between long table/target combinations after mysql.Lock adds the database.
	digest := sha256.Sum256([]byte(m.tableName + "\x00" + target))
	return fmt.Sprintf("migrator:%x", digest[:16])
}

func validateAndSort(migrations []Migration) ([]Migration, error) {
	if len(migrations) == 0 {
		return nil, ErrNoMigrations
	}
	ordered := slices.Clone(migrations)
	for index, migration := range ordered {
		if migration == nil {
			return nil, fmt.Errorf("migration at index %d is nil", index)
		}
	}
	slices.SortFunc(ordered, func(left, right Migration) int {
		return cmp.Compare(left.Version(), right.Version())
	})
	for index := 1; index < len(ordered); index++ {
		if ordered[index-1].Version() == ordered[index].Version() {
			return nil, fmt.Errorf("%w: %d", ErrDuplicateVersion, ordered[index].Version())
		}
	}
	return ordered, nil
}
