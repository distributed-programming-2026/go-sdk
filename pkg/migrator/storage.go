package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	sdkmysql "github.com/distributed-programming-2026/go-sdk/pkg/mysql"
)

var identifierPart = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

type migrationVersion struct {
	value int64
	valid bool
}

type migrationStorage interface {
	Init(context.Context) error
	LastVersion(context.Context, string) (migrationVersion, error)
	Applied(context.Context, string, int64) (bool, error)
	Store(context.Context, string, Migration) error
}

type storage struct {
	table  string
	client sdkmysql.ClientContext
}

func newStorage(tableName string, client sdkmysql.ClientContext) (*storage, error) {
	table, err := quoteIdentifier(tableName)
	if err != nil {
		return nil, fmt.Errorf("invalid migration table name: %w", err)
	}
	return &storage{table: table, client: client}, nil
}

func (s *storage) Init(ctx context.Context) error {
	query := `CREATE TABLE IF NOT EXISTS ` + s.table + ` (
  target VARCHAR(255) NOT NULL,
  version BIGINT NOT NULL,
  description TEXT NOT NULL,
  applied_at DATETIME(6) NOT NULL,
  PRIMARY KEY (target, version)
) ENGINE=InnoDB CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci`
	_, err := s.client.ExecContext(ctx, "migrator.storage.init", query)
	return err
}

func (s *storage) LastVersion(ctx context.Context, target string) (migrationVersion, error) {
	var version sql.NullInt64
	query := `SELECT MAX(version) FROM ` + s.table + ` WHERE target = ?`
	if err := s.client.GetContext(ctx, "migrator.storage.last_version", &version, query, target); err != nil {
		return migrationVersion{}, err
	}
	return migrationVersion{value: version.Int64, valid: version.Valid}, nil
}

func (s *storage) Applied(ctx context.Context, target string, version int64) (bool, error) {
	var applied bool
	query := `SELECT EXISTS(SELECT 1 FROM ` + s.table + ` WHERE target = ? AND version = ?)`
	err := s.client.GetContext(ctx, "migrator.storage.applied", &applied, query, target, version)
	return applied, err
}

func (s *storage) Store(ctx context.Context, target string, migration Migration) error {
	query := `INSERT INTO ` + s.table + ` (target, version, description, applied_at) VALUES (?, ?, ?, ?)`
	_, err := s.client.ExecContext(ctx, "migrator.storage.store", query,
		target, migration.Version(), migration.Description(), time.Now().UTC())
	return err
}

func quoteIdentifier(identifier string) (string, error) {
	parts := strings.Split(identifier, ".")
	if len(parts) == 0 || len(parts) > 2 {
		return "", fmt.Errorf("expected table or schema.table, got %q", identifier)
	}
	quoted := make([]string, len(parts))
	for index, part := range parts {
		if !identifierPart.MatchString(part) {
			return "", fmt.Errorf("invalid identifier %q", part)
		}
		quoted[index] = "`" + part + "`"
	}
	return strings.Join(quoted, "."), nil
}
