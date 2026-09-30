package migrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMigration struct {
	version     int64
	description string
	up          func(context.Context) error
}

func (m testMigration) Version() int64      { return m.version }
func (m testMigration) Description() string { return m.description }
func (m testMigration) Up(ctx context.Context) error {
	if m.up == nil {
		return nil
	}
	return m.up(ctx)
}

type fakeStorage struct {
	applied map[int64]bool
	last    migrationVersion
	stored  []int64
	initErr error
}

func (s *fakeStorage) Init(context.Context) error { return s.initErr }
func (s *fakeStorage) LastVersion(context.Context, string) (migrationVersion, error) {
	return s.last, nil
}
func (s *fakeStorage) Applied(_ context.Context, _ string, version int64) (bool, error) {
	return s.applied[version], nil
}
func (s *fakeStorage) Store(_ context.Context, _ string, migration Migration) error {
	s.stored = append(s.stored, migration.Version())
	s.applied[migration.Version()] = true
	return nil
}

type fakeLocker struct {
	entered  int
	exited   int
	lockName string
	timeout  time.Duration
}

func (l *fakeLocker) ExecuteWithLock(_ context.Context, name string, timeout time.Duration, callback func() error) error {
	l.entered++
	l.lockName = name
	l.timeout = timeout
	defer func() { l.exited++ }()
	return callback()
}

func newTestMigrator(storage migrationStorage, locker *fakeLocker) *Migrator {
	return &Migrator{
		storage: storage, locker: locker,
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		tableName: DefaultTableName, lockTimeout: defaultLockTimeout,
	}
}

func TestMigrateOrdersAndAppliesMigrationsPerTarget(t *testing.T) {
	storage := &fakeStorage{applied: map[int64]bool{2: true}, last: migrationVersion{value: 2, valid: true}}
	locker := &fakeLocker{}
	migrator := newTestMigrator(storage, locker)
	var called []int64
	migration := func(version int64) Migration {
		return testMigration{version: version, description: "test", up: func(context.Context) error {
			called = append(called, version)
			return nil
		}}
	}

	err := migrator.Migrate(t.Context(), "application", migration(3), migration(2), migration(4))

	require.NoError(t, err)
	assert.Equal(t, []int64{3, 4}, called)
	assert.Equal(t, []int64{3, 4}, storage.stored)
	assert.Equal(t, 1, locker.entered)
	assert.Equal(t, 1, locker.exited)
	assert.Equal(t, defaultLockTimeout, locker.timeout)
}

func TestMigrateRejectsOutOfOrderMigration(t *testing.T) {
	storage := &fakeStorage{applied: make(map[int64]bool), last: migrationVersion{value: 10, valid: true}}
	migrator := newTestMigrator(storage, &fakeLocker{})

	err := migrator.Migrate(t.Context(), "mysql", testMigration{version: 9})

	assert.ErrorIs(t, err, ErrOutOfOrder)
	assert.Empty(t, storage.stored)
}

func TestMigrateReturnsMigrationError(t *testing.T) {
	want := errors.New("migration failed")
	storage := &fakeStorage{applied: make(map[int64]bool)}
	migrator := newTestMigrator(storage, &fakeLocker{})

	err := migrator.Migrate(t.Context(), "application", testMigration{version: 1, up: func(context.Context) error {
		return want
	}})

	assert.ErrorIs(t, err, want)
	assert.Empty(t, storage.stored)
}

func TestMigrateRepanicsAfterLockCleanup(t *testing.T) {
	storage := &fakeStorage{applied: make(map[int64]bool)}
	locker := &fakeLocker{}
	migrator := newTestMigrator(storage, locker)
	panicValue := &struct{ message string }{"boom"}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = migrator.Migrate(t.Context(), "application", testMigration{version: 1, up: func(context.Context) error {
			panic(panicValue)
		}})
	}()

	assert.Same(t, panicValue, recovered)
	assert.Equal(t, 1, locker.exited)
	assert.Empty(t, storage.stored)
}

func TestMigrateValidatesBeforeTakingLock(t *testing.T) {
	locker := &fakeLocker{}
	migrator := newTestMigrator(&fakeStorage{applied: make(map[int64]bool)}, locker)

	assert.ErrorIs(t, migrator.Migrate(t.Context(), "target"), ErrNoMigrations)
	assert.ErrorIs(t, migrator.Migrate(t.Context(), "target",
		testMigration{version: 1}, testMigration{version: 1}), ErrDuplicateVersion)
	assert.Equal(t, 0, locker.entered)
}

func TestValidateAndSortDoesNotMutateCallerSlice(t *testing.T) {
	migrations := []Migration{testMigration{version: 2}, testMigration{version: 1}}
	original := append([]Migration(nil), migrations...)

	ordered, err := validateAndSort(migrations)

	require.NoError(t, err)
	assert.EqualValues(t, 1, ordered[0].Version())
	assert.True(t, reflect.DeepEqual(original, migrations))
}

func TestQuoteIdentifier(t *testing.T) {
	quoted, err := quoteIdentifier("infra.schema_migrations")
	require.NoError(t, err)
	assert.Equal(t, "`infra`.`schema_migrations`", quoted)

	_, err = quoteIdentifier("migrations; DROP TABLE users")
	assert.Error(t, err)
}
