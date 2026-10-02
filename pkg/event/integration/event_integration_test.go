package integration_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	sdkamqp "github.com/distributed-programming-2026/go-sdk/pkg/amqp"
	"github.com/distributed-programming-2026/go-sdk/pkg/event"
	eventamqp "github.com/distributed-programming-2026/go-sdk/pkg/event/amqp"
	"github.com/distributed-programming-2026/go-sdk/pkg/event/inbox"
	"github.com/distributed-programming-2026/go-sdk/pkg/event/outbox"
	"github.com/distributed-programming-2026/go-sdk/pkg/migrator"
	sdkmysql "github.com/distributed-programming-2026/go-sdk/pkg/mysql"
	"github.com/distributed-programming-2026/go-sdk/pkg/uow"
	rabbit "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestOutboxAMQPInboxFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := integrationClient(t)
	pool := sdkmysql.NewConnectionPool(client)
	unit := uow.NewUnitOfWork(pool, func(sdkmysql.ClientContext) struct{} { return struct{}{} })
	lockableUnit := uow.NewLockableUnitOfWork(unit)

	migrations, err := migrator.New(client, logger)
	require.NoError(t, err)
	require.NoError(t, migrations.Migrate(ctx, "event-outbox", outbox.Migrations(client)...))
	require.NoError(t, migrations.Migrate(ctx, "event-inbox", inbox.Migrations(client)...))
	_, err = client.ExecContext(ctx, "test.side-effects.create", `CREATE TABLE IF NOT EXISTS event_side_effect (
    event_id VARCHAR(64) NOT NULL PRIMARY KEY,
    payload VARCHAR(255) NOT NULL
) ENGINE=InnoDB`)
	require.NoError(t, err)
	_, err = client.ExecContext(ctx, "test.side-effects.clear", "DELETE FROM event_side_effect")
	require.NoError(t, err)
	_, err = client.ExecContext(ctx, "test.outbox.clear", "DELETE FROM event_outbox")
	require.NoError(t, err)
	_, err = client.ExecContext(ctx, "test.inbox.clear", "DELETE FROM event_inbox")
	require.NoError(t, err)

	name := fmt.Sprintf("event-sdk-%d", time.Now().UnixNano())
	exchange := &sdkamqp.ExchangeConfig{
		Name:       name,
		Kind:       rabbit.ExchangeTopic,
		AutoDelete: true,
	}
	queue := &sdkamqp.QueueConfig{
		Name:       name + ".queue",
		AutoDelete: true,
	}
	bind := &sdkamqp.BindConfig{
		QueueName:    queue.Name,
		ExchangeName: exchange.Name,
		RoutingKeys:  []string{"users.#"},
	}
	connection := sdkamqp.NewConnection("event-integration-test", sdkamqp.ConnectionConfig{
		URL:              testAMQPURL,
		ConnectTimeout:   5 * time.Second,
		ReconnectBackoff: 50 * time.Millisecond,
	}, logger)
	t.Cleanup(func() { assert.NoError(t, connection.Stop()) })
	producer := connection.Producer(exchange, nil, nil)
	directDispatcher, err := eventamqp.NewDispatcher(producer)
	require.NoError(t, err)

	applicationHandler := event.HandlerFunc(func(ctx context.Context, envelope event.Envelope) error {
		var payload wrapperspb.StringValue
		if err := envelope.UnmarshalPayload(&payload); err != nil {
			return err
		}
		return unit.ExecuteWithClientContext(ctx, func(client sdkmysql.ClientContext) error {
			_, err := client.ExecContext(
				ctx,
				"test.side-effects.insert",
				"INSERT INTO event_side_effect (event_id, payload) VALUES (?, ?)",
				envelope.ID(),
				payload.Value,
			)
			return err
		})
	})
	inboxHandler, inboxStore, err := inbox.Decorate(applicationHandler, unit, inbox.Config{
		Consumer: "integration-test",
	})
	require.NoError(t, err)
	amqpHandler, err := eventamqp.NewHandler(inboxHandler)
	require.NoError(t, err)
	connection.Consumer(ctx, amqpHandler, exchange, queue, bind, &sdkamqp.QoSConfig{
		PrefetchCount: 1,
	})
	require.NoError(t, connection.Start())

	outboxDispatcher, relay, err := outbox.Decorate(
		directDispatcher,
		lockableUnit,
		outbox.Config{Transport: "rabbitmq"},
	)
	require.NoError(t, err)
	envelope, err := event.New(ctx, "users", "created", wrapperspb.String("payload"))
	require.NoError(t, err)
	require.NoError(t, outboxDispatcher.Dispatch(ctx, envelope))

	processed, err := relay.ProcessBatch(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, processed)
	require.Eventually(t, func() bool {
		return sideEffectCount(t, client, envelope.ID()) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// The same event can be delivered again, but inbox suppresses its effect.
	require.NoError(t, directDispatcher.Dispatch(ctx, envelope))
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 1, sideEffectCount(t, client, envelope.ID()))

	deletedOutbox, err := relay.Cleanup(ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deletedOutbox)
	deletedInbox, err := inboxStore.Cleanup(ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deletedInbox)
}

func integrationClient(t *testing.T) sdkmysql.TransactionalClient {
	t.Helper()
	connector := sdkmysql.NewConnector()
	require.NoError(t, connector.Open(testMySQLDSN, sdkmysql.Config{
		MaxConnections:        8,
		ConnectionMaxLifeTime: time.Minute,
		ConnectionMaxIdleTime: time.Minute,
	}))
	t.Cleanup(func() { assert.NoError(t, connector.Close()) })
	return connector.TransactionalClient()
}

func sideEffectCount(t *testing.T, client sdkmysql.ClientContext, eventID string) int {
	t.Helper()
	var count int
	require.NoError(t, client.GetContext(
		t.Context(),
		"test.side-effects.count",
		&count,
		"SELECT COUNT(*) FROM event_side_effect WHERE event_id = ?",
		eventID,
	))
	return count
}
