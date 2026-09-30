package amqp

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	rabbit "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultTestTimeout = 45 * time.Second

func TestRequiredDependencies(t *testing.T) {
	queue := &QueueConfig{Name: "required-dependencies"}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := func(context.Context, Delivery) Disposition { return Ack }
	var nilContext context.Context

	assert.PanicsWithValue(t, "amqp: logger is required", func() {
		NewConnection("test", ConnectionConfig{}, nil)
	})
	assert.PanicsWithValue(t, "amqp: logger is required", func() {
		NewProducer("test", nil, queue, nil, nil)
	})
	assert.PanicsWithValue(t, "amqp: context is required", func() {
		NewConsumer(nilContext, handler, queue, nil, nil, logger)
	})
	assert.PanicsWithValue(t, "amqp: logger is required", func() {
		NewConsumer(context.Background(), handler, queue, nil, nil, nil)
	})
}

func TestDirectQueueAndPublisherConfirm(t *testing.T) {
	queue := QueueConfig{Name: uniqueName(t, "direct"), AutoDelete: true}
	received := make(chan Delivery, 1)
	connection := testConnection(t)
	producer := connection.Producer(nil, &queue, nil)
	connection.Consumer(context.Background(), func(_ context.Context, delivery Delivery) Disposition {
		received <- delivery
		return Ack
	}, &queue, nil, &QoSConfig{PrefetchCount: 1})
	require.NoError(t, connection.Start())

	require.NoError(t, producer.Publish(testContext(t), Delivery{Body: []byte("direct"), CorrelationID: "correlation-1"}))
	message := receive(t, received)
	assert.Equal(t, []byte("direct"), message.Body)
	assert.Equal(t, "correlation-1", message.CorrelationID)
}

func TestDirectExchange(t *testing.T) {
	name := uniqueName(t, "exchange")
	exchange := ExchangeConfig{Name: name, Kind: rabbit.ExchangeDirect, AutoDelete: true}
	queue := QueueConfig{Name: name + ".queue", AutoDelete: true}
	bind := BindConfig{QueueName: queue.Name, ExchangeName: exchange.Name, RoutingKeys: []string{"orders.created"}}
	received := make(chan Delivery, 1)
	connection := testConnection(t)
	producer := connection.Producer(&exchange, &queue, &bind)
	connection.Consumer(context.Background(), func(_ context.Context, delivery Delivery) Disposition {
		received <- delivery
		return Ack
	}, &queue, &bind, nil)
	require.NoError(t, connection.Start())
	require.NoError(t, producer.Publish(testContext(t), Delivery{RoutingKey: "orders.created", Body: []byte("exchange")}))
	assert.Equal(t, []byte("exchange"), receive(t, received).Body)
}

func TestNackRedeliveryAndRejectToDLQ(t *testing.T) {
	name := uniqueName(t, "retry")
	dlq := DLQConfig{
		Exchange: ExchangeConfig{Name: name + ".dlx", Kind: rabbit.ExchangeDirect, AutoDelete: true},
		Queue:    name + ".dlq", RoutingKey: "dead", Durable: false,
	}
	queue := QueueConfig{Name: name, AutoDelete: true, DLQ: &dlq}
	redelivered := make(chan bool, 1)
	var attempts atomic.Int32
	connection := testConnection(t)
	producer := connection.Producer(nil, &queue, nil)
	connection.Consumer(context.Background(), func(_ context.Context, delivery Delivery) Disposition {
		switch attempts.Add(1) {
		case 1:
			return Nack
		default:
			redelivered <- delivery.Redelivered
			return Reject
		}
	}, &queue, nil, &QoSConfig{PrefetchCount: 1})
	require.NoError(t, connection.Start())
	require.NoError(t, producer.Publish(testContext(t), Delivery{Body: []byte("retry-me")}))
	assert.True(t, receive(t, redelivered), "a nacked and requeued message must be marked as redelivered")

	raw, err := rabbit.Dial(testAMQPURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	channel, err := raw.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { _ = channel.Close() })
	require.Eventually(t, func() bool {
		message, ok, getErr := channel.Get(dlq.Queue, true)
		return getErr == nil && ok && string(message.Body) == "retry-me"
	}, 10*time.Second, 100*time.Millisecond)
}

func TestReconnectsChannelAndConnection(t *testing.T) {
	queue := QueueConfig{Name: uniqueName(t, "reconnect"), AutoDelete: true}
	received := make(chan Delivery, 2)
	connection := testConnection(t).(*connection)
	producer := connection.Producer(nil, &queue, nil).(*producer)
	connection.Consumer(context.Background(), func(_ context.Context, delivery Delivery) Disposition {
		received <- delivery
		return Ack
	}, &queue, nil, nil)
	require.NoError(t, connection.Start())

	producer.mu.RLock()
	channel := producer.channel
	producer.mu.RUnlock()
	require.NoError(t, channel.Close())
	require.Eventually(t, func() bool {
		return producer.Publish(testContext(t), Delivery{Body: []byte("after-channel")}) == nil
	}, 10*time.Second, 100*time.Millisecond)
	assert.Equal(t, "after-channel", string(receive(t, received).Body))

	exitCode, output, err := testRabbit.Exec(context.Background(), []string{"rabbitmqctl", "close_all_connections", "integration test"})
	require.NoError(t, err)
	data, _ := io.ReadAll(output)
	require.Equalf(t, 0, exitCode, "rabbitmqctl output: %s", data)
	require.Eventually(t, func() bool {
		return producer.Publish(testContext(t), Delivery{Body: []byte("after-connection")}) == nil
	}, 20*time.Second, 200*time.Millisecond)
	assert.Equal(t, "after-connection", string(receive(t, received).Body))
}

func TestGracefulConsumerAndProducerClose(t *testing.T) {
	queue := QueueConfig{Name: uniqueName(t, "graceful"), AutoDelete: true}
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	handlerFinished := make(chan struct{})
	connection := testConnection(t)
	producer := connection.Producer(nil, &queue, nil)
	consumer := connection.Consumer(context.Background(), func(context.Context, Delivery) Disposition {
		close(handlerStarted)
		<-releaseHandler
		close(handlerFinished)
		return Ack
	}, &queue, nil, nil)
	require.NoError(t, connection.Start())
	require.NoError(t, producer.Publish(testContext(t), Delivery{Body: []byte("in-flight")}))
	receive(t, handlerStarted)

	shortCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, consumer.Close(shortCtx), context.DeadlineExceeded)
	select {
	case <-handlerFinished:
		t.Fatal("consumer closed before the active handler completed")
	default:
	}

	close(releaseHandler)
	require.NoError(t, consumer.Close(testContext(t)))
	receive(t, handlerFinished)
	require.NoError(t, producer.Close(testContext(t)))
	assert.ErrorIs(t, producer.Publish(testContext(t), Delivery{Body: []byte("too-late")}), ErrClosed)
}

func testConnection(t *testing.T) Connection {
	t.Helper()
	connection := NewConnection("amqp-integration-test", ConnectionConfig{
		URL: testAMQPURL, ConnectTimeout: 5 * time.Second, ReconnectBackoff: 50 * time.Millisecond,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { assert.NoError(t, connection.Stop()) })
	return connection
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for AMQP message")
		var zero T
		return zero
	}
}

func uniqueName(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("go-sdk.%s.%d", prefix, time.Now().UnixNano())
}
