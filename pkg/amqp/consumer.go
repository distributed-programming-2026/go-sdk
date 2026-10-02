package amqp

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	rabbit "github.com/rabbitmq/amqp091-go"
)

// Disposition determines how a consumed message is settled. Nack requeues the
// message, while Reject dead-letters (or drops) it.
type Disposition uint8

const (
	Ack Disposition = iota
	Nack
	Reject
)

type Handler func(context.Context, Delivery) Disposition

type Consumer interface {
	Channel
	Close(context.Context) error
}

var consumerSequence atomic.Uint64

func NewConsumer(
	ctx context.Context,
	handler Handler,
	exchange *ExchangeConfig,
	queue *QueueConfig,
	bind *BindConfig,
	qos *QoSConfig,
	logger *slog.Logger,
) Consumer {
	return newConsumer(ctx, handler, exchange, queue, bind, qos, logger)
}

func newConsumer(
	ctx context.Context,
	handler Handler,
	exchange *ExchangeConfig,
	queue *QueueConfig,
	bind *BindConfig,
	qos *QoSConfig,
	logger *slog.Logger,
) *consumer {
	if ctx == nil {
		panic("amqp: context is required")
	}
	if logger == nil {
		panic("amqp: logger is required")
	}
	if queue == nil {
		panic("amqp: queue config is required")
	}
	if handler == nil {
		panic("amqp: handler is required")
	}
	return &consumer{
		ctx:      ctx,
		handler:  handler,
		exchange: exchange,
		queue:    queue,
		bind:     bind,
		qos:      qos,
		logger:   logger,
		tag:      fmt.Sprintf("go-sdk-%d", consumerSequence.Add(1)), closeDone: make(chan struct{}),
	}
}

type consumer struct {
	ctx      context.Context
	handler  Handler
	exchange *ExchangeConfig
	queue    *QueueConfig
	bind     *BindConfig
	qos      *QoSConfig
	logger   *slog.Logger

	mu        sync.Mutex
	channel   *rabbit.Channel
	reconnect func()
	tag       string

	lifecycleMu sync.Mutex
	closed      bool
	active      sync.WaitGroup
	closeOnce   sync.Once
	closeDone   chan struct{}
	closeErr    error
}

func (c *consumer) Connect(conn *rabbit.Connection) (err error) {
	c.lifecycleMu.Lock()
	if c.closed {
		c.lifecycleMu.Unlock()
		return ErrClosed
	}
	c.active.Add(1)
	c.lifecycleMu.Unlock()
	defer c.active.Done()

	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = channel.Close()
		}
	}()
	if c.exchange != nil {
		if err = declareExchange(channel, *c.exchange); err != nil {
			return fmt.Errorf("declare exchange: %w", err)
		}
	}
	if c.queue.DLQ != nil {
		if err = declareDLQ(channel, *c.queue.DLQ); err != nil {
			return err
		}
	}
	if err = declareQueue(channel, *c.queue); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}
	if c.bind != nil {
		if err = declareBind(channel, *c.bind); err != nil {
			return fmt.Errorf("bind queue: %w", err)
		}
	}
	if c.qos != nil {
		if err = channel.Qos(c.qos.PrefetchCount, c.qos.PrefetchSize, c.qos.Global); err != nil {
			return fmt.Errorf("configure QoS: %w", err)
		}
	}
	deliveries, err := channel.Consume(c.queue.Name, c.tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume queue: %w", err)
	}

	c.mu.Lock()
	old := c.channel
	c.channel = channel
	c.mu.Unlock()
	if old != nil && !old.IsClosed() {
		_ = old.Close()
	}
	c.watch(channel, deliveries)
	return nil
}

func (c *consumer) watch(channel *rabbit.Channel, deliveries <-chan rabbit.Delivery) {
	closed := channel.NotifyClose(make(chan *rabbit.Error, 1))
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				c.startClose()
				return
			case delivery, ok := <-deliveries:
				if !ok {
					deliveries = nil
					continue
				}
				c.handle(delivery)
			case <-closed:
				c.mu.Lock()
				current := c.channel == channel
				c.mu.Unlock()
				c.lifecycleMu.Lock()
				closing := c.closed
				c.lifecycleMu.Unlock()
				if current && !closing && c.ctx.Err() == nil && c.reconnect != nil {
					c.reconnect()
				}
				return
			}
		}
	}()
}

func (c *consumer) handle(message rabbit.Delivery) {
	c.lifecycleMu.Lock()
	if c.closed {
		c.lifecycleMu.Unlock()
		if err := message.Nack(false, true); err != nil {
			c.logger.Error("failed to requeue AMQP message during shutdown", "error", err)
		}
		return
	}
	c.active.Add(1)
	c.lifecycleMu.Unlock()
	defer c.active.Done()

	delivery := Delivery{RoutingKey: message.RoutingKey, CorrelationID: message.CorrelationId,
		ContentType: message.ContentType, Type: message.Type, MessageID: message.MessageId,
		Headers: message.Headers, Body: message.Body, Redelivered: message.Redelivered}
	disposition := c.handler(c.ctx, delivery)
	switch disposition {
	case Ack:
		if err := message.Ack(false); err != nil {
			c.logger.Error("failed to ack AMQP message", "error", err)
		}
	case Nack:
		if err := message.Nack(false, true); err != nil {
			c.logger.Error("failed to nack AMQP message", "error", err)
		}
	case Reject:
		if err := message.Reject(false); err != nil {
			c.logger.Error("failed to reject AMQP message", "error", err)
		}
	default:
		panic(fmt.Sprintf("amqp: invalid handler disposition %d", disposition))
	}
}

func (c *consumer) setReconnect(reconnect func()) { c.reconnect = reconnect }

func (c *consumer) Close(ctx context.Context) error {
	if ctx == nil {
		panic("amqp: context is required")
	}
	c.startClose()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeDone:
		return c.closeErr
	}
}

func (c *consumer) startClose() {
	c.closeOnce.Do(func() {
		c.lifecycleMu.Lock()
		c.closed = true
		c.lifecycleMu.Unlock()

		c.mu.Lock()
		if c.channel != nil && !c.channel.IsClosed() {
			c.closeErr = c.channel.Cancel(c.tag, false)
		}
		c.mu.Unlock()

		go func() {
			c.active.Wait()
			c.mu.Lock()
			if c.channel != nil && !c.channel.IsClosed() {
				if err := c.channel.Close(); c.closeErr == nil {
					c.closeErr = err
				}
			}
			c.mu.Unlock()
			close(c.closeDone)
		}()
	})
}

func (c *consumer) shutdown(ctx context.Context) error { return c.Close(ctx) }
