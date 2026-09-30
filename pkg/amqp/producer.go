package amqp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	rabbit "github.com/rabbitmq/amqp091-go"
)

var (
	ErrChannelUnavailable = errors.New("amqp: channel is unavailable")
	ErrPublishNacked      = errors.New("amqp: broker negatively acknowledged publish")
)

type Delivery struct {
	RoutingKey    string
	CorrelationID string
	ContentType   string
	Type          string
	MessageID     string
	Headers       rabbit.Table
	Body          []byte
	Redelivered   bool
}

type Producer interface {
	Channel
	Publish(context.Context, Delivery) error
	Close(context.Context) error
}

func NewProducer(appID string, exchange *ExchangeConfig, queue *QueueConfig, bind *BindConfig, logger *slog.Logger) Producer {
	return newProducer(appID, exchange, queue, bind, logger)
}

func newProducer(appID string, exchange *ExchangeConfig, queue *QueueConfig, bind *BindConfig, logger *slog.Logger) *producer {
	if logger == nil {
		panic("amqp: logger is required")
	}
	if exchange == nil && queue == nil {
		panic("amqp: exchange or queue config is required")
	}
	return &producer{
		appID: appID, exchange: exchange, queue: queue, bind: bind, logger: logger,
		closeDone: make(chan struct{}),
	}
}

type producer struct {
	appID    string
	exchange *ExchangeConfig
	queue    *QueueConfig
	bind     *BindConfig
	logger   *slog.Logger

	mu        sync.RWMutex
	channel   *rabbit.Channel
	reconnect func()

	lifecycleMu sync.Mutex
	closed      bool
	inFlight    sync.WaitGroup
	closeOnce   sync.Once
	closeDone   chan struct{}
	closeErr    error
}

func (p *producer) Connect(conn *rabbit.Connection) (err error) {
	p.lifecycleMu.Lock()
	if p.closed {
		p.lifecycleMu.Unlock()
		return ErrClosed
	}
	p.inFlight.Add(1)
	p.lifecycleMu.Unlock()
	defer p.inFlight.Done()
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = channel.Close()
		}
	}()
	if p.exchange != nil {
		if err = declareExchange(channel, *p.exchange); err != nil {
			return fmt.Errorf("declare exchange: %w", err)
		}
	}
	if p.queue != nil {
		if p.queue.DLQ != nil {
			if err = declareDLQ(channel, *p.queue.DLQ); err != nil {
				return err
			}
		}
		if err = declareQueue(channel, *p.queue); err != nil {
			return fmt.Errorf("declare queue: %w", err)
		}
	}
	if p.bind != nil {
		if err = declareBind(channel, *p.bind); err != nil {
			return fmt.Errorf("bind queue: %w", err)
		}
	}
	if err = channel.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms: %w", err)
	}

	p.mu.Lock()
	old := p.channel
	p.channel = channel
	p.mu.Unlock()
	if old != nil && !old.IsClosed() {
		_ = old.Close()
	}
	p.watch(channel)
	return nil
}

func (p *producer) Publish(ctx context.Context, delivery Delivery) error {
	if ctx == nil {
		panic("amqp: context is required")
	}
	p.lifecycleMu.Lock()
	if p.closed {
		p.lifecycleMu.Unlock()
		return ErrClosed
	}
	p.inFlight.Add(1)
	p.lifecycleMu.Unlock()
	defer p.inFlight.Done()

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.channel == nil || p.channel.IsClosed() {
		return ErrChannelUnavailable
	}
	exchange := ""
	routingKey := delivery.RoutingKey
	if p.exchange != nil {
		exchange = p.exchange.Name
	}
	if exchange == "" && routingKey == "" && p.queue != nil {
		routingKey = p.queue.Name
	}
	confirmation, err := p.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, true, false, rabbit.Publishing{
		Headers: delivery.Headers, ContentType: delivery.ContentType, DeliveryMode: rabbit.Persistent,
		CorrelationId: delivery.CorrelationID, MessageId: delivery.MessageID, Timestamp: time.Now(),
		Type: delivery.Type, AppId: p.appID, Body: delivery.Body,
	})
	if err != nil {
		return fmt.Errorf("publish AMQP message: %w", err)
	}
	if confirmation == nil {
		return errors.New("amqp: publisher confirm was not registered")
	}
	confirmed, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait for AMQP publish confirmation: %w", err)
	}
	if !confirmed {
		return ErrPublishNacked
	}
	return nil
}

func (p *producer) setReconnect(reconnect func()) { p.reconnect = reconnect }

func (p *producer) watch(channel *rabbit.Channel) {
	go func() {
		<-channel.NotifyClose(make(chan *rabbit.Error, 1))
		p.mu.RLock()
		current := p.channel == channel
		p.mu.RUnlock()
		p.lifecycleMu.Lock()
		closed := p.closed
		p.lifecycleMu.Unlock()
		if current && !closed && p.reconnect != nil {
			p.reconnect()
		}
	}()
}

func (p *producer) Close(ctx context.Context) error {
	if ctx == nil {
		panic("amqp: context is required")
	}
	p.closeOnce.Do(func() {
		p.lifecycleMu.Lock()
		p.closed = true
		p.lifecycleMu.Unlock()
		go func() {
			p.inFlight.Wait()
			p.mu.Lock()
			if p.channel != nil && !p.channel.IsClosed() {
				p.closeErr = p.channel.Close()
			}
			p.mu.Unlock()
			close(p.closeDone)
		}()
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closeDone:
		return p.closeErr
	}
}

func (p *producer) shutdown(ctx context.Context) error { return p.Close(ctx) }
