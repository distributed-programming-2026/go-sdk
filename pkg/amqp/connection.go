package amqp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/logging"
	rabbit "github.com/rabbitmq/amqp091-go"
)

var ErrClosed = errors.New("amqp: connection is closed")

type Connection interface {
	Start() error
	Stop() error
	AddChannel(Channel)
	Producer(exchange *ExchangeConfig, queue *QueueConfig, bind *BindConfig) Producer
	Consumer(
		ctx context.Context,
		handler Handler,
		exchange *ExchangeConfig,
		queue *QueueConfig,
		bind *BindConfig,
		qos *QoSConfig,
	) Consumer
}

type Channel interface {
	Connect(*rabbit.Connection) error
}

type managedChannel interface {
	Channel
	setReconnect(func())
	shutdown(context.Context) error
}

func NewConnection(appID string, config ConnectionConfig, logger *slog.Logger) Connection {
	if logger == nil {
		panic("amqp: logger is required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &connection{appID: appID, config: config, logger: logger.With(logging.Target("amqp")), ctx: ctx, cancel: cancel}
}

// NewAMQPConnection retains the constructor spelling used by rp-golib.
func NewAMQPConnection(appID string, config *ConnectionConfig, logger *slog.Logger) Connection {
	if config == nil {
		panic("amqp: connection config is required")
	}
	return NewConnection(appID, *config, logger)
}

type connection struct {
	appID  string
	config ConnectionConfig
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc

	mu          sync.RWMutex
	conn        *rabbit.Connection
	channels    []managedChannel
	started     bool
	stopped     bool
	reconnectMu sync.Mutex
	wg          sync.WaitGroup
}

func (c *connection) Start() error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return ErrClosed
	}
	if c.started && c.conn != nil && !c.conn.IsClosed() {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	conn, err := c.dial(c.ctx, c.config.ConnectTimeout)
	if err != nil {
		return err
	}
	if err := c.install(conn); err != nil {
		_ = conn.Close()
		return err
	}
	c.logger.Info("AMQP connection established")
	return nil
}

func (c *connection) Stop() error {
	c.cancel()
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true
	conn := c.conn
	channels := append([]managedChannel(nil), c.channels...)
	c.mu.Unlock()
	var errs []error
	for _, channel := range channels {
		if err := channel.shutdown(context.Background()); err != nil && !errors.Is(err, rabbit.ErrClosed) {
			errs = append(errs, err)
		}
	}
	if conn != nil {
		if err := conn.Close(); err != nil && !errors.Is(err, rabbit.ErrClosed) {
			errs = append(errs, err)
		}
	}
	c.wg.Wait()
	return errors.Join(errs...)
}

func (c *connection) AddChannel(channel Channel) {
	managed, ok := channel.(managedChannel)
	if !ok {
		panic("amqp: channel was not created by this package")
	}
	managed.setReconnect(func() { c.reconnectChannel(managed) })
	c.mu.Lock()
	c.channels = append(c.channels, managed)
	conn := c.conn
	started := c.started
	c.mu.Unlock()
	if started && conn != nil && !conn.IsClosed() {
		c.wg.Add(1)
		go func() { defer c.wg.Done(); c.reconnectChannel(managed) }()
	}
}

func (c *connection) Producer(exchange *ExchangeConfig, queue *QueueConfig, bind *BindConfig) Producer {
	p := newProducer(c.appID, exchange, queue, bind, c.logger)
	c.AddChannel(p)
	return p
}

func (c *connection) Consumer(
	ctx context.Context,
	handler Handler,
	exchange *ExchangeConfig,
	queue *QueueConfig,
	bind *BindConfig,
	qos *QoSConfig,
) Consumer {
	consumer := newConsumer(ctx, handler, exchange, queue, bind, qos, c.logger)
	c.AddChannel(consumer)
	return consumer
}

func (c *connection) install(conn *rabbit.Connection) error {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	c.mu.RLock()
	channels := append([]managedChannel(nil), c.channels...)
	c.mu.RUnlock()
	for _, channel := range channels {
		if err := channel.Connect(conn); err != nil {
			return fmt.Errorf("connect AMQP channel: %w", err)
		}
	}
	c.mu.Lock()
	c.conn = conn
	c.started = true
	c.mu.Unlock()
	c.watchConnection(conn)
	return nil
}

func (c *connection) watchConnection(conn *rabbit.Connection) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		err := <-conn.NotifyClose(make(chan *rabbit.Error, 1))
		if c.ctx.Err() != nil {
			return
		}
		if err != nil {
			c.logger.Error("AMQP connection lost", logging.Error(err))
		} else {
			c.logger.Warn("AMQP connection closed")
		}
		c.reconnectConnection(conn)
	}()
}

func (c *connection) reconnectConnection(lost *rabbit.Connection) {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	c.mu.RLock()
	if c.conn != lost || c.stopped {
		c.mu.RUnlock()
		return
	}
	c.mu.RUnlock()
	for c.ctx.Err() == nil {
		conn, err := c.dialOnce()
		if err == nil {
			c.mu.RLock()
			channels := append([]managedChannel(nil), c.channels...)
			c.mu.RUnlock()
			for _, channel := range channels {
				if err = channel.Connect(conn); err != nil {
					break
				}
			}
			if err == nil {
				c.mu.Lock()
				c.conn = conn
				c.mu.Unlock()
				c.watchConnection(conn)
				c.logger.Info("AMQP connection restored")
				return
			}
			_ = conn.Close()
		}
		c.logger.Error("failed to reconnect to AMQP", logging.Error(err))
		if !wait(c.ctx, c.config.reconnectDelay()) {
			return
		}
	}
}

func (c *connection) reconnectChannel(channel managedChannel) {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	for c.ctx.Err() == nil {
		c.mu.RLock()
		conn := c.conn
		stopped := c.stopped
		c.mu.RUnlock()
		if stopped || conn == nil || conn.IsClosed() {
			return
		}
		err := channel.Connect(conn)
		if err == nil {
			c.logger.Info("AMQP channel restored")
			return
		}
		c.logger.Error("failed to reconnect AMQP channel", logging.Error(err))
		if !wait(c.ctx, c.config.reconnectDelay()) {
			return
		}
	}
}

func (c *connection) dial(ctx context.Context, timeout time.Duration) (*rabbit.Connection, error) {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	var last error
	for {
		conn, err := c.dialOnce()
		if err == nil {
			return conn, nil
		}
		last = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("connect to AMQP: %w", last)
		case <-time.After(c.config.reconnectDelay()):
		}
	}
}

func (c *connection) dialOnce() (*rabbit.Connection, error) {
	return rabbit.DialConfig(c.config.connectionURL(), rabbit.Config{Dial: rabbit.DefaultDial(c.config.ConnectTimeout)})
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
