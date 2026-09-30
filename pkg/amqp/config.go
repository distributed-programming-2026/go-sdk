package amqp

import (
	"fmt"
	"net/url"
	"time"

	rabbit "github.com/rabbitmq/amqp091-go"
)

// ConnectionConfig describes a RabbitMQ connection. URL takes precedence over
// the individual fields when it is set.
type ConnectionConfig struct {
	URL              string
	User             string
	Password         string
	Host             string
	VirtualHost      string
	ConnectTimeout   time.Duration
	ReconnectBackoff time.Duration
}

func (c ConnectionConfig) connectionURL() string {
	if c.URL != "" {
		return c.URL
	}
	host := c.Host
	if host == "" {
		host = "localhost:5672"
	}
	vhost := c.VirtualHost
	if vhost == "" {
		vhost = "/"
	}
	u := &url.URL{Scheme: "amqp", Host: host, Path: vhost}
	u.User = url.UserPassword(c.User, c.Password)
	return u.String()
}

func (c ConnectionConfig) reconnectDelay() time.Duration {
	if c.ReconnectBackoff > 0 {
		return c.ReconnectBackoff
	}
	return time.Second
}

type ExchangeConfig struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       rabbit.Table
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       rabbit.Table
	DLQ        *DLQConfig
}

// DLQConfig declares the dead-letter topology and configures the source queue
// to dead-letter rejected messages into it.
type DLQConfig struct {
	Exchange   ExchangeConfig
	Queue      string
	RoutingKey string
	Durable    bool
	Args       rabbit.Table
}

type BindConfig struct {
	QueueName    string
	ExchangeName string
	RoutingKeys  []string
	NoWait       bool
	Args         rabbit.Table
}

type QoSConfig struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

func declareExchange(ch *rabbit.Channel, config ExchangeConfig) error {
	kind := config.Kind
	if kind == "" {
		kind = rabbit.ExchangeDirect
	}
	return ch.ExchangeDeclare(config.Name, kind, config.Durable, config.AutoDelete, config.Internal, config.NoWait, config.Args)
}

func declareQueue(ch *rabbit.Channel, config QueueConfig) error {
	args := cloneTable(config.Args)
	if config.DLQ != nil {
		if args == nil {
			args = rabbit.Table{}
		}
		args["x-dead-letter-exchange"] = config.DLQ.Exchange.Name
		if config.DLQ.RoutingKey != "" {
			args["x-dead-letter-routing-key"] = config.DLQ.RoutingKey
		}
	}
	_, err := ch.QueueDeclare(config.Name, config.Durable, config.AutoDelete, config.Exclusive, config.NoWait, args)
	return err
}

func declareDLQ(ch *rabbit.Channel, config DLQConfig) error {
	if config.Queue == "" {
		return fmt.Errorf("amqp: DLQ name is required")
	}
	if err := declareExchange(ch, config.Exchange); err != nil {
		return fmt.Errorf("declare dead-letter exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(config.Queue, config.Durable, false, false, false, config.Args); err != nil {
		return fmt.Errorf("declare dead-letter queue: %w", err)
	}
	routingKey := config.RoutingKey
	if routingKey == "" {
		routingKey = config.Queue
	}
	if err := ch.QueueBind(config.Queue, routingKey, config.Exchange.Name, false, nil); err != nil {
		return fmt.Errorf("bind dead-letter queue: %w", err)
	}
	return nil
}

func declareBind(ch *rabbit.Channel, config BindConfig) error {
	for _, key := range config.RoutingKeys {
		if err := ch.QueueBind(config.QueueName, key, config.ExchangeName, config.NoWait, config.Args); err != nil {
			return err
		}
	}
	return nil
}

func cloneTable(table rabbit.Table) rabbit.Table {
	if table == nil {
		return nil
	}
	cloned := make(rabbit.Table, len(table))
	for key, value := range table {
		cloned[key] = value
	}
	return cloned
}
