# amqp

Небольшая инфраструктурная обёртка над `amqp091-go` для publisher/consumer
с автоматическим восстановлением соединений и каналов.

Возможности:

- публикация через exchange или напрямую в очередь через default exchange;
- publisher confirms: `Publish` завершается только после broker ack;
- ручное подтверждение consumer-сообщений через `Ack`, `Nack` и `Reject`;
- panic при возврате handler-ом неизвестного `Disposition`, поскольку это
  ошибка программирования;
- `Nack` возвращает сообщение в очередь, повторная доставка имеет
  `Delivery.Redelivered == true`;
- `Reject` не возвращает сообщение и направляет его в настроенную DLQ;
- повторное объявление topology и восстановление producer/consumer после
  потери AMQP-соединения или канала;
- graceful `Close(ctx)` для producer и consumer: producer ожидает publisher
  confirms уже отправленных сообщений, consumer прекращает новые доставки и
  ожидает завершения текущего handler-а;
- структурированные логи с `logging.Target("amqp")`.

`context.Context` и `*slog.Logger` являются обязательными зависимостями.
Передача `nil` считается ошибкой программирования и вызывает panic.

## Прямая очередь

```go
conn := amqp.NewConnection("orders", amqp.ConnectionConfig{
    URL: "amqp://guest:guest@localhost:5672/",
}, logger)

queue := &amqp.QueueConfig{Name: "commands", Durable: true}
producer := conn.Producer(nil, queue, nil)
conn.Consumer(ctx, func(ctx context.Context, message amqp.Delivery) amqp.Disposition {
    if err := handle(ctx, message.Body); err != nil {
        return amqp.Nack // requeue; следующая доставка будет Redelivered
    }
    return amqp.Ack
}, queue, nil, &amqp.QoSConfig{PrefetchCount: 10})

if err := conn.Start(); err != nil { /* handle */ }
defer conn.Stop()

err := producer.Publish(ctx, amqp.Delivery{Body: payload})
```

## Exchange и DLQ

```go
exchange := &amqp.ExchangeConfig{Name: "events", Kind: "topic", Durable: true}
queue := &amqp.QueueConfig{
    Name: "billing.events",
    Durable: true,
    DLQ: &amqp.DLQConfig{
        Exchange: amqp.ExchangeConfig{Name: "events.dlx", Kind: "direct", Durable: true},
        Queue: "billing.events.dlq",
        RoutingKey: "billing.dead",
        Durable: true,
    },
}
bind := &amqp.BindConfig{
    QueueName: queue.Name, ExchangeName: exchange.Name,
    RoutingKeys: []string{"order.*"},
}
```

Возврат `amqp.Reject` из handler-а публикует исходное сообщение в эту DLQ.
Handler вызывается последовательно для одного consumer-а; горизонтальный
параллелизм достигается несколькими consumer-ами.

Интеграционные тесты требуют Docker и запускают `rabbitmq:4.1-alpine`:

```sh
mise run //pkg/amqp:all
```

Полная последовательная проверка монорепозитория: `mise run all`.
