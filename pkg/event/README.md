# event

Пакет описывает транспортно-независимые события и предоставляет AMQP-транспорт,
а также опциональные transactional outbox и inbox для MySQL.

Событие — уже произошедший факт. Ошибка декодирования или обработки приводит к
AMQP `Reject`, а не к бесконечному `Nack`/requeue. Для rejected-сообщений следует
настроить DLQ средствами `pkg/amqp`.

## Контракт события

Payload и сам envelope являются protobuf-сообщениями. Payload упаковывается в
`google.protobuf.Any`, а весь envelope отправляется в бинарном protobuf-формате.
Envelope содержит ID события, producer, тип, время возникновения и correlation
ID. Event ID и correlation ID создаются только пакетом `event`.
Событие, созданное из context обработчика, автоматически наследует correlation
ID обрабатываемого события.

```go
envelope, err := event.New(
	ctx,
	"users",
	"user-created",
	&events.UserCreated{UserId: userID},
)
```

Handler распаковывает ожидаемый protobuf payload без registry или codec:

```go
var created events.UserCreated
err := envelope.UnmarshalPayload(&created)
```

Схема envelope находится в `internal/eventpb/envelope.proto`. Generated Go-код
обновляется задачей `generate`, которая использует закреплённые в корневом
`mise.toml` версии `protoc` и `protoc-gen-go`.

## AMQP и маршрутизация

Routing key имеет формат `<producer>.<event-type>`. Используется обычная
topic-семантика RabbitMQ, поэтому клиент передаёт нужные binding keys напрямую:

```text
#                     все события
users.#               все события сервиса users
users.user-created    конкретное событие users
users.#, billing.#    события нескольких сервисов
```

```go
exchange := &amqp.ExchangeConfig{
	Name:    "events",
	Kind:    rabbit.ExchangeTopic,
	Durable: true,
}
producer := connection.Producer(exchange, nil, nil)
dispatcher, err := eventamqp.NewDispatcher(producer)

handler, err := eventamqp.NewHandler(applicationHandler)
connection.Consumer(
	ctx,
	handler,
	exchange,
	queue,
	&amqp.BindConfig{
		QueueName:    queue.Name,
		ExchangeName: exchange.Name,
		RoutingKeys:  []string{"users.#", "billing.invoice-paid"},
	},
	&amqp.QoSConfig{PrefetchCount: 100},
)
```

AMQP handler добавляет transport metadata. Если RabbitMQ передал
`x-delivery-count` или `x-death`, logging middleware записывает `retry_count` и
его источник. Когда точный счётчик отсутствует, логируется только `redelivered`.

## Logging middleware

```go
dispatcher = event.ChainDispatcher(
	dispatcher,
	event.DispatcherLogging(logger),
)
applicationHandler = event.ChainHandler(
	applicationHandler,
	event.HandlerLogging(logger),
)
```

Middleware пишет одну итоговую запись с event metadata, результатом и
длительностью. Payload по умолчанию не логируется.

## Опциональный outbox

Outbox декорирует dispatcher. Вызов `Dispatch` сохраняет envelope в текущей
транзакции `uow`; публикация выполняется отдельно через `Relay`.

```go
dispatcher, relay, err := outbox.Decorate(
	amqpDispatcher,
	lockableUnitOfWork,
	outbox.Config{Transport: "rabbitmq"},
)

processed, err := relay.ProcessBatch(ctx)
deleted, err := relay.Cleanup(ctx, cutoff, 1000)
```

`ProcessBatch` и `Cleanup` предназначены для вызова внешними jobs. Гарантия
доставки — at-least-once, поэтому consumers должны быть идемпотентными.

## Опциональный inbox

Inbox декорирует handler и не вызывает его повторно для уже обработанного
`(consumer, event_id)`. Marker и изменения handler-а фиксируются одной
транзакцией, если handler использует тот же context-aware `UnitOfWork`.

```go
handler, inboxStore, err := inbox.Decorate(
	applicationHandler,
	unitOfWork,
	inbox.Config{Consumer: "billing-events"},
)

deleted, err := inboxStore.Cleanup(ctx, cutoff, 1000)
```

## Миграции

Outbox и inbox подключаются независимо:

```go
err := migrations.Migrate(ctx, "event-outbox", outbox.Migrations(client)...)
err := migrations.Migrate(ctx, "event-inbox", inbox.Migrations(client)...)
```

Если декоратор не используется, его миграции регистрировать не нужно.

## Проверка

Интеграционные тесты запускают MySQL 8.4 и RabbitMQ 4.1 через testcontainers.

```sh
mise run //pkg/event:all
```
