# go-sdk

Монорепозиторий переиспользуемых Go-модулей. Каждый каталог в `pkg/` и
`internal/` — отдельный модуль, объединённый локально через `go.work`.

## Требования

- [mise](https://mise.jdx.dev/);
- Docker для интеграционных тестов;

Версии Go, golangci-lint и генераторов задаются в `mise.toml` и устанавливаются
командой:

```sh
mise install
```

## Сборка и проверка

Полная проверка генерирует код, обновляет метаданные Go-модулей, запускает
тесты и линтер во всех модулях. Модули обрабатываются последовательно, чтобы
интеграционные тесты не конкурировали за ресурсы Docker:

```sh
mise run all
```

Эквивалентные команды для отдельных групп и модулей:

```sh
MISE_JOBS=1 mise run //pkg/...:all
MISE_JOBS=1 mise run //internal/...:all
mise run //pkg/logging:all
```

Задачу можно запускать из любого каталога внутри репозитория. `all` включает
`generate`, `modules`, `build`, `test` и `lint`; отдельный этап запускается аналогично,
например `mise run //pkg/mysql:test`.

## Модули

- [`pkg/logging`](pkg/logging) — JSON-логирование поверх `log/slog`;
- [`pkg/mysql`](pkg/mysql) — MySQL-клиент, соединения и advisory locks;
- [`pkg/uow`](pkg/uow) — вложенный unit of work и транзакционные блокировки;
- [`pkg/migrator`](pkg/migrator) — forward-only миграции MySQL;
- [`pkg/amqp`](pkg/amqp) — producer/consumer RabbitMQ с восстановлением;
- [`internal/sharedpool`](internal/sharedpool) — внутренний пул ресурсов с
  подсчётом ссылок.
