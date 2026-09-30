# logging

Пакет создаёт настроенный `*slog.Logger`, который пишет JSON и добавляет в
каждую запись `app_id`, `@timestamp` (Unix milliseconds) и `time` (RFC3339Nano).

```go
logger := logging.New(logging.Config{
    AppID: "orders-api",
    Debug: true,
})

started := time.Now()
err := doWork()
logger.Error("request failed",
    logging.Error(err),
    logging.DurationNs(time.Since(started)),
    logging.Duration(time.Since(started)),
)
```

`logging.Error` раскрывает ошибки из `errors.Join` и добавляет поле `stack`,
если хотя бы одна из ошибок содержит stack trace. По умолчанию debug-записи
отключены, а вывод направлен в `os.Stderr`.
