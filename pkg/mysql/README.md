# mysql

`mysql` содержит базовые абстракции для работы с MySQL: подключение,
транзакционные клиенты и соединения, переиспользуемый пул соединений и
advisory locks.

```go
connector := mysql.NewConnector()
err := connector.Open(dsn, mysql.Config{
    MaxConnections:        10,
    ConnectionMaxLifeTime: 30 * time.Minute,
    ConnectionMaxIdleTime: 5 * time.Minute,
})
if err != nil {
    return err
}
defer connector.Close()

client := connector.TransactionalClient()
var name string
err = client.GetContext(ctx, "users.name-by-id", &name,
    "SELECT name FROM users WHERE id = ?", userID)
```

`queryID` перед SQL-запросом добавляется в возвращаемую ошибку и помогает
найти место вызова. Для операций, которым нужно закреплённое соединение,
используйте `NewConnectionPool`; `NewLocker` выполняет callback под MySQL
advisory lock.

## Проверка

Интеграционные тесты запускают `mysql:8.4` через testcontainers, поэтому нужен
Docker:

```sh
mise run //pkg/mysql:all
```

Полная последовательная проверка монорепозитория: `mise run all`.
