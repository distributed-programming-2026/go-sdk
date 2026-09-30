# migrator

`migrator` запускает forward-only миграции под MySQL. История хранится в
таблице с полями `target`, `version`, `description`, `applied_at` и составным
первичным ключом `(target, version)`. Поэтому `mysql` и `application` могут
использовать одну таблицу, сохраняя независимый порядок версий.

```go
client := mysql.NewTransactionalClientFromSQLx(db)
m, err := migrator.New(client, logger)
if err != nil {
	return err
}
err = m.Migrate(ctx, "application", createUsers, backfillUsers)
```

Миграции сортируются по версии. Уже применённые версии пропускаются, а
неприменённая версия меньше последней применённой возвращает `ErrOutOfOrder`.
Параллельные запуски одного target сериализуются advisory lock из `pkg/mysql`.

Отдельный физический реестр настраивается через
`WithTableName("mysql_migrations")`. По умолчанию используется таблица
`migrations`; время ожидания lock настраивается через `WithLockTimeout`.

Panic из `Migration.Up` не преобразуется в error: после освобождения lock и
соединения распространяется исходное значение panic. `Up` должен быть
идемпотентным, поскольку MySQL выполняет implicit commit для многих DDL-команд.

Интеграционные тесты запускают `mysql:8.4` через testcontainers:

```sh
mise run //pkg/migrator:all
```

Полная последовательная проверка монорепозитория: `mise run all`.
