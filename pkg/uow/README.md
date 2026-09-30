# uow

`uow` объединяет операции MySQL в unit of work. Вложенные вызовы с одним
`context.Context` используют одну транзакцию. Транзакция коммитится после
завершения внешнего вызова; ошибка или panic в любом вложенном вызове
переводит её в rollback-only.

`NewUnitOfWork` может передавать callback как `mysql.ClientContext`, так и
пользовательский provider репозиториев. `NewLockableUnitOfWork` дополнительно
берёт набор advisory locks из `pkg/mysql`. Блокировки берутся в
лексикографическом порядке и освобождаются после commit/rollback внешней
транзакции, включая блокировки, взятые вложенным unit of work.

```go
pool := mysql.NewConnectionPool(connector.TransactionalClient())
unit := uow.NewUnitOfWork(pool, func(client mysql.ClientContext) *Repositories {
    return &Repositories{Users: NewUserRepository(client)}
})

err := unit.ExecuteWithRepositoryProvider(ctx, func(repos *Repositories) error {
    return repos.Users.Save(ctx, user)
})
```

Если provider не нужен, передайте builder, возвращающий пустую структуру, и
используйте `ExecuteWithClientContext`.

## Проверка

Интеграционные тесты запускают `mysql:8.4` через testcontainers, поэтому нужен
Docker:

```sh
mise run //pkg/uow:all
```

Полная последовательная проверка монорепозитория: `mise run all`.
