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
