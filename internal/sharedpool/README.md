# sharedpool

`sharedpool` — потокобезопасный пул разделяемых ресурсов с подсчётом ссылок.
Для каждого ключа модуль создаёт один ресурс и переиспользует его между
несколькими потребителями.

```go
pool := sharedpool.NewPool(func(key string) (io.Closer, error) {
    return openResource(key)
})

lease, err := pool.Get("primary")
if err != nil {
    return err
}
defer lease.Close()

resource := lease.Value()
```

`Close` у lease идемпотентен. Ресурс закрывается только после освобождения
последнего lease с тем же ключом. Это внутренний модуль: импортировать его
могут только модули внутри `github.com/distributed-programming-2026/go-sdk`.

## Проверка

```sh
mise run //internal/sharedpool:all
```

Полная последовательная проверка монорепозитория: `mise run all`.
