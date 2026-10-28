# integrationtests

Общее окружение интеграционных тестов Go-пакета: контейнеры и Docker-сеть,
подготовка образов, зависимости, ручные jobs, таймауты и диагностика.
Основа работает с `testing`; адаптер testify suite подключается отдельно.

## Обычные тесты

```go
var deps struct { DB *sql.DB }

var env = integrationtests.New(integrationtests.Config{
    Containers: []integrationtests.ContainerSpec{{
        Name: "mysql",
        Request: testcontainers.ContainerRequest{
            Image: "mysql:8.4",
            ExposedPorts: []string{"3306/tcp"},
            Env: map[string]string{"MYSQL_ROOT_PASSWORD": "test"},
            WaitingFor: wait.ForLog("port: 3306  MySQL Community Server"),
        },
        Logs: integrationtests.OnFailure, // значение по умолчанию
    }},
    Setup: func(ctx context.Context, env *integrationtests.Environment) error {
        endpoint, err := env.HostEndpoint(ctx, "mysql", "3306/tcp")
        if err != nil { return err }
        db, err := sql.Open("mysql", "root:test@tcp("+endpoint+")/mysql?parseTime=true")
        if err != nil { return err }
        // Регистрируем очистку сразу после выделения ресурса, до следующей ошибки.
        if err := env.AddCleanup("mysql client", func(context.Context) error {
            return db.Close()
        }); err != nil { return err }
        if err := db.PingContext(ctx); err != nil { return err }
        deps.DB = db
        return nil
    },
})

func TestMain(m *testing.M) {
    os.Exit(integrationtests.Run(m, env))
}

func TestQuery(t *testing.T) {
    ctx := env.Bind(t)
    var result int
    require.NoError(t, deps.DB.QueryRowContext(ctx, "SELECT 1").Scan(&result))
    require.Equal(t, 1, result)
}
```

Нужны импорты `integrationtests`, `testcontainers`, `testcontainers/wait`,
`context`, `database/sql`, `os`, `testing`, `testify/require` и драйвер MySQL.
В обычных вложенных `t.Run` и параллельных тестах вызывайте `env.Bind(t)`
для каждого `*testing.T`. Используйте возвращённый контекст для операций I/O:
он отменяется при завершении теста и при аварии окружения.

## Фазы и отмена

| Фаза | Таймаут по умолчанию | Действие |
|---|---|---|
| Prepare | 5 минут | Валидация графа, build/pull образов, включая jobs |
| Up | 1 минута | Сеть, сервисы по зависимостям, readiness, хуки, Setup |
| Tests | 5 минут | Весь `m.Run()` |
| Down | 1 минута | Клиенты, контейнеры в обратном порядке, сеть |

Переопределение: `Config{Timeouts: integrationtests.Timeouts{Tests: 10*time.Minute}}`.
Нулевые значения используют defaults, отрицательные запрещены. Ненулевой
`go test -timeout` должен быть больше `Tests + Down`, иначе runner отклонит запуск.
`-timeout=0` отключает штатный таймер Go, оставляя ограничения SDK.
SDK пишет начало/завершение каждой фазы и heartbeat через 10 секунд.
Runner устанавливает общий диагностический logger testcontainers до запуска
тестов; сообщения Docker/testcontainers буферизуются до ошибки прогона.

`RunContext(ctx, m, env)` также учитывает внешний контекст. SIGINT/SIGTERM,
таймаут, неожиданная остановка сервиса и `env.Abort(err)` отменяют окружение.
Активные привязанные тесты получают ошибку, новые привязанные тесты сразу падают.
Обычная ошибка assertion или job не отменяет остальные тесты.

Go не позволяет остановить чужую горутину. После аварии SDK запускает `Down`
с независимым контекстом и затем завершает процесс ненулевым кодом. Во время
очистки тесты могут ещё выполнять код. Клиенты должны учитывать отмену контекста.
Очистка продолжается после отдельных ошибок и ограничена своим таймаутом;
ошибки удаления выводятся с ID ресурсов. Недоступный Docker, SIGKILL и аварийный
panic самого Go-процесса не позволяют гарантировать очистку; Ryuk остаётся
дополнительной страховкой. Один экземпляр `Environment` нельзя перезапускать.

## Зависимости и адреса

`DependsOn: []string{"mysql"}` означает, что MySQL должен пройти readiness
и `AfterReady` до создания зависимого сервиса. Неизвестные зависимости, циклы,
повторные имена и зависимости от ручных jobs отклоняются до обращения к Docker.
Имена — логические имена и сетевые aliases; фиксированный `Request.Name` запрещён,
чтобы параллельные пакеты и повторные jobs не конфликтовали.

- `HostEndpoint(ctx, "mysql", "3306/tcp")` возвращает `Host()` с опубликованным
  динамическим портом, например `localhost:49153`. Используется Go-клиентами тестов.
- `NetworkEndpoint("mysql", "3306/tcp")` возвращает `mysql:3306` для контейнеров
  в общей сети. `host.docker.internal` автоматически не подставляется.
- `Configure(ctx, env, request)` вычисляет env, команду и другие настройки
  непосредственно перед созданием экземпляра, когда его зависимости готовы.
- `AfterReady(ctx, env, container)` инициализирует готовый сервис, например создаёт
  логические базы. Его собственный опубликованный адрес доступен через контейнер
  или `HostEndpoint`. Ошибка хука прерывает запуск окружения.
- `Config.Setup` создаёт общих клиентов после всех автоматических сервисов.
  Для клиента, нужного между запусками сервисов, используйте `AfterReady`.

## Переиспользование и jobs

Общие части описываются Go-функциями, без отдельного языка шаблонов:

```go
func appSpec(name string, manual bool, command ...string) integrationtests.ContainerSpec {
    return integrationtests.ContainerSpec{
        Name: name,
        Manual: manual,
        DependsOn: []string{"mysql"},
        Request: testcontainers.ContainerRequest{
            Image: "my-app:test",
            Cmd: command,
        },
        Configure: func(ctx context.Context, env *integrationtests.Environment,
            request *testcontainers.ContainerRequest) error {
            endpoint, err := env.NetworkEndpoint("mysql", "3306/tcp")
            if err != nil { return err }
            if request.Env == nil { request.Env = make(map[string]string) }
            request.Env["MYSQL_ADDRESS"] = endpoint
            return nil
        },
    }
}

// Регистрируются в Config.Containers:
// appSpec("api", false, "serve") — дополнительно задайте WaitingFor для API.
// appSpec("recalculate", true, "recalculate")

result, err := env.RunJob(ctx, "recalculate",
    integrationtests.WithCommand("recalculate", "--date=2026-10-28"),
    integrationtests.WithEnv(map[string]string{"BATCH_SIZE": "100"}),
)
// result.ID, result.ExitCode, result.Logs
```

Каждый вызов создаёт отдельный контейнер, ждёт завершения и удаляет его.
Ненулевой exit code возвращается как ошибка вместе с результатом. Переданный
контекст задаёт таймаут конкретной job, общая отмена окружения тоже учитывается.
Job options применяются после Configure. Ошибка запуска/очистки также возвращается
вызывающему тесту; она сама по себе не вызывает `Abort`.

Обычные map/slice настроек копируются для каждого экземпляра. Wait strategies,
readers, mount sources и замыкания — расширения testcontainers, которые SDK
не может произвольно клонировать: создавайте их в Configure, если они изменяемые
или используются конкурентными jobs. Image/FromDockerfile задаются в шаблоне,
менять образ в Configure/job options нельзя. Для сборки используйте стандартный
`Request.FromDockerfile`; подготовленные образы остаются Docker-кешем.
Локальный образ используется без pull, если не задан `AlwaysPullImage`;
при явно заданной платформе Prepare выполняет pull для этой платформы.

## Testify suite

```go
import integrationsuite "github.com/distributed-programming-2026/go-sdk/pkg/integrationtests/suite"

type APISuite struct { integrationsuite.Suite }

func TestAPI(t *testing.T) {
    suite.Run(t, &APISuite{Suite: integrationsuite.Suite{Env: env}})
}

func (s *APISuite) TestRequest() {
    ctx := s.Context()
    // Запрос с ctx; логирование и регистрация уже подключены.
    _ = ctx
    s.Run("nested", func() { _ = s.Context() })
}
```

Адаптер не занимает SetupTest/TearDownTest: используется SetT и собственный Run.
При переопределении SetT/Run нужно делегировать базовому типу. Обычные suite hooks
сохраняются. Как и testify suite, адаптер не поддерживает `t.Parallel()`.

## Логи

`Logs: OnFailure` включает сбор stdout/stderr и диагностики testcontainers.
`Logs: Disabled` отключает сбор SDK. `LogBufferBytes` у контейнера переопределяет
общий `Config.LogBufferBytes`; по умолчанию сохраняется хвост 1 MiB на экземпляр.
Переполнение помечается в выводе. Пользовательские log consumers работают
независимо от политики SDK.

При падении привязанного теста через `t.Logf` выводятся доступные сообщения
за время его регистрации. При аварии запуска/прогона или падении непривязанных
тестов runner выводит буферы на уровне пакета. Для общих контейнеров интервалы
параллельных тестов могут пересекаться; точная принадлежность сообщений требует
correlation ID приложения.

## Проверка

```sh
mise run //pkg/integrationtests:all
go test -race ./...
INTEGRATIONTESTS_DOCKER=1 go test -race -count=1 ./...
```

Последняя команда дополнительно проверяет реальный MySQL, динамические порты,
сеть и повторные jobs. Остальные проверки используют fake runtime и дочерние
процессы для аварийного завершения, без Docker. Проверка всего монорепозитория:
`mise run all`.

## HTTP/JSON и gRPC мок-серверы

Моки — настоящие сетевые серверы в процессе тестов. Приложения в контейнерах
отправляют им обычные HTTP/gRPC-запросы. HTTP настраивается стандартным
`http.Handler`/`http.ServeMux`, gRPC — Go-реализациями сгенерированных protobuf
интерфейсов. Отдельного языка ожиданий нет: состояние сценария, проверки аргументов
и число вызовов остаются обычным Go-кодом (в том числе с testify/mock).

```go
import (
    "github.com/distributed-programming-2026/go-sdk/pkg/integrationtests/mockhttp"
    "github.com/distributed-programming-2026/go-sdk/pkg/integrationtests/mockgrpc"
)

var chargeCalls atomic.Int32

billing := mockhttp.New("billing", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost || r.URL.Path != "/charge" {
        http.Error(w, "unexpected request", http.StatusNotFound)
        return
    }
    chargeCalls.Add(1)
    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(map[string]string{"payment_id": "test-42"})
}))

catalog := mockgrpc.New("catalog", func(server *grpc.Server) {
    // catalogMock реализует сгенерированный protobuf-интерфейс сервиса.
    catalogpb.RegisterCatalogServer(server, &catalogMock{})
})

env := integrationtests.New(integrationtests.Config{
    Servers: []integrationtests.ServerSpec{billing, catalog},
    Containers: []integrationtests.ContainerSpec{{
        Name: "api",
        DependsOn: []string{"billing", "catalog"},
        Request: testcontainers.ContainerRequest{
            Image: "my-service:test",
            WaitingFor: wait.ForHTTP("/health").WithPort("8080/tcp"),
            ExposedPorts: []string{"8080/tcp"},
        },
        Configure: func(ctx context.Context, env *integrationtests.Environment,
            request *testcontainers.ContainerRequest) error {
            billingAddress, err := env.NetworkEndpoint("billing")
            if err != nil { return err }
            catalogAddress, err := env.NetworkEndpoint("catalog")
            if err != nil { return err }
            request.Env = map[string]string{
                "BILLING_URL": "http://" + billingAddress,
                "CATALOG_GRPC_ADDRESS": catalogAddress,
            }
            return nil
        },
    }},
})
```

После вызова реального API проверяйте ожидания в тесте:
`require.EqualValues(t, 1, chargeCalls.Load())`. Не вызывайте `require`/`FailNow`
в горутине обработчика: возвращайте ошибки протокола и проверяйте результаты
в тесте. Для изменения ответов между сценариями используйте потокобезопасное
состояние (mutex/atomic), отдельные экземпляры моков или сгенерированные mocks.
Общее состояние и ожидания не сбрасываются автоматически между тестами.

`HostEndpoint(ctx, "billing")` без порта возвращает адрес мока для Go-клиента;
HTTP-клиент добавляет `http://`, gRPC-клиент использует адрес напрямую.
`NetworkEndpoint("billing")` возвращает адрес общего шлюза для контейнеров.
Контейнеры и jobs обязаны указать нужные моки в `DependsOn` — SDK добавит их
сетевую конфигурацию. Порты выбираются динамически; фиксировать адрес хоста не нужно.

Серверы запускаются в Up до приложений. Если они нужны контейнерам, SDK заранее
поднимает один общий шлюз на `alpine:3.22` с `HostAccessPorts` testcontainers
(`testcontainers/sshd:1.4.0` для закреплённой версии testcontainers-go).
Образы шлюза входят в Prepare. Туннели готовы до старта приложений, поэтому
поддерживаются запросы к мокам при запуске и из readiness-проверок.
В окружениях только с Go-клиентами Docker для моков не требуется.

`Down` сначала закрывает клиентов и контейнеры, затем мок-серверы. Аварийный
выход сервера или шлюза отменяет окружение. HTTP Shutdown закрывает соединения
при превышении дедлайна; gRPC сначала выполняет GracefulStop, затем Stop.
Unary и streaming RPC поддерживаются; credentials, interceptors и другие
параметры gRPC можно передать дополнительными `grpc.ServerOption` в `mockgrpc.New`.

Логи HTTP (метод, URI, статус, длительность) и gRPC (метод, код, длительность)
входят в те же ограниченные буферы и диагностику упавшего теста. Тела запросов,
ответов и metadata автоматически не выводятся. На ServerSpec действуют
`Logs: Disabled` и `LogBufferBytes`, как на контейнерах.

Для другого протокола задайте `ServerSpec.New`, возвращающий реализацию
`Server` с методами `Serve(net.Listener) error` и `Shutdown(context.Context) error`.
SDK управляет listener, случайным портом, адресами, отменой и очисткой;
`New` получает startup-контекст и буферизованный ServerLogger. Startup-контекст
заканчивается после Up, поэтому его нельзя использовать как контекст запросов.
