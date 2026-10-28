[tasks.all]
run = [
    { task = "generate" },
    { task = "modules" },
    { task = "build" },
    { task = "test" },
    { task = "lint" },
]

[tasks.generate]
run = "go generate ./..."

[tasks."modules"]
run = "go mod tidy"

[tasks.build]
run = "go build ./..."

[tasks."test"]
run = "go test ./..."

[tasks."lint"]
run = "golangci-lint run"
