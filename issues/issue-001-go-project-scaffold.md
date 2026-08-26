# [US-001] 建立 Go 项目骨架与工程规范

## Description

从空仓库建立可独立编译和测试的 Go 服务骨架，为 API、worker、模拟器及内部组件提供稳定的代码边界和统一工程规范。

## Acceptance Criteria

- [x] 创建 Go 1.22 或更高版本的单一 Go module。
- [x] 创建 `cmd/api`、`cmd/worker` 和 `cmd/sim` 三个独立入口。
- [x] 三个入口均可独立编译。
- [x] 创建 `internal/config`、`internal/domain`、`internal/transport/http`、`internal/messaging`、`internal/storage/postgres` 和 `internal/sse` 包边界。
- [x] 运行配置统一从环境变量读取，并支持本地 `.env` 加载。
- [x] API、worker、sim 和运维日志统一使用 `log/slog` 输出结构化 JSON。
- [x] 提供执行 `go test -cover ./...` 的本地测试命令。
- [x] 提供 golangci-lint 配置并通过 `golangci-lint run`。

## Dependencies

None

## Type

infra

## Priority

high
