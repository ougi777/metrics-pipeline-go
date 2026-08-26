# metrics-pipeline-go

Metrics pipeline 的 Go 服务工程，包含 API、worker 和负载模拟器三个独立入口。

## 本地运行

复制 `.env.example` 为 `.env` 后，可分别运行三个进程：

```powershell
go run ./cmd/api
go run ./cmd/worker
go run ./cmd/sim
```

所有配置来自环境变量。开发环境启动时会自动加载当前目录的 `.env`，已存在的环境变量拥有更高优先级。

## 工程检查

安装项目固定使用的 lint 版本：

```powershell
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1
```

执行构建、测试和静态检查：

```powershell
make build
make test
make lint
```

直接执行测试命令：

```powershell
go test -cover ./...
```
