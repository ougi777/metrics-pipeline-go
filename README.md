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

## Docker Compose 本地环境

Compose 启动 PostgreSQL、RabbitMQ、API 和 worker。首次使用可从示例配置创建本地 `.env`：

```powershell
Copy-Item .env.example .env
```

构建并启动完整环境：

```powershell
docker compose build
docker compose up --detach --wait
```

API 和 worker 使用同一应用镜像及独立二进制，可分别重启：

```powershell
docker compose restart api
docker compose restart worker
```

检查配置、依赖健康状态和 Go 测试：

```powershell
docker compose config --quiet
docker compose exec postgres pg_isready -U metrics -d metrics
docker compose exec rabbitmq rabbitmq-diagnostics -q ping
go test -cover ./...
```

停止容器会保留命名数据卷，后续启动继续使用原有数据：

```powershell
docker compose stop
docker compose down
```

RabbitMQ 管理界面地址为 `http://localhost:15672`，本地默认账号和密码均为 `metrics`。
