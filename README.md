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

## 数据库迁移

`migrate` 一次性容器在 PostgreSQL 健康后自动执行，API 和 worker 会等待迁移成功。迁移文件位于 `migrations/`，已应用版本及 SHA-256 校验和记录在 `schema_migrations`。

每次执行迁移都会确保 UTC 今天前 8 天至未来 2 天的指标与事件日分区存在。已有数据库可安全重复执行：

```powershell
docker compose run --rm migrate
```

本机运行迁移时，默认连接 `localhost:5432`：

```powershell
go run ./cmd/migrate
```

worker 每小时预建日分区并维护严格的 168 小时窗口：完整过期分区通过 `DROP TABLE` 删除，截止日内的过期指标和事件以批次删除；已发布的过期 Outbox 同步清理，待发布 Outbox 与任务事件序号持续保留。维护日志记录 cutoff、分区数、删除行数和耗时。

## N1-N6 性能验收

完整 Compose 环境启动后运行 `make perf`。命令执行 50 个并发任务、每任务 10 点/秒、默认持续 10 分钟，注入 2% 重复批次并执行一次 worker 强制退出/重启；随后测量历史查询、摘要和 SSE 延迟。结果写入 `perf-report.json`，包含每项指标的测量值、阈值和 PASS/FAIL。通过 `--duration` 可执行短时冒烟，少于 10 分钟的结果按 N1 标准判定失败。
