# metrics-pipeline-go

面向训练任务指标的异步写入管道。API 接收批量指标并发布到 RabbitMQ；worker 消费消息、批量写入 PostgreSQL，同时发布 SSE 事件。项目提供迁移工具、负载模拟器和 N1-N6 验收压测工具。

## 组件

| 组件 | 入口 | 职责 |
| --- | --- | --- |
| API | `cmd/api` | 校验指标批次并写入消息队列，提供历史、摘要、审计和 SSE 接口 |
| worker | `cmd/worker` | 消费 RabbitMQ 消息，批量写入 PostgreSQL，维护分区并发布 SSE 事件 |
| migrate | `cmd/migrate` | 创建或升级数据库结构，维护时间分区 |
| sim | `cmd/sim` | 按指定任务数、速率和批大小持续上报指标 |
| perf | `cmd/perf` | 执行 N1-N6 写入、恢复、查询和 SSE 验收，并生成 JSON 报告 |

## 环境要求

- Go 1.22+
- Docker Desktop，已启动并启用 Docker Compose
- PowerShell 7 或 Windows PowerShell

## 一键启动

从示例创建本地配置，并启动 PostgreSQL、RabbitMQ、迁移、API 和 worker：

```powershell
Copy-Item .env.example .env
docker compose up --build --detach --wait
```

检查服务状态：

```powershell
docker compose ps
Invoke-WebRequest http://127.0.0.1:8081/readyz
docker compose exec postgres pg_isready -U metrics -d metrics
docker compose exec rabbitmq rabbitmq-diagnostics -q ping
```

停止服务并保留数据卷：

```powershell
docker compose stop
docker compose down
```

服务端口：

| 服务 | 地址 |
| --- | --- |
| API | `http://127.0.0.1:8080` |
| API 就绪检查 | `http://127.0.0.1:8081/readyz` |
| PostgreSQL | `127.0.0.1:5432` |
| RabbitMQ AMQP | `127.0.0.1:5672` |
| RabbitMQ 管理台 | `http://127.0.0.1:15672` |

本地环境的 PostgreSQL 数据库名、用户名和密码均为 `metrics`；连接时选择 SSL 禁用。RabbitMQ 管理台账号和密码均为 `metrics`。

`127.0.0.1` 会使用 IPv4 回环地址。Windows 上 `localhost` 可能优先解析为 IPv6 的 `[::1]`，当 API 仅监听 IPv4 时会出现连接失败。

## 本机运行

Docker Compose 负责依赖服务后，可以将 API、worker 和模拟器作为本机进程运行。配置文件在启动时自动加载，系统环境变量拥有更高优先级。

```powershell
Copy-Item .env.example .env
docker compose up postgres rabbitmq --detach --wait
go run ./cmd/migrate
go run ./cmd/api
```

另开一个 PowerShell 窗口启动 worker：

```powershell
go run ./cmd/worker
```

再开一个窗口运行负载模拟器：

```powershell
go run ./cmd/sim --endpoint http://127.0.0.1:8080/api/v1/ingest/metrics --tasks 5 --rate 10 --duration 30s --batch-size 10 --audit
```

## API 示例

写入接口接受一个任务的一批 sample。每个 sample 可包含多个 metric key；worker 会将一个 `loss` 和一个 `lr` 展开为两个 metric point。

```powershell
$timestamp = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$payload = @{
  task_id = 'demo-task-001'
  batch = @(
    @{ step = 1; ts = $timestamp; metrics = @{ loss = 0.82; lr = 0.001 } }
    @{ step = 2; ts = $timestamp + 1000; metrics = @{ loss = 0.71; lr = 0.001 } }
  )
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/api/v1/ingest/metrics -ContentType 'application/json' -Body $payload
```

API 返回 `accepted` 和 `task_id`。API 成功响应表示消息已经进入队列，随后 worker 会完成数据库写入。查询接口在 worker 写入后即可返回数据。

```powershell
$taskID = 'demo-task-001'

# 历史指标：keys、from、to、step_from、step_to 和 max_points 都是可选参数。
Invoke-RestMethod "http://127.0.0.1:8080/api/v1/tasks/$taskID/metrics?keys=loss,lr&max_points=500"

# 每个 key 的最新值、最小值、最大值和平均值。
Invoke-RestMethod "http://127.0.0.1:8080/api/v1/tasks/$taskID/summary"

# 对账：point_count 是展开后的 metric point 数；distinct_steps 是任务中的不同 step 数。
Invoke-RestMethod "http://127.0.0.1:8080/api/v1/admin/tasks/$taskID/audit"

# SSE 长连接。通过 Ctrl+C 结束观察。
curl.exe -N "http://127.0.0.1:8080/api/v1/tasks/$taskID/metrics/stream"
```

`max_points` 默认值为 500，最大值为 5000。历史数据超过该限制时，接口会返回 `downsampled: true`、`bucket_ms` 和每个桶的代表点；代表点包含 `v`、`min` 与 `max`。

## 数据库迁移与查看

`migrate` 在 Compose 启动过程中自动执行，已应用版本与 SHA-256 校验和记录在 `schema_migrations`。需要手动执行时使用：

```powershell
docker compose run --rm migrate
```

迁移会创建 `metric_points`、`metric_events`、`metric_outbox` 和 `task_event_counters`。指标与事件表按 UTC 日期分区，初始迁移确保当前日期前 8 天至后 2 天的分区存在。worker 每小时维护 168 小时数据窗口：完整过期日分区会被删除，截止日内的过期记录按批次清理。

在 DBeaver 或 Navicat 中连接 PostgreSQL 时，连接参数为：

| 字段 | 值 |
| --- | --- |
| Host | `127.0.0.1` |
| Port | `5432` |
| Database | `metrics` |
| User | `metrics` |
| Password | `metrics` |
| SSL | 禁用 |

表位于 `metrics` 数据库的 `public` schema。连接到默认 `postgres` 数据库时，表树会显示为空。旧版 Navicat 读取 PostgreSQL 16 元数据时可能报 `datlastsysoid` 相关错误，升级客户端后可正常浏览表结构。

## 工程检查

项目在 PowerShell 中使用 Go 命令完成构建、测试与静态检查：

```powershell
go build -buildvcs=false ./cmd/...
go test ./...
go vet ./...
```

可选的 golangci-lint：

```powershell
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1
golangci-lint run
```

## N1-N6 性能验收

压测工具位于 `cmd/perf/main.go`。正式验收使用 50 个任务、每任务 10 samples/s、每批 10 个 sample，持续 10 分钟。每个 sample 含 `loss` 与 `lr` 两个 key，因此负载为 500 samples/s、约 1000 metric points/s。工具在第 1 秒终止并恢复 worker，随后执行历史查询、摘要查询和 SSE 延迟测量。

完整验收：

```powershell
docker compose up --build --detach --wait
go run ./cmd/perf --endpoint http://127.0.0.1:8080/api/v1/ingest/metrics --duration 10m --report perf-report.json
```

30 秒冒烟用于确认压测链路、worker 恢复和报告生成：

```powershell
go run ./cmd/perf --endpoint http://127.0.0.1:8080/api/v1/ingest/metrics --duration 30s --report perf-smoke.json
```

冒烟结果会记录各项实测值。N1 的正式通过条件要求 10 分钟持续负载，因此 30 秒报告会将 N1 标记为失败。`--report` 每次运行都会覆盖指定文件；需要保留多次结果时传入不同文件名。

| 验收项 | 过程 | 通过条件 |
| --- | --- | --- |
| N1 | 持续上报并轮询审计接口 | 500 samples/s 持续 10 分钟，对账通过 |
| N2 | 注入 2% 重复批次并对账 | 重复 metric point 计数为 0 |
| N3 | 压测第 1 秒执行 `docker compose kill worker`，2 秒后恢复 worker | 丢失和重复 metric point 计数均为 0 |
| N4 | 为一个任务补入 8 小时、每秒 2 key 的历史数据，重复测量 20 次 | 历史查询 P95 小于 200 ms |
| N5 | 重复测量摘要接口 20 次 | 摘要查询 P95 小于 100 ms |
| N6 | 建立 SSE 连接后写入 20 个 sample | SSE 端到端延迟小于 1000 ms |

报告结构包括：

```json
{
  "pass": true,
  "load": {
    "samples_per_second": 500,
    "metric_points_per_second": 1000
  },
  "checks": [
    { "id": "N1", "value": 500, "threshold": 500, "unit": "samples/s", "pass": true }
  ],
  "audit": {},
  "errors": []
}
```

检查 `checks` 中每个项目的 `pass`，并用 `audit` 中的 `point_count`、`distinct_steps`、`missing_steps` 验证写入完整性。持续正式负载时，`point_count` 对应 sample 数乘以每个 sample 的 metric key 数；`distinct_steps` 对应唯一的 step 数。
