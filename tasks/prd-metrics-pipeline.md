# PRD：metrics-pipeline Go 训练指标采集与查询服务

## 1. Overview

交付可独立部署的 Go 指标管道服务，完成训练指标的批量接入、RabbitMQ 缓冲、批量幂等落库、历史查询、时间降采样、任务摘要、SSE 实时推送、断点续传、模拟器和对账验收。

服务面向训练中心监控页，v1 无前端、鉴权和多租户功能。

## 2. Goals

- 支持 50 个并发训练任务、合计 500 个采样点/秒持续 10 分钟。
- 合法指标零丢失，重复上报零重复落库。
- worker kill -9 后重启，消息可恢复并产生 exactly-once effect。
- 历史曲线查询 P95 < 200ms，任务摘要 P95 < 100ms。
- 指标落库到 SSE 客户端收到事件的延迟 < 1 秒。
- 提供 Docker Compose 一键启动、健康检查、对账和模拟验收工具。

## 2.1 Scope Priority

- 原任务书 MUST：F1-F6、历史查询、摘要、SSE 实时推送和 N1-N6 验收指标。
- `[Reviewed v1 baseline addition]`：F7 健康检查与审计、Last-Event-ID 续传、7 天自动清理、SIGTERM 优雅退出。
- `[Extension]`：`duplicate_steps` 响应字段、step 维度分桶字段、长期事件归档、Redis Streams 或 TimescaleDB。

## 3. User Stories

### US-001：从零搭建项目骨架与完整服务链路

作为开发者，我希望从空仓库建立可运行的 Go 服务骨架，并通过 Docker Compose 启动 PostgreSQL、RabbitMQ、API 和 worker。

**验收标准：**

- [ ] 创建 Go module，使用 Go 1.22 或更高版本。
- [ ] 创建 `cmd/api`、`cmd/worker` 和 `cmd/sim` 三个独立入口，并可分别编译。
- [ ] 创建配置、领域模型、HTTP、RabbitMQ、PostgreSQL、SSE 和日志的基础内部包边界。
- [ ] 创建数据库迁移目录和基础迁移执行机制。
- [ ] Compose 可启动全部基础服务。
- [ ] 数据库迁移和日分区初始化成功。
- [ ] API 与 worker 可独立重启。
- [ ] 配置全部来自环境变量。
- [ ] 使用 `log/slog` 输出结构化 JSON 日志。
- [ ] 提供多阶段 Dockerfile 和本地测试命令。
- [ ] `go test` 和静态检查通过。

### US-002：批量接入训练指标

作为指标生产者，我希望批量提交某个任务的训练指标。

**验收标准：**

- [ ] API 接受 `POST /api/v1/ingest/metrics`。
- [ ] 整批校验失败时返回 `400 INVALID_PARAMS`，不接受部分数据。
- [ ] 单次 `batch` 最多包含 500 个采样点；step、时间戳和数值字段满足 v1 校验边界。
- [ ] 同一指标点重试时复用首次上报的 `ts`；改变 `ts` 后按新指标点处理。
- [ ] 校验成功后发布 RabbitMQ，并立即返回 `accepted` 和 `task_id`。
- [ ] MQ 发布失败经过重试后返回 `503 MQ_UNAVAILABLE`。
- [ ] API 不直接写入 PostgreSQL。

### US-003：批量消费与幂等落库

作为系统管理员，我希望 worker 可靠消费指标并批量写入 PostgreSQL。

**验收标准：**

- [ ] worker 展开每个 `metrics` key-value 为一条指标记录。
- [ ] 累计 500 条展开指标或等待 1 秒时触发批量写入。
- [ ] 一个 flush 使用 PostgreSQL 事务和 `pgx.Batch` 一次往返提交。
- [ ] 数据库事务提交后，对该 flush 关联的所有 delivery 执行 ack。
- [ ] 数据库事务失败时，对该 flush 关联的所有 delivery 执行 `nack(requeue=true)`。
- [ ] 一条 delivery 被拆分到多个 flush 时，全部分片成功后才 ack。
- [ ] 使用 `ON CONFLICT DO NOTHING` 消除相同四字段重复数据。
- [ ] worker 重启后未 ack 消息可以重新消费。

### US-004：分区、事件日志与保留策略

作为系统管理员，我希望指标数据和 SSE 事件具备可维护的存储结构。

**验收标准：**

- [ ] `metric_points` 按 UTC 自然日 RANGE 分区。
- [ ] 主键为 `(task_id, key, step, ts)`。
- [ ] 指标和事件数据保留 7×24 小时。
- [ ] 清理任务可以删除过期分区或对应数据。
- [ ] `metric_events` 为每个任务维护递增 `event_seq`。

### US-005：历史查询与时间降采样

作为监控页用户，我希望按任务、指标、时间和 step 范围查询历史曲线。

**验收标准：**

- [ ] 支持 `keys`、`from`、`to`、`step_from`、`step_to`、`max_points`。
- [ ] 时间条件和 step 条件取交集。
- [ ] 每个 key 独立判断是否超过 `max_points`。
- [ ] 超限后始终按时间分桶，并在 SQL 层计算 `min`、`max`、`avg`。
- [ ] `v` 使用桶内平均值，`step` 和 `ts` 取桶内最早点。
- [ ] 使用现有 `bucket_ms` 表达桶宽。
- [ ] 任务存在但过滤结果为空时返回 `200` 和空 `series`。
- [ ] 任务没有任何落库数据时返回 `404 TASK_NOT_FOUND`。

### US-006：任务指标摘要

作为监控页用户，我希望查看任务各指标的最新值和统计摘要。

**验收标准：**

- [ ] 返回 `last`、`min`、`max`、`avg`、`last_step` 和 `updated_at`。
- [ ] 无数据任务返回 `404 TASK_NOT_FOUND`。
- [ ] 单任务摘要接口 P95 小于 100ms。
- [ ] 聚合查询使用任务、key、时间索引。

### US-007：SSE 实时推送与断点续传

作为监控页用户，我希望实时接收新指标，并在网络断开后继续接收遗漏事件。

**验收标准：**

- [ ] 支持现有 SSE endpoint 和 `metrics`、`ping` 事件结构。
- [ ] 每个任务每次成功 flush 生成一个事件，payload 可包含多个 step 和 key。
- [ ] SSE ID 使用 `{task_id}:{event_seq}`。
- [ ] `Last-Event-ID` 在 7 天事件保留窗口内按序补发。
- [ ] 没有 `Last-Event-ID` 时只推送连接建立后的新事件。
- [ ] 格式错误或超出保留窗口的游标返回 `400 INVALID_PARAMS`。
- [ ] 每 15 秒发送心跳。
- [ ] 测试窗口内每条样本从落库到订阅端收到事件的延迟小于 1 秒。
- [ ] 客户端断开后释放 Hub 订阅和 goroutine。
- [ ] API 实例重启或切换实例后仍能从事件日志恢复。

### US-008：健康检查、就绪检查与审计

作为验收人员，我希望检查服务依赖状态并核对任务数据。

**验收标准：**

- [ ] `/healthz` 返回进程存活状态。
- [ ] `/readyz` 检查 PostgreSQL 和 RabbitMQ 可达性。
- [ ] audit 响应严格遵循任务书 5.5 结构。
- [ ] `point_count` 统计去重后的指标行数。
- [ ] `distinct_steps` 统计任务内不同 step 值。
- [ ] 不同 ts 造成的 `(key, step)` 重复写入结构化日志。
- [ ] `duplicate_steps` 不进入 v1 audit 响应。

### US-009：模拟器与验收对账

作为验收人员，我希望使用 CLI 模拟 LF callback 并自动对账。

**验收标准：**

- [ ] `cmd/sim` 生成 task_id、step、ts、loss 和 lr。
- [ ] 支持任务数量、持续时间、采样速率和 batch 大小配置。
- [ ] 支持 2% 重复批次注入。
- [ ] 支持断连注入。
- [ ] 支持 `eval_loss`、`gpu_util`、`gpu_mem`、`throughput` 开关。
- [ ] `loss` 使用指数衰减加噪声生成。
- [ ] `lr` 使用 warmup 加余弦衰减生成。
- [ ] `eval_loss` 按每 N step 稀疏生成。
- [ ] `gpu_util` 和 `gpu_mem` 使用随机游走生成。
- [ ] `throughput` 按开关生成 tokens/s 指标。
- [ ] `--audit` 输出 PASS/FAIL 及服务端 audit 结果。

### US-010：端到端链路测试

作为 QA 工程师，我希望自动验证完整指标链路。

**验收标准：**

- [ ] E2E 测试启动或连接 PostgreSQL、RabbitMQ、API 和 worker。
- [ ] 测试提交指标，验证 API 响应、MQ 消费、数据库落库和查询结果。
- [ ] 测试重复提交，验证无重复落库。
- [ ] 测试 worker 强制退出后重启，验证数据可恢复。
- [ ] 测试 SSE 收到指标、断线重连和 `Last-Event-ID` 补发。
- [ ] 测试非法 batch、空过滤结果和无数据任务。
- [ ] 测试可重复执行并在结束时清理测试数据。

## 4. Functional Requirements

- FR-1：系统必须提供 `POST /api/v1/ingest/metrics`。
- FR-2：系统必须使用统一错误结构 `{"error":{"code":"...","message":"..."}}`。
- FR-3：系统必须可靠保存已通过校验的接入消息，直到 worker 完成持久化处理。
- FR-4：接入接口必须在消息可靠发布确认后返回成功响应。
- FR-5：worker 必须在批量事务提交后确认关联 delivery，并在事务失败后重新入队关联 delivery。
- FR-6：系统必须按 `(task_id, key, step, ts)` 实现幂等落库。
- FR-7：系统必须提供历史查询、摘要和 SSE endpoint。
- FR-8：系统必须支持时间与 step 条件交集过滤。
- FR-9：系统必须在 SQL 层完成时间降采样。
- FR-10：每个成功持久化的新指标 flush 必须产生一个任务级实时事件。
- FR-11：实时事件必须广播给所有在线 API 实例，并在 1 秒内进入订阅连接。
- FR-12：SSE 必须按任务事件序号有序推送，并支持 7 天窗口内的断点续传。
- FR-13：系统必须通过 PostgreSQL 事件日志支持 7 天内断点续传。
- FR-14：系统必须提供 simulator、audit、healthz 和 readyz。
- FR-15：系统必须监听 SIGTERM 并执行优雅退出。

## 5. Fixed API Contract

v1 严格遵循任务书 5.1–5.6：

- `POST /api/v1/ingest/metrics`
- `GET /api/v1/tasks/{task_id}/metrics`
- `GET /api/v1/tasks/{task_id}/metrics/stream`
- `GET /api/v1/tasks/{task_id}/summary`
- `GET /api/v1/admin/tasks/{task_id}/audit`

字段名、状态码、错误码、SSE 事件名、SSE ID 格式保持现有定义。新增字段和新增 SSE 事件只作为后续扩展候选。

## 6. Normative Contract Appendix

本附录完整复制任务书 5.1–5.6，作为本 PRD 的规范性接口、数据模型、幂等和错误契约。开发、测试和验收以本附录为准。

### 6.1 批量上报 · POST /api/v1/ingest/metrics

请求 `Content-Type: application/json`：

```json
{
  "task_id": "ft-20260825-0001",
  "batch": [
    {
      "step": 120,
      "ts": 1756089600123,
      "metrics": {
        "loss": 1.234,
        "lr": 3e-05
      }
    }
  ]
}
```

响应 200：

```json
{
  "accepted": 40,
  "task_id": "ft-20260825-0001"
}
```

同一指标点重试时必须复用首次上报的 `ts`；修改 `ts` 将被视为新的指标点。该语义保证 `(task_id, key, step, ts)` 能够作为业务唯一键实现幂等落库。

### 6.2 历史查询 · GET /api/v1/tasks/{task_id}/metrics

| 参数 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| keys | 否 | 全部 key | 逗号分隔，如 `loss,lr` |
| from / to | 否 | 全量 | 毫秒时间戳，左闭右开 |
| step_from / step_to | 否 | 全量 | 步数区间，与时间区间取交集 |
| max_points | 否 | 500 | 每条曲线返回点数上限；超出触发服务端降采样 |

响应 200：

```json
{
  "task_id": "ft-20260825-0001",
  "downsampled": true,
  "bucket_ms": 57600,
  "series": {
    "loss": [
      { "step": 12, "ts": 1756089600123, "v": 1.23, "min": 1.10, "max": 1.40 },
      { "step": 452, "ts": 1756090176123, "v": 0.98, "min": 0.91, "max": 1.05 }
    ],
    "lr": [ ... ]
  }
}
```

### 6.3 实时推送 · GET /api/v1/tasks/{task_id}/metrics/stream（SSE）

```text
event: metrics
id: ft-20260825-0001:00128
data: {"points":[{"step":120,"ts":1756089600123,"loss":1.234,"lr":3e-05}]}

event: ping
data: {"ts":1756089615000}
```

`id` 格式为 `{task_id}:{递增序号}`，用于 `Last-Event-ID` 续传；`ping` 为 15 秒心跳。task_id 尚无任何数据时连接保持建立，不报错；该任务之后开始上报时数据能推送。

### 6.4 任务摘要 · GET /api/v1/tasks/{task_id}/summary

```json
{
  "task_id": "ft-20260825-0001",
  "last_step": 1234,
  "updated_at": 1756099812345,
  "metrics": {
    "loss": { "last": 0.87, "min": 0.79, "max": 2.31, "avg": 1.42 },
    "lr": { "last": 2.9e-05, "min": 0.0, "max": 5e-05, "avg": 3.1e-05 }
  }
}
```

### 6.5 对账端点 · GET /api/v1/admin/tasks/{task_id}/audit

```json
{
  "task_id": "ft-20260825-0001",
  "point_count": 40218,
  "distinct_steps": 20011,
  "first_step": 0,
  "last_step": 5000,
  "keys": ["loss", "lr"],
  "missing_steps": []
}
```

`point_count` 为该任务去重后的落库点数；`distinct_steps` 为任务内不同 step 值的数量；`missing_steps` 为 step 区间内的可选缺口检测结果。不同 ts 造成的 `(key, step)` 重复通过结构化日志记录，`duplicate_steps` 暂不进入 v1 响应。

### 6.6 错误码规范

| HTTP | code | 场景 |
| --- | --- | --- |
| 400 | INVALID_PARAMS | 字段缺失、类型错误、step 为负、batch 超长；整批拒绝 |
| 404 | TASK_NOT_FOUND | 查询或摘要的 task_id 无任何数据 |
| 503 | MQ_UNAVAILABLE | 接入层写 MQ 失败，包含重试后仍失败 |
| 500 | INTERNAL | 未预期异常；响应体不得泄露堆栈 |

所有错误响应统一格式：`{"error":{"code":"...","message":"人类可读描述"}}`。

## 7. Non-Goals

- Kubernetes、Volcano、GPU 调度和训练进程集成。
- LLaMA-Factory 源码修改。
- 前端监控页面。
- 用户体系、鉴权和多租户。
- Redis、TimescaleDB 作为 v1 运行依赖。
- 训练任务生命周期管理。

## 8. Technical Constraints

- 语言与模块：使用 Go 1.22 或更高版本，统一采用 Go module mode，仓库包含单一 `go.mod`。
- HTTP：使用 Gin v1.10 完成路由、中间件、JSON binding 和响应处理。
- PostgreSQL：使用 `jackc/pgx` v5 和 `pgxpool`；批量写入使用 `pgx.Batch`。
- RabbitMQ：使用 `rabbitmq/amqp091-go`。
- AMQP 并发模型：每个发布或消费 goroutine 独占 AMQP Channel；连接恢复后重新创建 Channel、声明拓扑并恢复 confirms 或消费。
- 日志：所有 API、worker、sim 和运维日志统一通过 `log/slog` 输出结构化 JSON。
- 配置：运行配置统一来自环境变量；本地开发环境可通过 `godotenv` 加载 `.env`。
- 部署：提供 Docker Compose 和多阶段 Dockerfile；API 与 worker 使用独立启动命令。
- 单元测试：使用标准库 `testing`，可使用 `testify` 编写断言和测试辅助代码。
- 覆盖率：项目测试命令必须执行 `go test -cover ./...` 并输出覆盖率结果。
- 静态检查：项目必须提供 golangci-lint 配置，并通过 `golangci-lint run`。

## 9. Technical Design

- `[Assumption]` 项目从空仓库开始，建议采用以下基础目录边界：

  - `cmd/api`：HTTP API、RabbitMQ 发布、SSE、实例内存 Hub。
  - `cmd/worker`：业务队列消费、批量落库、Outbox relay、实时事件发布。
  - `cmd/sim`：造数、故障注入和对账。
  - `internal/config`：环境变量解析与配置校验。
  - `internal/domain`：指标、事件和审计领域模型。
  - `internal/transport/http`：Gin 路由、请求绑定、响应和错误处理中间件。
  - `internal/messaging`：RabbitMQ Exchange、Queue、发布、消费和重试。
  - `internal/storage/postgres`：连接池、批量写入、查询、事件日志和 Outbox。
  - `internal/sse`：连接管理、Hub、事件补发和心跳。
  - `migrations`：表结构、分区和索引迁移。
  - `tests/e2e`：完整链路测试和测试数据清理。
- 项目骨架必须支持 API、worker、sim 独立编译和独立启动。
- worker 必须维护 flush 与 AMQP delivery 的关联；一条 delivery 被拆分时，全部分片成功后才 ack。
- 每个 flush 使用 PostgreSQL 事务和 `pgx.Batch` 一次往返提交；事务失败时对关联 delivery 执行 `nack(requeue=true)`。
- 业务 MQ 使用持久化 Exchange/Queue、publisher confirms、手动 ack 和 DLQ。
- 实时 MQ 使用 fanout Exchange 和每实例独立临时队列。
- Outbox 与 `metric_points`、`metric_events` 在同一个 PostgreSQL 事务中提交。
- SSE 连接先注册 Hub，再读取事件日志并按高水位补发，最后发送实时缓存事件。
- 有效消息的临时失败持续重试；协议性错误进入 DLQ。
- `[Assumption]` 缺失的附录 A、11.3 配置节和第 12 节实现约束按本 PRD 补全。

## 10. Design Decisions

### DD-001：RabbitMQ 作为业务缓冲和实时广播总线

选择 RabbitMQ 统一承载业务消息和实时广播。业务链路使用持久化 Exchange/Queue，实时链路使用 fanout Exchange 和每个 API 实例的独立临时队列。

选择依据：符合任务书技术栈，支持业务流量削峰、手动 ack、重试和多 API 实例广播。API 内存 Hub 保留为进程内连接管理层。

代价：增加 Exchange、Queue、连接恢复和消息确认的运维复杂度；实时消息的可靠补偿由 PostgreSQL 事件日志承担。

备选方案：PostgreSQL `LISTEN/NOTIFY` 或 API 轮询事件表。两者基础设施更少，广播和故障恢复策略需要额外设计。

### DD-002：Outbox 与事件日志支持原子落库和断点续传

worker 在同一 PostgreSQL 事务中写入实际新增的 `metric_points`、`metric_events` 和 Outbox 记录。relay 提交 RabbitMQ 发布确认后更新 Outbox 状态。

选择依据：数据库事务保证指标和发布意图同时提交；事件日志提供 API 重启、实例切换和客户端断线后的有序补发。

代价：增加事件存储、relay 重试和事件清理逻辑。RabbitMQ 与 PostgreSQL 采用最终一致的发布流程，重复发布通过 `event_seq` 去重。

### DD-003：PostgreSQL 原生按日分区

选择 PostgreSQL 原生 RANGE 分区和 SQL 时间桶聚合。

选择依据：无需引入 TimescaleDB，符合任务书推荐方案，足以覆盖 500 采样点/秒和目标查询延迟，分区删除可支持 7 天保留策略。

代价：需要维护日分区预建、过期分区清理和跨分区查询索引。TimescaleDB 与预聚合表保留为后续演进方案。

### DD-004：时间降采样和 step 交集过滤

查询先应用 `from/to` 与 `step_from/step_to` 的交集，再按 key 判断结果数量，超出 `max_points` 后始终按时间分桶。

选择依据：保持现有 `bucket_ms` 响应契约，前端曲线拥有稳定的时间轴；step 条件继续提供精确范围过滤。

代价：step 维度桶聚合暂不进入 v1，相关响应字段列为后续扩展。

### DD-005：业务幂等键保留契约外脏数据

使用 `(task_id, key, step, ts)` 作为主键。相同四字段重复消息被忽略；相同 `(task_id, key, step)` 携带不同 ts 的数据继续落库，并由 audit 内部检测、结构化记录。

选择依据：严格遵循任务书数据模型，保留原始脏数据证据，便于验收和问题定位。

代价：同一 key 和 step 可能存在多个时间戳，`duplicate_steps` 需要通过内部日志或后续 audit 字段观察。

### DD-006：SSE 事件按任务级 flush 生成序号

每个任务每次成功 flush 生成一个 `event_seq`，事件 payload 可包含多个 step 和 key。重复指标未产生实际插入时不生成 SSE 事件。

选择依据：保持现有 `points` 数组结构，减少事件数量，便于客户端按任务顺序恢复。

代价：事件序号与训练 step 采用两套独立序列，客户端需要使用 SSE ID 进行断点管理。

### DD-007：瞬时错误重试，协议错误进入 DLQ

数据库连接、网络和 RabbitMQ 临时故障持续重试；JSON 解析失败、消息结构非法等协议错误进入 DLQ。

选择依据：有效指标获得恢复机会，异常消息获得隔离和人工排查入口。

代价：需要错误分类、延迟重试和 DLQ 监控。

## 11. Success Metrics

| 编号 | 指标 | 目标值 | 验证方式 |
| --- | --- | --- | --- |
| N1 | 写入吞吐 | 50 个并发任务，每个 10 个采样点/秒，合计 500 个采样点/秒，持续至少 10 分钟，零丢失；采样点按入站 batch item 计数，worker 展开 key-value 后形成落库行 | simulator `--audit` 对账；`point_count` 与理论值一致 |
| N2 | 重复上报 | 注入 2% 重复批次后零重复落库 | audit 比较去重后的 `point_count` 和预期结果 |
| N3 | 消费者故障恢复 | worker kill -9 后重启，不丢失、不重复 | 故障演练后执行 audit |
| N4 | 历史查询性能 | 查询 8 小时曲线，约 28,800 个原始点/key，`max_points=500`，P95 < 200ms | 压测脚本计时 |
| N5 | 摘要性能 | 单任务全量聚合 P95 < 100ms | 压测脚本计时 |
| N6 | SSE 实时性 | 新指标落库到 SSE 订阅方收到事件，测试窗口内每条样本延迟 < 1 秒 | 上报端和订阅端双端打点 |

7 天保留窗口内的 `Last-Event-ID` 断点续传、健康检查、自动清理和 SIGTERM 优雅退出属于评审确认的 v1 基线新增要求。

## 12. Open Questions

- audit 响应增加 `duplicate_steps` 字段。
- 历史查询增加 step 分桶相关字段。
- 事件日志超过 7 天的长期归档策略。
- Redis Streams 或 TimescaleDB 的演进评估。
