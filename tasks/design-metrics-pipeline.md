# 采用 RabbitMQ 缓冲、PostgreSQL 原子落库与事件日志构建 metrics-pipeline v1

Author(s): metrics-pipeline 项目组  
Last updated: 2026-08-26  
Discussion at: [prd-metrics-pipeline.md](./prd-metrics-pipeline.md)  
Status: Draft

## Abstract / 摘要

我们采用一条可恢复的异步链路交付 metrics-pipeline v1：API 完整校验批次并等待 RabbitMQ publisher confirm，worker 以 500 条展开指标或 1 秒为阈值批量消费，PostgreSQL 在同一事务中写入指标点、任务事件和 Outbox，Outbox relay 再把事件广播到各 API 实例。历史查询和任务摘要直接聚合原始分区数据，SSE 通过任务级事件序号和 7 天事件日志完成补发。v1 的运行依赖限定为 Go、RabbitMQ 和 PostgreSQL；数据库唯一键与消息确认顺序共同提供 exactly-once effect，固定 HTTP/SSE 契约保持稳定。

## Background / 背景与动机

### 验收流量会把一次训练回调展开成更多数据库行

验收负载包含 50 个任务，每个任务每秒产生 10 个采样点，持续至少 10 分钟。每个采样点通常携带 `loss` 和 `lr` 两个 key，因此 500 个入站采样点/秒会形成约 1,000 条 `metric_points` 行/秒；10 分钟基线约为 300,000 个采样点和 600,000 条指标行。`accepted` 统计入站 `batch` 元素，audit 的 `point_count` 统计展开、去重后的指标行，模拟器必须同时维护这两套计数。

### 消息确认与数据库提交之间只有一种可靠顺序

下面这段时序包含两个独立故障窗口：

```text
RabbitMQ delivery
      |
      v
PostgreSQL transaction ----> commit
      |
      v
RabbitMQ ack
```

ack 早于 commit 时，worker 在两者之间退出会造成已确认消息丢失。commit 早于 ack 时，worker 在两者之间退出会触发消息重投。我们固定使用“事务提交后 ack”，再用 `(task_id, key, step, ts)` 唯一键吸收重投。这个组合提供端到端 exactly-once effect，同时保留 RabbitMQ 的至少一次交付语义。

### 数据库提交与实时广播需要共享一份持久化意图

worker 若先提交指标再直接发布实时事件，进程可能在两步之间退出，数据库已有数据，在线客户端却收不到事件。worker 若先发布再提交，客户端可能看到最终回滚的数据。我们把 `metric_points`、`metric_events` 和 `metric_outbox` 放进一个 PostgreSQL 事务；relay 只处理已经提交的 Outbox。发布确认与 Outbox 标记之间的退出会产生重复广播，SSE ID 和任务级高水位负责去重。

### 断点续传需要同时处理补发和实时消息的竞态

简单的“先查事件表，再订阅实时消息”会遗漏两步之间提交的事件。简单的“先订阅，再查事件表”会重复发送查询结果与实时缓冲中的同一事件。SSE 连接因此采用固定握手：先注册 Hub 订阅，再读取数据库高水位并补发，最后按 `event_seq` 去重后切换到实时流。

### 7 天数据量要求查询与保留策略从第一版就具备边界

按常见的两个指标 key 估算，持续负载一天约形成 8,640 万条指标行，7 天约形成 6.05 亿条。按 UTC 自然日分区让删除完整过期日变成元数据操作；历史曲线和任务摘要从原始点读取，并利用任务、key、时间和 step 索引在 SQL 层完成聚合。首次完整压测将验证 N4/N5，实测结果决定预聚合的引入时机。

## Design / Proposal / 设计

### 三个二进制共享领域契约，各自拥有清晰的故障边界

```text
cmd/sim
   |
   | POST batch
   v
cmd/api ----publisher confirm----> RabbitMQ durable ingest queue
   |                                      |
   | history / summary / SSE              | manual delivery
   v                                      v
PostgreSQL <----one transaction------ cmd/worker
   |                                      |
   | metric_events                        | outbox relay
   v                                      v
SSE replay                       RabbitMQ realtime fanout
                                          |
                                          v
                                API instance event bridges
                                          |
                                          v
                                    in-memory Hubs
```

目录边界如下：

```text
cmd/api/                       API 进程装配与生命周期
cmd/worker/                    consumer、batcher、relay、保留任务装配
cmd/sim/                       造数、故障注入、SSE 计时与 audit
internal/config/               环境变量解析、默认值和启动校验
internal/domain/               MetricPoint、MetricEvent、错误码
internal/transport/http/       Gin 路由、binding、中间件和响应
internal/messaging/            AMQP 拓扑、publisher、consumer、恢复
internal/storage/postgres/     事务写入、查询、摘要、audit、分区维护
internal/sse/                  Hub、补发、心跳和慢订阅者处理
migrations/                    前向数据库迁移
tests/e2e/                     全链路与故障恢复测试
```

`internal` 包之间通过小接口连接。`cmd` 只负责读取配置、创建依赖、启动 goroutine 和执行关闭顺序。领域模型不依赖 Gin、pgx 或 AMQP 类型，使校验、批处理和确认状态机可以用标准库测试。

### 接入 API 只在持久消息获得确认后返回成功

`POST /api/v1/ingest/metrics` 按以下顺序执行：

1. 以受限 reader 读取 JSON，请求体上限设为 1 MiB。
2. 解码固定结构并拒绝未知字段。
3. 校验 `task_id`、`batch` 以及每个采样点；任何错误都会让整批返回 `400 INVALID_PARAMS`。
4. 构造带 `schema_version: 1` 的内部消息，并投递到 publisher 工作池。
5. publisher 使用持久化消息、mandatory publish 和 confirm 模式发布。
6. broker ack 后返回 `200`；连接失败、broker nack、unroutable 或重试耗尽后返回 `503 MQ_UNAVAILABLE`。

核心校验边界如下：

| 字段 | v1 规则 |
| --- | --- |
| `task_id` | 1–64 字符，匹配 `[A-Za-z0-9][A-Za-z0-9._-]*` |
| `batch` | 1–500 个采样点 |
| `step` | `0 <= step <= 2147483647` |
| `ts` | 正 Unix 毫秒值，可转换为 PostgreSQL `timestamptz` |
| `metrics` | 每个采样点至少 1 个 key，每个 key 1–32 字符 |
| value | 有限 `float64`；JSON 解码同时排除 `NaN` 和无穷值 |

`accepted` 等于 `len(batch)`。API 生成 `message_id` 和 `correlation_id` 并写入 AMQP headers 与结构化日志。调用方在超时后重试可能让同一消息多次进入队列，数据库唯一键会吸收这些副本。

publisher 工作池中的每个 goroutine 独占一个 AMQP Channel。连接重建后，该 goroutine 重新声明拓扑、开启 confirm、注册 return/confirm 通知，再开始接收新发布任务。单次请求等待对应 delivery tag 的最终结果，默认在三次指数退避重试后返回 503。

### 业务队列提供持久缓冲，协议错误进入独立死信队列

v1 声明以下拓扑：

| 名称 | 类型与属性 | 用途 |
| --- | --- | --- |
| `metrics.exchange` | durable direct exchange | 接入消息入口 |
| `metrics.ingest.v1` | durable queue | worker 竞争消费 |
| `metrics.ingest` | routing key | API 到业务队列 |
| `metrics.dlx` | durable direct exchange | 协议错误隔离 |
| `metrics.ingest.dlq` | durable queue | 人工排查与重放 |
| `metrics.realtime` | durable fanout exchange | 已持久化事件广播 |
| `metrics.realtime.<instance>` | exclusive auto-delete queue | 每个 API 实例的实时订阅 |

业务消息使用 persistent delivery mode，Compose 为 RabbitMQ 数据目录挂载持久卷。worker 使用 manual ack 和有限 prefetch。JSON 损坏、版本未知、字段违反内部消息契约等协议错误执行 `nack(requeue=false)`，队列的 dead-letter 配置负责投递 DLQ。PostgreSQL 连接失败和事务冲突属于瞬时错误，worker 先执行带抖动的短退避，再执行 `nack(requeue=true)`。

### worker 用交付跟踪器保证跨 flush 消息的确认安全

worker 将一个采样点的 `metrics` map 展开为多条 `MetricPoint`。缓冲器以展开后的行数计数；500 行或 1 秒计时器任一先到都会触发 flush。一个 AMQP delivery 可以跨越多个 flush，因此每条 delivery 都有独立跟踪器：

```go
type deliveryState struct {
    tag           uint64
    totalRows     int
    committedRows int
    terminal      bool
}

type flushPart struct {
    delivery *deliveryState
    rows     []domain.MetricPoint
}
```

一个 consumer goroutine、一个 AMQP Channel 和一个顺序 batcher 构成一个确认域。flush 成功后，batcher 按 `flushPart` 增加 `committedRows`；当计数到达 `totalRows` 时执行单条 delivery ack。flush 失败后，batcher 对该 flush 涉及的 delivery 逐条执行 `nack(requeue=true)`，将这些 delivery 标记为 terminal，并丢弃它们仍在本地缓冲中的片段。已经提交的片段会在重投时再次到达，唯一键会把它们转成无副作用写入。

确认与拒绝始终由拥有 Channel 的 goroutine 执行。Channel 断开时，broker 自动回收所有未确认 delivery；连接恢复逻辑创建新 Channel、重声明拓扑、恢复 QoS 和消费。这个边界避开 `amqp091-go` Channel 的并发访问风险。

### 一个 PostgreSQL 事务提交指标、事件和发布意图

核心表承担四种职责：

```sql
CREATE TABLE metric_points (
    task_id varchar(64) NOT NULL,
    key varchar(32) NOT NULL,
    step integer NOT NULL,
    ts timestamptz NOT NULL,
    value double precision NOT NULL,
    PRIMARY KEY (task_id, key, step, ts)
) PARTITION BY RANGE (ts);

CREATE INDEX metric_points_task_time
    ON metric_points (task_id, key, ts, step);

CREATE TABLE task_event_counters (
    task_id varchar(64) PRIMARY KEY,
    last_event_seq bigint NOT NULL DEFAULT 0
);

CREATE TABLE metric_events (
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    task_id varchar(64) NOT NULL,
    event_seq bigint NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (created_at, task_id, event_seq)
) PARTITION BY RANGE (created_at);

CREATE INDEX metric_events_task_seq
    ON metric_events (task_id, event_seq);

CREATE TABLE metric_outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id varchar(64) NOT NULL,
    event_seq bigint NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_until timestamptz,
    claim_token uuid,
    published_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0,
    UNIQUE (task_id, event_seq)
);
```

每个 flush 开启事务并通过一次 `pgx.Batch.SendBatch` 提交两类 DML：

1. 为本 flush 涉及的任务执行 `INSERT ... ON CONFLICT DO NOTHING`，保证 `task_event_counters` 行存在。
2. 执行一个数据修改 CTE：插入 `metric_points` 并取得 `RETURNING` 行；按 task 聚合真实新增行；锁定并递增任务计数器；插入 `metric_events` 和 `metric_outbox`；返回事件及重复 step 诊断。

事务在读取全部 batch result 并关闭 batch 后提交。COMMIT 成功才推进 delivery 跟踪器。`ON CONFLICT DO NOTHING` 的 `RETURNING` 只包含真实新增行，因此纯重复 flush 不会生成 SSE 事件。

任务计数器行同时承担序号分配锁。并发 worker 写入同一任务时会在这行串行化，每个包含新增指标的任务每个 flush 递增一次；不同任务可以并行。序号允许在事务回滚时复用，已提交事件始终保持连续递增。

事件 payload 把新增指标按 `(step, ts)` 重新组合：

```json
{
  "points": [
    {"step": 120, "ts": 1756089600123, "loss": 1.234, "lr": 0.00003}
  ]
}
```

同一 `(task_id, key, step)` 以不同 `ts` 插入时，数据修改 CTE 返回诊断记录。worker 提交事务后通过 `slog` 输出 `task_id`、`key`、`step`、旧 `ts`、新 `ts`、delivery tag 和 message ID。原始行全部保留，audit v1 响应继续遵循固定字段。

### Outbox relay 以至少一次广播换取可恢复的实时链路

worker 内运行一个 relay leader。多个 worker 通过 PostgreSQL advisory lock 竞争领导权，持锁连接存活期间只有一个 relay 发布事件。relay 每 100ms 拉取最多 100 条待发布记录，使用短事务和 `FOR UPDATE SKIP LOCKED` 写入 `claim_token` 与租约，然后提交认领事务。发布获得 RabbitMQ confirm 后，relay 按 token 设置 `published_at`；失败记录按指数退避设置下次尝试时间。

进程在 RabbitMQ confirm 与 `published_at` 更新之间退出时，同一事件会再次广播。API event bridge 和 SSE subscription 都按 `(task_id, event_seq)` 去重。Outbox relay 按 `id` 发布，单 leader 让正常路径保持提交顺序；API 仍以数据库事件序号作为最终顺序依据。

实时队列承载低延迟广播，`metric_events` 承载恢复。API 的 AMQP 连接重建后会先为活跃 task 从各自内存高水位补读事件表，再恢复实时转发。API 进程退出会断开其 SSE 连接，客户端携带最后收到的 ID 重连即可继续。

### SSE 握手把补发边界固定在数据库高水位

SSE ID 使用 `{task_id}:{event_seq}`；输出时将序号补足至少五位，解析时按十进制整数处理。task_id 校验规则排除了冒号，因此游标可以无歧义拆分。心跳每 15 秒发送一次 `ping`，且不携带 ID。

带 `Last-Event-ID` 的连接执行以下算法：

```text
1. 解析游标并确认 task_id 与路径一致。
2. 查询游标对应事件；事件已超出 7 天窗口、序号越界或格式错误时返回 400。
3. 在 task Hub 注册带缓冲订阅，开始暂存实时事件。
4. 读取 task_event_counters.last_event_seq 作为高水位 H。
5. 从 metric_events 按序读取 (cursor, H] 并发送。
6. 对暂存事件排序，发送 seq > H 的事件；其余事件视为重复。
7. 进入实时循环，只发送 seq > lastSent 的事件。
```

省略 `Last-Event-ID` 时，步骤 3 之后读取高水位 `H`，并把 `H` 作为本连接基线。连接只发送 `seq > H` 的事件；尚无数据的 task 使用 `H=0` 并保持连接。

每个订阅使用固定容量 channel，默认容量为 256 个事件。慢客户端填满 channel 时，Hub 取消该订阅并结束 HTTP 流；客户端使用最后成功处理的 SSE ID 重连。Hub 广播路径保持常数时间，单个慢连接无法阻塞同任务的其他订阅者。发送循环和心跳循环都监听 `r.Context().Done()`，退出时同步注销订阅并停止 ticker。

### 历史查询按 key 决定原样返回或时间降采样

查询先验证参数，再确认 task 在保留窗口内拥有至少一条指标数据。task 有数据且过滤交集为空时返回 `200` 和空 `series`；任务在保留窗口内无任何指标行时返回 `404 TASK_NOT_FOUND`。

过滤条件统一表达为：

```sql
WHERE task_id = $1
  AND ($2::text[] IS NULL OR key = ANY($2))
  AND ($3::timestamptz IS NULL OR ts >= $3)
  AND ($4::timestamptz IS NULL OR ts <  $4)
  AND ($5::integer IS NULL OR step >= $5)
  AND ($6::integer IS NULL OR step <= $6)
```

时间条件与 step 条件天然取交集。第一条统计查询返回每个 key 的 `count/min(ts)/max(ts)`；每个 key 单独比较 `count` 与 `max_points`。只要一个 key 超限，响应的 `downsampled` 为 `true`。所有超限 key 共用一个 `bucket_ms`，它取各超限 key 所需桶宽的最大值；每个 key 以自身最早时间为桶原点，从而保证该 key 的桶数不超过 `max_points`。处于限额内的 key 保留原始点。

聚合查询在 PostgreSQL 中计算：

```sql
SELECT
    key,
    bucket_no,
    (array_agg(step ORDER BY ts, step))[1] AS step,
    min(ts) AS ts,
    avg(value) AS v,
    min(value) AS min,
    max(value) AS max
FROM bucketed
GROUP BY key, bucket_no
ORDER BY key, ts, step;
```

原始点使用 `v=min=max=value`，因此同一响应结构可以同时容纳稀疏原始 key 和高频降采样 key。`downsampled=false` 时 `bucket_ms` 返回 `0`；`downsampled=true` 时返回实际毫秒桶宽。`max_points` 默认 500，合法范围为 1–5000。

### 摘要直接聚合保留窗口内的原始指标

摘要接口从 `metric_points` 读取当前 7 天窗口内的数据，按 key 计算 `min`、`max` 和 `avg`，并按 `(ts, step)` 的确定性顺序选取最新值。任务级 `updated_at` 和 `last_step` 取所有 key 中最新的指标点。查询依靠分区裁剪和 `(task_id, key, ts, step)` 索引控制扫描范围，N5 压测结果作为后续引入预聚合的依据。

### 分区维护同时覆盖预建、精确保留和并发安全

`metric_points` 按指标 `ts` 的 UTC 自然日分区，`metric_events` 按 `created_at` 的 UTC 自然日分区。迁移服务创建父表、索引模板和当前分区；worker 中的 maintenance leader 使用 advisory lock，每小时预建过去 8 天至未来 2 天的分区。

保留截止时间为数据库 `clock_timestamp() - interval '7 days'`。维护任务执行两类操作：

1. 删除指标与事件中上界早于截止时间的完整日分区，并清理对应的已发布 Outbox。
2. 在截止时间所在分区内分批删除过期指标、事件及已发布 Outbox，使窗口保持精确的 168 小时。

每批边界删除限制行数并记录耗时、行数和剩余量，减少长事务与 autovacuum 压力。待发布 Outbox 始终保留，已发布记录可在事件确认保留后清理。`task_event_counters` 体积与任务数同阶，并持续保留序号，避免任务 ID 再次出现时复用 SSE ID。

### audit、健康检查和日志共同支撑验收定位

audit 直接按固定契约返回 `point_count`、`distinct_steps`、`first_step`、`last_step`、`keys` 和 `missing_steps`。`point_count` 使用展开后的去重行数；`distinct_steps` 使用任务内 `COUNT(DISTINCT step)`。`missing_steps` 在 `first_step..last_step` 范围可控时通过 `generate_series` 计算，超出安全上限时返回空数组并写入带原因的结构化日志，保护管理端点的资源边界。

API 和 worker 都提供管理监听端口。`/healthz` 只反映进程事件循环存活，`/readyz` 检查 PostgreSQL、RabbitMQ 连接状态及迁移版本；任一依赖失效时返回 503。Compose 将 API 的业务端口和两个进程的管理健康检查分别接入 healthcheck。

所有进程使用 `slog` JSON handler。关键日志字段统一包含 `service`、`instance_id`、`task_id`、`message_id`、`delivery_tag`、`event_seq`、`flush_rows`、`duration_ms`、`attempt` 和 `error_class`。日志正文省略请求完整 payload，避免高吞吐下重复存储指标值。

### 优雅退出让已接收工作落在可恢复边界上

API 收到 SIGTERM 后先让 readiness 失败，再停止接受新请求，等待在途 publisher confirm，关闭 HTTP server 和 AMQP 连接。SSE 客户端随 server shutdown 断开，并从最后事件 ID 恢复。

worker 收到 SIGTERM 后取消新 delivery、停止 batch timer 输入、flush 当前缓冲、提交成功片段并 ack，然后停止 relay 与维护任务。关闭期限到达时，worker 关闭 AMQP Channel；broker 会重投全部未确认 delivery。Compose 的 stop grace period 必须大于应用关闭期限。

### 模拟器把理论输入集合保存到对账阶段

`cmd/sim` 为每个任务维护确定性随机种子、step、首次 `ts` 和已发送 batch。重复注入直接重放已保存 batch，因此同一指标点复用首次 `ts`。断连注入关闭并重建 HTTP client transport，随后继续发送。

loss 使用指数衰减叠加有界噪声，lr 使用 warmup 加余弦衰减；可选指标分别使用稀疏周期、随机游走和 tokens/s 模型。模拟器在内存中维护预期唯一 `(task_id,key,step,ts)` 集合及每任务 step 集合，`--audit` 将服务端结果与这些集合的计数进行比较，输出机器可读 JSON 和终端 PASS/FAIL。

SSE 延迟测试在提交端记录样本身份与发送时间，在订阅端按收到事件内的 `(task_id,key,step,ts)` 关联，报告 max、P50、P95 和 P99。N6 的验收门槛使用测试窗口内 max 小于 1 秒。

## Rationale / 理由与取舍

### RabbitMQ 同时覆盖削峰与多实例广播

RabbitMQ 是任务书指定依赖，持久业务队列原生提供 publisher confirm、manual ack、竞争消费和 DLQ。实时 fanout exchange 让每个 API 实例获得完整事件流，PostgreSQL 事件表补足临时队列的恢复能力。

PostgreSQL `LISTEN/NOTIFY` 能减少一套实时拓扑，payload 大小、断线期间通知保存和连接池隔离都需要额外机制。API 轮询事件表提供简单恢复，轮询间隔与 N6 的 1 秒上限直接竞争，并持续增加数据库读负载。v1 选择 RabbitMQ 广播与事件表补发的组合。

### Transactional Outbox 把双写失败收敛成可去重的重复发布

数据库事务能原子保存指标和发布意图，relay 的唯一剩余故障窗口会产生重复事件。`event_seq` 让这个结果可检测、可去重、可补发。

worker 在 commit 后直接 publish 的方案包含永久事件缺口。分布式事务会显著提高部署与运维复杂度。v1 接受 Outbox 的额外表、relay 和清理成本，换取清晰的恢复路径。

### PostgreSQL 原生分区满足当前负载并保留标准运行环境

按日 RANGE 分区支持索引裁剪和快速删除完整过期日，SQL 时间桶覆盖当前查询契约。TimescaleDB hypertable 会提供成熟的保留策略与 continuous aggregate，同时增加扩展安装、镜像、升级和故障排查要求。当前验收规模可以由原生 PostgreSQL 覆盖，TimescaleDB 的评估触发点设为单实例写入或查询压测持续越过目标值。

### 摘要先采用原始分区聚合

v1 直接聚合 7 天保留窗口内的 `metric_points`，保持写入路径和数据模型简单。分区裁剪与查询索引覆盖当前验收路径，N5 压测持续超过 100ms 时再评估日级预聚合、分钟级预聚合或缓存方案。

### 任务级计数器让游标连续且易于验证

全局数据库 sequence 可以保证同一任务的事件值递增，同时会在任务内留下其他任务造成的间隙。UUID 能提供唯一性，却无法直接表达补发顺序。任务计数器用一行锁换取连续序号；同一任务每秒通常只有一个 flush，锁竞争低于可观测阈值。

### 时间桶保持现有响应契约和训练曲线的时间语义

step 桶适合 step 均匀、时间抖动明显的训练数据，响应契约当前只暴露 `bucket_ms`。应用层抽样可以实现 LTTB 等视觉算法，同时需要把全部原始点传入 Go 内存。v1 在 SQL 层采用时间桶，返回平均值并保留 min/max 尖峰；step 分桶字段进入后续契约版本评审。

### 四字段主键保留原始脏数据证据

`(task_id,key,step)` 唯一约束会把不同时间戳的重复 step 强制合并，并需要定义覆盖规则。四字段主键严格匹配调用方重试契约，同时间戳重试被吸收，不同时间戳记录完整保留。v1 通过结构化日志呈现这类异常，`duplicate_steps` 字段进入后续 audit 契约评审。

### 单 relay leader让正常路径天然有序

多 relay 能提高发布吞吐，也会让同一任务的 RabbitMQ 到达顺序依赖并发调度。验收负载下事件发布量约为每个活跃任务每次 flush 一条，单 relay 足以覆盖。advisory lock 提供自动接管，数据库事件日志继续承担最终顺序来源。

## Compatibility / 兼容性

### 这是全新服务，PRD 固定契约就是 v1 兼容性基线

仓库当前只有任务书与 PRD，尚无已发布二进制、数据库或调用方实现。设计保持五个固定 endpoint、错误包络、字段名、状态码、SSE 事件名和 ID 结构。`duplicate_steps`、step 分桶字段、长期事件归档、Redis 与 TimescaleDB 均留在后续版本讨论范围。

以下行为需要在联调前共同确认并固化为契约测试：

- `accepted` 统计入站 `batch` 元素，`point_count` 统计展开后的唯一指标行。
- 原始历史点返回 `v=min=max=value`，降采样响应使用同一结构。
- `bucket_ms=0` 表示原始响应，正值表示至少一个 key 经过时间降采样。
- `Last-Event-ID` 过期、越界、任务不匹配或格式错误统一返回 `400 INVALID_PARAMS`。
- 相同 `(task_id,key,step)` 携带不同 `ts` 时形成两条合法存储行，并产生结构化诊断日志。

### 可靠性带来可量化的写入和存储成本

每条真实新增指标会写入原始分区；每个 task/flush 额外生成一条事件和一条 Outbox。Outbox relay 与 API bridge 可能重复传输事件，消费者按事件序号去重。7 天事件 payload 会占用额外磁盘，容量评估应使用模拟器的实际压缩后行宽测量，并在压测报告中记录每日增长量。

### 保留清理会改变超过 7 天的查询与续传结果

原始指标和事件严格保留 168 小时。清理后，历史与摘要只反映窗口内数据；任务窗口内无指标时返回 404；落在窗口外的 SSE 游标返回 400。客户端应把 400 视为重新加载历史快照并建立新实时基线的信号。

### 数据库迁移采用前向版本并由独立服务串行执行

Compose 中的 `migrate` 一次性服务先完成 schema 版本升级，API 与 worker 的 `depends_on` 健康条件随后放行。应用启动时校验 schema 版本。每个迁移提供可审阅的向前 SQL；生产回退使用旧镜像兼容新 schema 的方式完成，破坏性 schema 收缩安排在独立后续版本。

## Implementation / Transition / 实现与过渡

### 六个阶段各自形成可验收的闭环

1. **骨架与契约**：建立 Go module、三个入口、配置、日志、Gin 错误中间件、迁移服务、Compose 和单元测试命令。门槛为三个二进制可编译，`healthz/readyz` 行为通过测试。
2. **可靠接入**：实现 RabbitMQ 拓扑、publisher 工作池、confirm、mandatory return 和连接恢复。门槛为 broker 重启与 unroutable 测试都返回预期结果。
3. **幂等落库**：实现 delivery tracker、双阈值 batcher、单事务 CTE、分区维护和 audit。门槛为 2% 重复注入、事务失败及 worker `kill -9` 后对账通过。
4. **查询与摘要**：实现交集过滤、按 key 计数、时间桶与原始分区聚合。门槛为边界语义契约测试以及 N4/N5 压测达标。
5. **事件与 SSE**：实现 Outbox relay、API event bridge、Hub、补发握手、心跳和慢订阅者退出。门槛为断线补发、API 重启、重复广播去重和 goroutine 泄漏测试通过。
6. **验收工具与收口**：完成模拟器、故障注入、结构化报告、7 天清理演练和完整 E2E。门槛为 N1–N6 全部生成可复现报告。

### 测试层次直接覆盖每个故障边界

| 层次 | 重点用例 | 工具 |
| --- | --- | --- |
| 单元 | 整批校验、消息展开、bucket 计算、delivery tracker、SSE 游标 | `testing`、`testify` |
| PostgreSQL 集成 | 并发幂等、事务回滚、任务序号、摘要聚合、分区裁剪 | Compose PostgreSQL |
| RabbitMQ 集成 | confirm、return、重连、manual ack、DLQ、重复广播 | Compose RabbitMQ |
| HTTP 契约 | 五个 endpoint、错误包络、空结果、404、SSE 格式 | `httptest` |
| E2E | 提交到查询、摘要、SSE、重连补发、kill -9 恢复 | `tests/e2e` + Compose |
| 性能 | 50 任务、500 采样点/秒、10 分钟、N4/N5/N6 | `cmd/sim` 与压测脚本 |

CI 的基础命令为：

```powershell
go test -race -cover ./...
golangci-lint run
docker compose up --build --wait
go run ./cmd/sim --tasks 50 --rate 10 --duration 10m --duplicate-rate 0.02 --audit
```

E2E 使用唯一测试前缀创建 task ID，并逐个删除明确测试 task 的数据，满足重复执行要求。故障恢复测试通过 Compose 向 worker 发送 `SIGKILL`，重新启动同一服务后等待 audit 收敛。

### 性能结论以基线数据决定后续演进

首次完整压测必须记录 PostgreSQL 版本、RabbitMQ 队列类型、CPU/内存、连接池大小、分区数、平均 batch 行数、数据库提交 P95、Outbox 延迟、查询 `EXPLAIN (ANALYZE, BUFFERS)` 和最终磁盘增长。以下信号触发下一轮设计评审：

- N4 在索引和 SQL 调优后仍超过 200ms：评估预聚合历史表或 TimescaleDB。
- 单 relay 的 Outbox 积压持续超过 1 秒：评估按 task 分片的多 relay 与 API 重排缓冲。
- N5 在索引和 SQL 调优后仍超过 100ms：评估日级预聚合、分钟级预聚合或缓存。
- 事件日志的 7 天容量超过部署预算：评估 payload 压缩、事件合并上限与外部归档。

## Appendix / 附录

### 关键不变量可以直接转成代码断言和测试

1. API 只对 RabbitMQ 已确认的持久消息返回 `200`。
2. worker 只对 PostgreSQL 已提交 delivery 执行 ack。
3. 任何 delivery 的全部片段提交后才进入 ack 终态。
4. `metric_events` 和 `metric_outbox` 只来源于 `metric_points RETURNING` 的真实新增行。
5. 同一任务的已提交 `event_seq` 严格递增。
6. SSE 对每条连接只发送 `event_seq > lastSent` 的 metrics 事件。
7. 历史查询在 PostgreSQL 完成降采样，Go 进程只组装有界结果。
8. 任务摘要只统计保留窗口内的唯一指标行。

### v1 配置按职责分组并在启动时集中校验

| 组 | 代表配置 |
| --- | --- |
| 服务 | `SERVICE_NAME`、`INSTANCE_ID`、`HTTP_ADDR`、`ADMIN_ADDR`、`SHUTDOWN_TIMEOUT` |
| PostgreSQL | `DATABASE_URL`、`PG_MAX_CONNS`、`PG_MIN_CONNS`、`PG_QUERY_TIMEOUT` |
| RabbitMQ | `AMQP_URL`、`AMQP_PREFETCH`、`AMQP_PUBLISHERS`、`AMQP_CONFIRM_TIMEOUT` |
| 批处理 | `WORKER_BATCH_MAX=500`、`WORKER_FLUSH_INTERVAL=1s` |
| SSE | `SSE_HEARTBEAT_INTERVAL=15s`、`SSE_SUBSCRIBER_BUFFER=256` |
| 保留 | `RETENTION_WINDOW=168h`、`PARTITION_MAINTENANCE_INTERVAL=1h` |
| 模拟器 | tasks、rate、duration、batch size、duplicate rate、disconnect rate、metric switches |

`RETENTION_WINDOW`、SSE ID 格式、API 字段和错误码属于 v1 语义配置，生产环境固定使用本文值。吞吐类配置可以通过压测调整，并将最终值记录在部署清单中。

### 后续扩展各自拥有明确触发条件

`duplicate_steps` 需要 API 契约版本评审；step 分桶需要新增响应字段并协调前端；长期归档需要先确定恢复时限与成本目标；Redis Streams 和 TimescaleDB 需要由压测数据证明当前组件达到瓶颈。v1 保持当前边界，所有扩展通过独立设计提案进入。
