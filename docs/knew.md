# Metrics Pipeline 学习记录

记录日期：2026-08-28

本文记录本项目中 RabbitMQ 发布、消费、PostgreSQL flush 的学习过程。每个主题都按照“疑惑起因 -> 代码定位 -> 原理 -> 异常路径 -> 最终结论”整理，便于后续复习。

## 一、先建立完整数据链路

最初的主要疑惑来自三个概念混在了一起：API 收到的 batch、RabbitMQ 的 delivery、写数据库时的 flush batch。理解这三个边界后，整条链路就清晰了。

```text
HTTP 请求
  -> IngestMetricsRequest
  -> domain.MetricBatch
  -> RabbitMQMetricBatchPublisher
  -> metrics.exchange
  -> metrics.ingest.v1
  -> RabbitMQMetricConsumer
  -> IngestMessage
  -> []domain.MetricPoint
  -> consumer 内存 buffer
  -> MetricPointStore.Flush
  -> PostgreSQL 事务提交
  -> RabbitMQ Delivery ACK
```

三个 batch 的含义：

| 名称 | 边界 | 含义 |
| --- | --- | --- |
| HTTP `batch` | 一次 API 请求 | 多个采样点 `MetricSample` |
| MQ delivery | 一条 RabbitMQ 消息 | publisher 将一个 `MetricBatch` 封装成一条 `IngestMessage` |
| flush batch | consumer 内存缓冲 | 最多 500 个 `MetricPoint`，可以混合多个 delivery，也可以只包含一个 delivery 的一部分 |

最终结论：数据可靠性包含两段确认。API 等待 RabbitMQ publisher confirm；worker 等待 PostgreSQL 事务提交，然后对 delivery 执行 ACK。

## 二、Publisher 是怎样注入 Service 的

### 疑惑起因

看到下面代码时，最初的问题是：`NewRabbitMQMetricBatchPublisher` 是否创建一个 publisher，再交给 service 使用？

```go
metricPublisher, err := messaging.NewRabbitMQMetricBatchPublisher(...)
ingestService := ingestservice.NewService(metricPublisher)
```

### 代码定位与原理

`ingest.Service` 依赖 `Publisher` 接口：

```go
type Publisher interface {
    PublishMetricBatch(context.Context, domain.MetricBatch) error
}
```

`RabbitMQMetricBatchPublisher` 实现了这个接口，因此可以注入 `ingest.Service`。HTTP handler 调用：

```text
IngestMetrics
  -> service.IngestMetricBatch
  -> publisher.PublishMetricBatch
```

构造函数同时完成运行时初始化：

1. 填充默认配置并校验参数。
2. 创建 `jobs` 和 `done` channel。
3. 按 `Publishers` 数量创建 AMQP session。
4. 每个 session 声明 RabbitMQ 拓扑并开启 confirm 模式。
5. 每个 session 启动一个独占它的发布 worker goroutine。

### 最终结论

`NewRabbitMQMetricBatchPublisher` 同时承担对象构造和 RabbitMQ 发布资源初始化。创建成功后，API service 只通过 `Publisher` 接口提交指标批次。

## 三、PublisherConfig 的作用

### 疑惑起因

`PublisherConfig` 字段较多，容易把 publisher 并发数理解成消费者取消息数量。

### 字段含义

| 字段 | 作用 |
| --- | --- |
| `URL` | RabbitMQ 连接地址 |
| `Publishers` | API 进程内的发布 worker 数量 |
| `WriteTimeout` | AMQP 网络写入超时 |
| `ConfirmTimeout` | 发布后等待 Broker ACK/NACK 的最长时间 |
| `MaxAttempts` | 单条消息最多发布尝试次数 |
| `InitialBackoff` | 第一次重试前的等待时间 |
| `MaxBackoff` | 指数退避的最大等待时间 |
| `Topology` | Exchange、Queue、Routing Key、DLQ 定义 |
| `IDGenerator` | 生成 `message_id`，也方便测试注入固定 ID |
| `Clock` | 生成消息时间戳，也方便测试注入固定时间 |

### 发布 worker 数量

`Publishers=3` 表示 API 进程启动 3 个发布 goroutine。每个 goroutine 独占一个 AMQP session/Channel，最多并行处理 3 次“发布消息并等待 confirm”的流程。

它控制的是生产端写入 RabbitMQ 的并发度。消费端数量由 worker 进程和 consumer 配置控制。

### 最终结论

`PublisherConfig` 统一控制连接、并发、超时、重试和消息元数据生成。生产行为主要由前七个字段决定，`IDGenerator` 和 `Clock` 同时提升可测试性。

## 四、为什么使用 jobs channel 和发布 worker

### 疑惑起因

API 已经拿到了 `MetricBatch`，直觉上可以在 HTTP goroutine 中直接调用 AMQP publish。当前实现先把任务放入 `p.jobs`，再由 worker 发布。

### 代码组织

每个发布任务包含：

```go
type publishJob struct {
    ctx    context.Context
    batch  domain.MetricBatch
    result chan error
}
```

`PublishMetricBatch` 中的第一个 `select` 负责提交任务：

```go
select {
case <-ctx.Done():
    return ctx.Err()
case <-p.done:
    return ErrPublisherClosed
case p.jobs <- job:
}
```

它等待三个事件中的一个：请求取消、publisher 关闭、任务成功进入 `jobs`。

第二个 `select` 负责等待结果：

```go
select {
case <-ctx.Done():
    return ctx.Err()
case <-p.done:
    return ErrPublisherClosed
case err := <-result:
    return err
}
```

worker 持续监听 `p.jobs`：

```text
取 publishJob
  -> 编码消息
  -> 发布到 RabbitMQ
  -> 等待 return/confirm
  -> 必要时重连和重试
  -> 将最终 error 写入 job.result
```

### 这样设计解决的问题

1. 每个 AMQP Channel 由一个 worker 独占，confirm 与当前发布任务保持清晰对应。
2. `Publishers` 固定并发上限，HTTP 高峰期间维持可控的 MQ 压力。
3. `jobs` 提供有限缓冲，形成背压。
4. 重试、重连、重新声明拓扑集中在 publisher 内部。
5. HTTP 请求仍会等待 `job.result`，Broker ACK 后才返回成功。

### 最终结论

`jobs` 解耦 HTTP 请求 goroutine 与 AMQP Channel 所有权；`result` 将可靠发布结果送回对应请求。

## 五、RabbitMQ 拓扑和 ingest 命名

### ingest 的含义

`ingest` 表示数据接入：外部指标进入系统处理管道的第一步。

```text
POST /api/v1/ingest/metrics
  -> metrics.ingest.v1
  -> worker 后续处理
```

### 默认拓扑

```text
Exchange:    metrics.exchange
类型:        direct
Routing Key: metrics.ingest
Queue:       metrics.ingest.v1
```

路由关系：

```text
metrics.exchange
  -- binding key: metrics.ingest --> metrics.ingest.v1
```

publisher 发布时调用：

```go
PublishWithContext(
    ctx,
    "metrics.exchange",
    "metrics.ingest",
    true,
    false,
    message,
)
```

`direct` Exchange 使用 routing key 精确匹配 binding key，因此消息进入 `metrics.ingest.v1`。

### declareTopology 做什么

每次 publisher/consumer 创建 session 或重连时都会声明：

1. 持久化死信 Exchange `metrics.dlx`。
2. 持久化死信 Queue `metrics.ingest.dlq`。
3. 将死信 Queue 绑定到死信 Exchange。
4. 持久化业务 Exchange `metrics.exchange`。
5. 持久化业务 Queue `metrics.ingest.v1`，配置死信去向。
6. 将业务 Queue 按 `metrics.ingest` 绑定到业务 Exchange。

RabbitMQ 会复用参数一致的已有资源。相同名称配合冲突参数时，Broker 返回声明错误并关闭当前 Channel。

### ExchangeDeclare 参数

```go
session.ExchangeDeclare(
    topology.IngestExchange,     // metrics.exchange
    topology.IngestExchangeKind, // direct
    true,                        // durable，Broker 重启后保留
    false,                       // autoDelete
    false,                       // internal，客户端可以直接发布
    false,                       // noWait，等待 Broker 返回声明结果
    nil,                         // 扩展参数
)
```

### 最终结论

publisher 只指定 Exchange 和 Routing Key；RabbitMQ 根据已声明的 binding 将消息放入 Queue。拓扑属于 Broker 中的共享资源，各 publisher worker 使用同一套路由。

## 六、publisher confirm、return 与 session

### Confirm(false) 的疑惑

`client.Confirm(false)` 中，`Confirm` 方法开启当前 AMQP Channel 的 publisher confirm 模式；参数 `false` 表示 `noWait=false`，客户端等待 Broker 确认该模式已开启。

开启后，RabbitMQ 对后续发布发送：

```text
basic.ack  -> 发布确认成功
basic.nack -> Broker 拒绝该次发布
```

### publisherSession 返回什么

```go
type publisherSession struct {
    client   amqpSession
    confirms <-chan amqp.Confirmation
    returns  <-chan amqp.Return
}
```

三个字段的职责：

| 字段 | 职责 |
| --- | --- |
| `client` | 声明拓扑、发布消息、关闭连接和 Channel |
| `confirms` | 接收 Broker 的 publish ACK/NACK |
| `returns` | 接收 mandatory 消息无法路由时的退回事件 |

注册代码：

```go
confirms: client.NotifyPublish(make(chan amqp.Confirmation, 1))
returns:  client.NotifyReturn(make(chan amqp.Return, 1))
```

### 谁向两个 channel 写数据

两个 channel 位于 Go 进程内存中。RabbitMQ 通过网络发送 AMQP 协议帧，`amqp091-go` 的网络读取与分发逻辑将事件写入 channel：

```text
RabbitMQ basic.ack/basic.nack
  -> amqp091-go
  -> confirms chan

RabbitMQ basic.return
  -> amqp091-go
  -> returns chan
```

单元测试中的 fake session 会手动写入 channel，用来模拟 Broker 行为：

```go
session.confirmReceiver <- amqp.Confirmation{Ack: true}
```

### worker 如何判断结果

`publishOnce` 发布后监听：

```text
returns 收到消息       -> ErrUnroutable
confirms 收到 Ack=true -> 成功
confirms 收到 Ack=false-> ErrBrokerNack
等待超时               -> ErrConfirmTimeout
publisher 关闭          -> ErrPublisherClosed
```

调用 `PublishMetricBatch` 的 service 最终只接收一个 `error`：

```text
err == nil -> 可靠发布成功，HTTP 返回 200
err 有值   -> 发布失败或重试耗尽，HTTP 返回 503 MQ_UNAVAILABLE
```

### 最终结论

`confirms` 和 `returns` 是 publisher worker 内部的协议事件入口；`job.result` 是 publisher 对 API service 的业务结果出口。

## 七、消息发布的完整数据流

HTTP JSON 经校验后转换为 `domain.MetricBatch`：

```json
{
  "task_id": "task-1",
  "batch": [
    {
      "step": 10,
      "ts": 1720000000000,
      "metrics": {"loss": 1.2}
    }
  ]
}
```

publisher 生成 `IngestMessage`：

```json
{
  "schema_version": 1,
  "message_id": "随机ID",
  "correlation_id": "与message_id相同",
  "task_id": "task-1",
  "batch": [
    {
      "step": 10,
      "ts": 1720000000000,
      "metrics": {"loss": 1.2}
    }
  ]
}
```

AMQP 属性还包含 `ContentType=application/json`、持久化投递模式、UTC 时间戳和协议 Headers。

发布失败时，worker 关闭当前 session，重新连接、声明拓扑、开启 confirm、注册通知，然后按指数退避重试。同一次重试复用同一个 `message_id` 和 JSON body，消费者可通过数据库唯一键吸收重复投递。

## 八、Consumer 如何消费消息

### 启动和会话

worker 创建 PostgreSQL store，再将其注入 RabbitMQ consumer：

```go
store, _ := postgres.NewMetricPointStore(pool)
consumer, _ := messaging.NewRabbitMQMetricConsumer(config, store, logger)
consumer.Run(ctx)
```

consumer 的 `openSession` 完成：

```text
连接 RabbitMQ
  -> 声明拓扑
  -> Qos(Prefetch=16)
  -> Consume(metrics.ingest.v1, autoAck=false)
  -> 获得 deliveries <-chan amqp.Delivery
```

`Prefetch=16` 表示 Broker 最多向当前 consumer 交付 16 条尚未 ACK 的 delivery。`autoAck=false` 开启手动确认。

### Delivery 的处理

每次从 `deliveries` 取出一个 `amqp.Delivery`，代码执行：

```text
校验 ContentType、schema_version、message_id、correlation_id
  -> DecodeIngestMessage
  -> ExpandMetricPoints
  -> 放入内存 buffer
```

协议错误执行：

```go
delivery.Nack(false, false)
```

`requeue=false` 让消息进入 `metrics.ingest.dlq`。

### 最终结论

RabbitMQ delivery 是消费确认的基本单位；数据库 flush 使用展开后的 `MetricPoint` 作为批处理单位。

## 九、MetricBatch、MetricSample 与 MetricPoint

领域模型定义：

```go
type MetricBatch struct {
    TaskID  string
    Samples []MetricSample
}

type MetricSample struct {
    Step            int64
    TimestampMillis int64
    Metrics         map[string]float64
}

type MetricPoint struct {
    TaskID          string
    Key             string
    Step            int64
    TimestampMillis int64
    Value           float64
}
```

一个 sample：

```go
Metrics: map[string]float64{
    "loss": 1.2,
    "accuracy": 0.95,
}
```

会展开为两行 `MetricPoint`：

```text
task-1 | loss     | step=10 | ts=... | 1.2
task-1 | accuracy | step=10 | ts=... | 0.95
```

最终结论：`MetricBatch` 面向请求和消息，`MetricPoint` 面向数据库行与 flush 计数。

## 十、Flush 流程与 delivery 生命周期

### Flush 触发条件

consumer 的 buffer 保存 `bufferedPoint`：

```go
type bufferedPoint struct {
    point    domain.MetricPoint
    delivery *deliveryState
}
```

满足任一条件会触发 flush：

```text
累计 500 个 MetricPoint
第一行进入 buffer 后经过 1 秒
worker 开始优雅关闭
```

### 为什么每个 point 要保存 deliveryState

一个 delivery 可以展开为 600 个点，跨越两个 flush；一个 flush 也可以混合多个 delivery。`deliveryState` 负责追踪来源和完成度：

```go
type deliveryState struct {
    delivery      amqp.Delivery
    totalRows     int
    committedRows int
    terminal      bool
}
```

字段含义：

| 字段 | 含义 |
| --- | --- |
| `delivery` | 原始 RabbitMQ 消息及 DeliveryTag |
| `totalRows` | 该消息展开后的 MetricPoint 总数 |
| `committedRows` | 已经由成功 flush 安全处理的点数 |
| `terminal` | 已经 ACK 或 NACK，防止重复终结 |

### 具体例子：batch1 600 点，batch2 100 点

收到 batch1 时创建 `state1(totalRows=600)`。

```text
第一次 flush：batch1 前 500 点
state1.committedRows = 500
500 < 600，保留未确认状态
```

batch1 剩余 100 点留在新 buffer。收到 batch2 后创建 `state2(totalRows=100)`，其 100 点也进入同一个 buffer。

```text
第二次 flush 共 200 点：
state1 对应 100 点
state2 对应 100 点
```

`committedRowsByDelivery` 使用 `*deliveryState` 作为 map key，分别统计：

```go
map[*deliveryState]int{
    state1: 100,
    state2: 100,
}
```

事务提交后：

```text
state1: 500 + 100 = 600，满足 totalRows，ACK batch1
state2:   0 + 100 = 100，满足 totalRows，ACK batch2
```

一条 delivery 的成功消费判断：

```go
state.committedRows == state.totalRows
```

### Flush 失败

数据库 flush 失败后，本次 flush 涉及的所有 delivery 都执行：

```go
delivery.Nack(false, true)
```

`requeue=true` 让整条消息重新进入队列。已经提交过的部分可能再次到达，数据库通过唯一键和 `ON CONFLICT DO NOTHING` 吸收重复数据。

### 最终结论

`bufferedPoint.delivery` 保存每一行的来源，`deliveryState` 跨多个 flush 累计完成度。数据库事务提交是增加 `committedRows` 的前提，全部完成是 MQ ACK 的前提。

## 十一、PostgreSQL Store 的事务闭环

`MetricPointStore.Flush` 的执行步骤：

```text
[]MetricPoint
  -> encodeMetricPoints 转成五组数组
  -> BeginTx(ReadCommitted)
  -> 确保时间分区存在
  -> seed_task_event_counters.sql
  -> persist_metric_flush.sql
  -> 读取生成的事件
  -> Commit
  -> 返回 nil
```

五组数组：

```text
taskIDs[]
keys[]
steps[]
timestamps[]
values[]
```

`Flush` 返回 `nil` 表示事务已经提交。consumer 随后增加 `committedRows` 并判断 ACK。

如果 SQL、扫描结果或 Commit 失败，defer 中的 Rollback 负责回滚事务，consumer 随后 NACK 并重新入队。

## 十二、PostgreSQL 基础语法学习

### AS 的含义

`AS` 的核心作用是命名。

```sql
SELECT task_id AS id
```

给结果列命名为 `id`。

```sql
FROM task_event_counters AS counters
```

给表起短名 `counters`，后续可以写 `counters.task_id`。

```sql
WITH input AS (
    SELECT ...
)
```

给括号里的查询结果命名为 `input`。

```sql
FROM unnest(...) AS points(task_id, key, step, ts, value)
```

给 `unnest` 的结果表命名为 `points`，同时给结果列命名。

常见模式：

```text
列 AS 新列名
表 AS 短表名
结果 AS 表名(列名...)
CTE名称 AS (一段查询)
```

### WITH 和 CTE

```sql
WITH input AS (...),
     inserted AS (...)
SELECT ...;
```

`WITH` 定义一组按名称引用的中间结果，称为 CTE。可以类比为：

```go
input := 生成输入行()
inserted := 插入并返回新增行(input)
```

逗号表示后面还有下一个 CTE，最后一个 CTE 后面接主查询。

### 参数、类型转换和数组

```sql
$1::varchar[]
```

含义：

```text
$1          第一个 SQL 参数
::          PostgreSQL 类型转换符
varchar[]   字符串数组
```

其他类型：

```text
integer[]          整数数组
timestamptz[]      带时区时间数组
double precision[] 双精度浮点数组
```

### unnest

`unnest` 将多组数组按相同下标展开成行：

```text
$1 = [task-1, task-1]
$2 = [loss, accuracy]
$3 = [10, 10]
$4 = [时间A, 时间A]
$5 = [1.2, 0.95]
```

展开后：

```text
task-1 | loss     | 10 | 时间A | 1.2
task-1 | accuracy | 10 | 时间A | 0.95
```

## 十三、seed_task_event_counters.sql

SQL：

```sql
INSERT INTO task_event_counters (task_id)
SELECT DISTINCT task_id
FROM unnest($1::varchar[]) AS tasks(task_id)
ON CONFLICT (task_id) DO NOTHING;
```

执行过程：

1. `$1` 接收本次 flush 涉及的任务 ID 数组。
2. `unnest` 将数组展开成多行。
3. `DISTINCT` 去除重复任务 ID。
4. 将任务 ID 插入 `task_event_counters`。
5. 已有任务通过 `ON CONFLICT DO NOTHING` 保留现有计数器。

用途：确保后续 SQL 可以锁定并递增每个任务的 `last_event_seq`。

## 十四、persist_metric_flush.sql 逐模块理解

### 1. input：数组转行

```sql
WITH input AS (
    SELECT task_id, key, step, ts, value
    FROM unnest(
        $1::varchar[],
        $2::varchar[],
        $3::integer[],
        $4::timestamptz[],
        $5::double precision[]
    ) AS points(task_id, key, step, ts, value)
)
```

作用：将 Go 传入的五组列式数组按下标组合成多行 `MetricPoint` 数据。

### 2. inserted：幂等插入并返回新增行

```sql
inserted AS (
    INSERT INTO metric_points (task_id, key, step, ts, value)
    SELECT task_id, key, step, ts, value
    FROM input
    ON CONFLICT (task_id, key, step, ts) DO NOTHING
    RETURNING task_id, key, step, ts, value
)
```

`ON CONFLICT` 依赖 `(task_id, key, step, ts)` 唯一约束。重复指标被跳过。

`RETURNING` 返回本次真正新增的行。因此：

```text
input 有 3 行
1 行发生唯一键冲突
成功新增 2 行
inserted 中有 2 行
```

确认结论：`inserted` 是由 `metric_points` 成功新增并经 `RETURNING` 返回的行组成的 CTE 中间表。

### 3. point_payloads：同一采样点合成 JSON

按 `task_id + step + ts` 分组，将多条指标聚合成：

```json
{
  "accuracy": 0.95,
  "loss": 1.2,
  "step": 10,
  "ts": 1720000000000
}
```

`jsonb_object_agg` 将多个 `key/value` 聚合成 JSON 对象；`jsonb_build_object` 添加 `step` 和毫秒时间戳。

### 4. task_payloads：同一任务合成事件

按 `task_id` 分组，将多个 point 聚合为：

```json
{
  "points": [
    {"loss": 1.2, "step": 10, "ts": 1720000000000},
    {"loss": 0.8, "step": 11, "ts": 1720000001000}
  ]
}
```

### 5. locked_counters：锁定任务计数器

```sql
FOR UPDATE OF counters
```

它为涉及的 `task_event_counters` 行加行锁。同一任务的并发 flush 会依次获得锁，从而顺序分配事件序号。

`ORDER BY counters.task_id` 统一多任务加锁顺序，降低死锁风险。`MATERIALIZED` 固定保存这份中间结果。

### 6. sequenced：递增事件序号

```sql
SET last_event_seq = counters.last_event_seq + 1
```

每个产生了新增 payload 的任务递增一次序号，并通过 `RETURNING` 输出：

```text
task_id + event_seq + payload
```

### 7. events：写业务事件历史

将 `sequenced` 写入 `metric_events`。这张表保存已经生成的任务指标事件。

### 8. outbox：写可靠发布待办

将同一事件写入 `metric_outbox`。后续 Outbox relay 可以读取并广播事件。

### 9. 最终 SELECT

```sql
SELECT task_id, event_seq, payload
FROM outbox
ORDER BY task_id;
```

返回本次事务实际生成的事件，Go 代码读取这些结果，然后提交事务。

## 十五、问题到解决的完整闭环

### 发布端闭环

```text
问题：API 如何确认消息真的交给 RabbitMQ？
定位：Confirm(false) + NotifyPublish + publishOnce
机制：等待 Broker ACK/NACK，并监听 mandatory return
异常：超时、NACK、无路由时重建 session 并有界重试
结果：最终 error 回传 service，HTTP 映射为 200 或 503
```

### 消费端闭环

```text
问题：一条 delivery 跨多个 flush 时，何时 ACK？
定位：deliveryState + bufferedPoint.delivery
机制：每次成功事务按来源累计 committedRows
异常：flush 失败时 NACK(requeue=true)
结果：committedRows == totalRows 时 ACK 单条 delivery
```

### 数据库闭环

```text
问题：消息重投会造成重复指标和重复事件吗？
定位：metric_points 唯一键 + inserted CTE
机制：ON CONFLICT DO NOTHING，后续事件只读取 RETURNING 的新增行
异常：事务任一步骤失败则 Rollback
结果：重复数据被吸收，新增指标、事件、Outbox 在同一事务中提交
```

## 十六、需要长期记住的系统不变量

1. API 只在 publisher 收到可靠发布确认后返回成功。
2. worker 只在 PostgreSQL 事务提交后推进 delivery 的完成计数。
3. 一条 delivery 的全部 MetricPoint 安全处理后才 ACK。
4. 数据库唯一键吸收 RabbitMQ 重投造成的重复指标。
5. 事件只来源于 `INSERT ... RETURNING` 返回的真实新增指标。
6. 协议错误进入 DLQ；数据库瞬时失败重新入队。
7. publisher worker 独占发布 Channel；consumer 循环拥有消费 Channel 的 ACK/NACK 操作。

## 十七、代码索引

- API 装配：[internal/app/api/run.go](../internal/app/api/run.go)
- HTTP ingest：[internal/transport/http/ingest.go](../internal/transport/http/ingest.go)
- ingest service：[internal/service/ingest/service.go](../internal/service/ingest/service.go)
- 领域模型：[internal/domain/metrics.go](../internal/domain/metrics.go)
- 消息协议：[internal/messaging/message.go](../internal/messaging/message.go)
- RabbitMQ publisher：[internal/messaging/publisher.go](../internal/messaging/publisher.go)
- RabbitMQ topology：[internal/messaging/topology.go](../internal/messaging/topology.go)
- RabbitMQ consumer：[internal/messaging/consumer.go](../internal/messaging/consumer.go)
- worker 装配：[internal/app/worker/run.go](../internal/app/worker/run.go)
- PostgreSQL store：[internal/storage/postgres/metric_point_store.go](../internal/storage/postgres/metric_point_store.go)
- 计数器初始化 SQL：[internal/storage/postgres/sql/seed_task_event_counters.sql](../internal/storage/postgres/sql/seed_task_event_counters.sql)
- flush SQL：[internal/storage/postgres/sql/persist_metric_flush.sql](../internal/storage/postgres/sql/persist_metric_flush.sql)

## 十八、复习自测

1. `Publishers=3` 控制哪一端的并发？
2. `Confirm(false)` 中的方法和参数分别表达什么？
3. `confirms`、`returns`、`job.result` 分别连接哪两个组件？
4. `direct` Exchange 如何把消息路由到 `metrics.ingest.v1`？
5. 一个 delivery 展开 600 行时，为什么第一次 500 行提交后仍然保持未 ACK？
6. 一个 flush 混合两个 delivery 时，代码如何分别累计完成行数？
7. `WITH input AS (...)` 中的 `input` 是什么？
8. `$1::varchar[]` 的三个组成部分分别是什么？
9. `inserted` 为什么只包含真正新增的指标行？
10. PostgreSQL Commit、RabbitMQ ACK、数据库幂等三者如何组成可靠消费闭环？
