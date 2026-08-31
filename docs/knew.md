# Metrics Pipeline 实现速查

记录日期：2026-08-28

本文记录指标接入、批量落库，以及 Issue 9、Issue 10 的实际实现和接口约定。

## 一、整体数据链路

```text
HTTP ingest
  -> IngestMetricsRequest
  -> domain.MetricBatch
  -> RabbitMQ publisher
  -> metrics.exchange / metrics.ingest.v1
  -> RabbitMQ consumer
  -> 展开为 []domain.MetricPoint
  -> 内存 buffer
  -> MetricPointStore.Flush
  -> PostgreSQL 事务提交
  -> RabbitMQ Delivery ACK
```

三个批次边界：

| 对象 | 含义 |
| --- | --- |
| HTTP batch | 一次请求中的多个 `MetricSample` |
| MQ delivery | publisher 封装后的一条 RabbitMQ 消息 |
| flush batch | consumer 缓存的最多 500 个 `MetricPoint`，可以来自多个 delivery |

发布端等待 RabbitMQ publisher confirm；消费端等待 PostgreSQL 事务提交后再 ACK。两段确认共同保证请求成功和消息落库之间的可靠性。

## 二、消费者与批量落库

### 1. 消息展开

一个采样点可以包含多个指标：

```json
{
  "step": 10,
  "ts": 1720000000000,
  "metrics": {"loss": 1.2, "accuracy": 0.95}
}
```

它会展开成两行 `MetricPoint`：

```text
task-1 | loss     | step=10 | ts=... | 1.2
task-1 | accuracy | step=10 | ts=... | 0.95
```

`MetricBatch` 面向 HTTP 和 MQ 消息；`MetricPoint` 面向数据库行和 flush 计数。

### 2. 消费和 flush

consumer 会连接 RabbitMQ、声明拓扑、设置 `Prefetch=16`，并以 `autoAck=false` 接收 delivery。协议错误执行 `Nack(requeue=false)`，消息进入 DLQ。

buffer 满 500 个点、首个点进入后经过 1 秒、worker 优雅关闭时，任一条件都能触发 flush。

```go
type bufferedPoint struct {
    point    domain.MetricPoint
    delivery *deliveryState
}

type deliveryState struct {
    delivery      amqp.Delivery
    totalRows     int
    committedRows int
    terminal      bool
}
```

一个 delivery 展开出的点可能跨越多个 flush，一个 flush 也可能混合多个 delivery。每次事务成功后，按 `deliveryState` 累加 `committedRows`；只有 `committedRows == totalRows` 才 ACK 该 delivery。

### 3. 事务、幂等和失败处理

`MetricPointStore.Flush` 在一个 PostgreSQL 事务内完成：

```text
编码列式数组
  -> 确保指标时间分区
  -> 初始化任务事件计数器
  -> 插入 metric_points
  -> 生成 metric_events 和 metric_outbox
  -> Commit
```

指标唯一键为 `(task_id, key, step, ts)`。插入使用 `ON CONFLICT DO NOTHING`，因此消息重投产生的重复指标会被吸收；事件只根据 `INSERT ... RETURNING` 返回的真实新增行生成。

事务提交失败或 flush 出错时，涉及的 delivery 执行 `Nack(requeue=true)`。重投可能再次处理已经提交的点，数据库唯一键保证落库幂等。

## 三、Issue 9：历史指标过滤查询

### 1. 接口

```http
GET /api/v1/tasks/{task_id}/metrics
```

参数约定：

| 参数 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `keys` | 否 | 全部 key | 逗号分隔，例如 `loss,lr` |
| `from` / `to` | 否 | 全量 | 毫秒时间戳，时间范围为左闭右开 `[from, to)` |
| `step_from` / `step_to` | 否 | 全量 | 步数范围，与时间范围取交集；当前 `step_to` 为闭区间 |
| `max_points` | 否 | 500 | 每个 key 的点数上限，超出时触发降采样；当前允许 `1..5000` |

参数错误返回 `400 INVALID_PARAMS`。

### 2. 查询条件和任务语义

SQL 从 `metric_points` 查询，固定先按 `task_id` 和最近 7 天窗口过滤，再叠加请求条件：

```text
task_id = task_id
ts >= 最近 7 天
key = ANY(keys)       （指定 keys 时）
ts >= from            （指定 from 时）
ts <  to              （指定 to 时）
step >= step_from    （指定 step_from 时）
step <= step_to      （指定 step_to 时）
```

“每条曲线”指 `series` 中的每个指标 key。例如 `loss` 是一条曲线，`lr` 是另一条曲线。过滤结果按 key 分组，每个 key 的数组就是该曲线的数据点。

任务存在性独立于 keys、时间和 step 过滤：最近 7 天内任务有过任意落库数据时，任务即存在。任务存在但过滤结果为空时返回 `200` 和空 `series`；最近 7 天内没有任务数据时返回 `404 TASK_NOT_FOUND`。

### 3. 响应结构

接口返回 JSON：

```json
{
  "task_id": "task-1",
  "downsampled": true,
  "bucket_ms": 3801,
  "series": {
    "loss": [
      {
        "step": 12,
        "ts": 1720000000000,
        "v": 1.23,
        "min": 1.10,
        "max": 1.40
      }
    ]
  }
}
```

`series.loss` 和 `series.lr` 分别代表两条曲线。每条曲线固定按 `(ts ASC, step ASC)` 返回。

原始点统一输出 `{step, ts, v, min, max}`，其中 `v=min=max=value`。降采样点的 `v` 是桶内平均值，`min`、`max` 是桶内极值。

## 四、Issue 10：SQL 时间降采样

### 1. 触发条件

降采样按 key 独立判断。先完成 Issue 9 的全部过滤，再统计每个 key 的点数：

```text
max_points = 500
loss 过滤后 700 点 -> 降采样
lr   过滤后 100 点 -> 原样返回
```

因此一次响应可以同时包含原始曲线和降采样曲线。只有至少一个 key 触发降采样时，`downsampled` 才为 `true`。

### 2. 时间桶算法

降采样始终按时间分桶。当前 SQL 先为每个超限 key 计算所需桶宽，再取其中最大的桶宽作为响应的 `bucket_ms`：

```text
单个 key 的候选桶宽 = ceil((key_max_ts - key_min_ts + 1ms) / max_points)
响应 bucket_ms      = 所有超限 key 候选桶宽的最大值
```

每个 key 仍以自己的最早时间 `key_min_ts` 作为分桶起点：

```text
bucket_no = floor((point.ts - key_min_ts) / bucket_ms)
桶 n      = [key_min_ts + n*bucket_ms,
             key_min_ts + (n+1)*bucket_ms)
```

`bucket_ms` 是时间桶宽，不代表每个桶一定有数据，也不承诺每条曲线恰好产生 `max_points` 个桶。使用 `ceil` 后，超限曲线的桶数量保持在点数上限附近；多 key 共用响应中的桶宽可以让元数据保持简单且稳定。

### 3. 桶内聚合

每个 `(key, bucket_no)` 聚合为一个响应点：

| 字段 | 取值 |
| --- | --- |
| `v` | `avg(value)` |
| `min` | `min(value)` |
| `max` | `max(value)` |
| `ts` | 桶内最早点的时间 |
| `step` | 按 `(ts, step)` 排序后的最早点的 step |

SQL 的主要 CTE 分工：

```text
filtered       应用任务、时间、key、step 过滤
stats          按 key 统计点数及时间范围
bucket_config  计算降采样所需 bucket_ms
raw_points     返回未超限 key 的原始点
sampled_points 返回超限 key 的桶聚合点
result_points  合并原始点和聚合点
meta           返回任务存在性和 bucket_ms
```

最后统一按 `(key, ts, step)` 排序。Go repository 负责扫描 SQL 结果并转换为响应模型，HTTP handler 负责按 key 组装 `series`。

## 五、关键不变量

1. publisher confirm 成功后，ingest API 才返回成功。
2. PostgreSQL Commit 成功后，consumer 才增加 delivery 的完成计数。
3. delivery 的全部 `MetricPoint` 完成后才 ACK。
4. 唯一键和 `ON CONFLICT DO NOTHING` 保证重复投递的落库幂等。
5. 协议错误进入 DLQ；数据库或网络瞬时错误重新入队。
6. 历史查询先过滤，再按 key 判断点数，最后只对超限 key 做时间降采样。
7. 原始点和降采样点使用同一响应格式，前端只需按 `series[key]` 绘制曲线。

## 六、代码索引

- API 装配：[internal/app/api/run.go](../internal/app/api/run.go)
- HTTP ingest：[internal/transport/http/ingest.go](../internal/transport/http/ingest.go)
- ingest service：[internal/service/ingest/service.go](../internal/service/ingest/service.go)
- 领域模型：[internal/domain/metrics.go](../internal/domain/metrics.go)
- 消息协议：[internal/messaging/message.go](../internal/messaging/message.go)
- RabbitMQ publisher：[internal/messaging/publisher.go](../internal/messaging/publisher.go)
- RabbitMQ consumer：[internal/messaging/consumer.go](../internal/messaging/consumer.go)
- worker 装配：[internal/app/worker/run.go](../internal/app/worker/run.go)
- 历史查询 handler：[internal/transport/http/history.go](../internal/transport/http/history.go)
- 历史查询 service：[internal/service/history/service.go](../internal/service/history/service.go)
- PostgreSQL store：[internal/storage/postgres/metric_point_store.go](../internal/storage/postgres/metric_point_store.go)
- 历史查询 SQL：[internal/storage/postgres/sql/query_metric_history.sql](../internal/storage/postgres/sql/query_metric_history.sql)
- 计数器初始化 SQL：[internal/storage/postgres/sql/seed_task_event_counters.sql](../internal/storage/postgres/sql/seed_task_event_counters.sql)
- flush SQL：[internal/storage/postgres/sql/persist_metric_flush.sql](../internal/storage/postgres/sql/persist_metric_flush.sql)
- 历史查询测试：[internal/transport/http/history_test.go](../internal/transport/http/history_test.go)
- 降采样集成测试：[internal/storage/postgres/metric_point_store_integration_test.go](../internal/storage/postgres/metric_point_store_integration_test.go)

## 七、验证命令

```powershell
go test ./...
go test -tags=integration ./internal/storage/postgres -run TestMetricPointStoreQueriesAndDownsamplesHistory -count=1
git diff --check
```
