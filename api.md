# Metrics Pipeline API 文档

本文档以 [任务书](docs/m3-task-go.md)、路由实现和接口测试为依据，描述当前 `metrics-pipeline` 服务的 HTTP 与 SSE 对外契约。

## 1. 服务地址

本地 Docker Compose 默认暴露以下地址：

| 服务 | 地址 | 用途 |
| --- | --- | --- |
| 业务 API | `http://localhost:8080` | 指标接收、查询、SSE 和审计 |
| API 管理端点 | `http://localhost:8081` | API 进程存活与依赖就绪检查 |
| Worker 管理端点 | `http://localhost:8091` | Worker 进程存活与依赖就绪检查 |

业务 API 路径统一以 `/api/v1` 为前缀。当前版本面向内网调用，无鉴权机制。

## 2. 公共约定

### 2.1 数据格式

- 请求与普通响应采用 JSON，编码为 UTF-8。
- 时间戳字段使用 Unix 毫秒，时区语义为 UTC。
- `step` 为 `0` 至 `2147483647` 的整数。
- `task_id` 长度为 `1` 至 `64`，格式为 `^[A-Za-z0-9][A-Za-z0-9._-]*$`。
- 指标 key 长度为 `1` 至 `32`，格式为 `^[A-Za-z0-9._-]+$`。
- 指标值为有限 JSON 数值。
- 任务书约定的常用 key 包括 `loss`、`lr`、`eval_loss`、`gpu_util`、`gpu_mem` 与 `throughput`；服务接受符合 key 格式的自定义指标。

### 2.2 错误响应

业务 API 的错误响应使用统一结构：

```json
{
  "error": {
    "code": "INVALID_PARAMS",
    "message": "human-readable description"
  }
}
```

| HTTP 状态 | `error.code` | 适用场景 |
| --- | --- | --- |
| `400` | `INVALID_PARAMS` | 路径、查询参数、请求体、字段值或 SSE 游标校验失败 |
| `404` | `TASK_NOT_FOUND` | 保留窗口内不存在该任务的指标数据 |
| `503` | `MQ_UNAVAILABLE` | 指标接收时 RabbitMQ 发布确认失败 |
| `500` | `INTERNAL` | 存储或服务内部异常 |

健康检查端点使用独立的状态响应格式，见第 8 节。

### 2.3 数据保留与一致性

- 指标点与 SSE 事件保留最近 168 小时。
- 指标接收接口在 RabbitMQ 确认接收后返回成功；Worker 随后异步批量持久化到 PostgreSQL。
- 指标点的幂等键为 `(task_id, key, step, ts)`。相同键的重复上报由存储层吸收。
- `accepted` 表示已被接收进入消息链路的采样条数，重复数据仍计入该值。

## 3. 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/v1/ingest/metrics` | 批量接收训练指标 |
| `GET` | `/api/v1/tasks/{task_id}/metrics` | 查询历史指标曲线并按需降采样 |
| `GET` | `/api/v1/tasks/{task_id}/summary` | 查询任务指标摘要 |
| `GET` | `/api/v1/tasks/{task_id}/metrics/stream` | 订阅任务指标 SSE 流 |
| `GET` | `/api/v1/admin/tasks/{task_id}/audit` | 查询任务数据对账结果 |
| `GET` | `/healthz` | 进程存活检查，管理端口 |
| `GET` | `/readyz` | PostgreSQL 与 RabbitMQ 就绪检查，管理端口 |

## 4. 批量接收指标

### `POST /api/v1/ingest/metrics`

请求头：`Content-Type: application/json`。服务接受带 `charset` 参数的 JSON Content-Type。

请求体上限为 `1 MiB`，请求体包含一个 JSON 对象，字段集合为 `task_id` 与 `batch`。

```json
{
  "task_id": "ft-20260825-0001",
  "batch": [
    {
      "step": 120,
      "ts": 1756089600123,
      "metrics": {
        "loss": 1.234,
        "lr": 0.00003
      }
    },
    {
      "step": 121,
      "ts": 1756089601123,
      "metrics": {
        "loss": 1.198,
        "lr": 0.00003
      }
    }
  ]
}
```

字段说明：

| 字段 | 类型 | 必填 | 约束 | 说明 |
| --- | --- | --- | --- | --- |
| `task_id` | string | 是 | 遵循公共 `task_id` 规则 | 训练任务标识 |
| `batch` | array | 是 | `1` 至 `500` 个采样点 | 同一任务的一组采样 |
| `batch[].step` | integer | 是 | `0` 至 `2147483647` | 训练步数 |
| `batch[].ts` | integer | 是 | 正 Unix 毫秒，最大 `253402300799999` | 采样时刻 |
| `batch[].metrics` | object | 是 | 至少包含一个 key | 指标 key 到数值的映射 |
| `batch[].metrics.{key}` | number | 是 | 有限数值，key 遵循公共规则 | 指标值 |

任一字段校验失败时，服务以 `400 INVALID_PARAMS` 拒绝整批数据。

成功响应：`200 OK`

```json
{
  "accepted": 2,
  "task_id": "ft-20260825-0001"
}
```

| 状态 | 含义 |
| --- | --- |
| `200` | 批次已通过校验并得到 RabbitMQ 发布确认 |
| `400` | Content-Type、JSON、字段名、字段类型、字段范围或批次限制校验失败 |
| `503` | RabbitMQ 在发布重试后仍无法确认消息 |

调用示例：

```bash
curl --request POST 'http://localhost:8080/api/v1/ingest/metrics' \
  --header 'Content-Type: application/json' \
  --data '{"task_id":"ft-20260825-0001","batch":[{"step":120,"ts":1756089600123,"metrics":{"loss":1.234,"lr":0.00003}}]}'
```

## 5. 查询历史指标

### `GET /api/v1/tasks/{task_id}/metrics`

服务按任务、指标 key、时间范围和 step 范围读取保留窗口内的数据。时间与 step 过滤同时生效，结果为各条件的交集。

查询参数：

| 参数 | 类型 | 必填 | 默认值 | 约束与语义 |
| --- | --- | --- | --- | --- |
| `keys` | string | 否 | 所有 key | 逗号分隔的指标 key。重复 key 自动合并。|
| `from` | integer | 否 | 保留窗口起点 | Unix 毫秒，时间区间下界，包含该值。|
| `to` | integer | 否 | 当前可查询数据 | Unix 毫秒，时间区间上界，排除该值。|
| `step_from` | integer | 否 | 最小 step | 下界，包含该值。|
| `step_to` | integer | 否 | 最大 step | 上界，包含该值。|
| `max_points` | integer | 否 | `500` | 每条指标曲线的点数上限，范围为 `1` 至 `5000`。|

参数组合满足 `from < to` 与 `step_from <= step_to`。时间参数为正 Unix 毫秒，`keys` 提供时至少包含一个合法 key。

当某条曲线在筛选后的点数大于 `max_points` 时，PostgreSQL 按时间桶聚合该曲线。聚合点的 `v` 是桶内平均值，`min` 与 `max` 保留桶内极值，`step` 取桶内最早时间点的 step，`ts` 取桶内最早时间。未聚合的原始点中 `v`、`min`、`max` 值相同。

成功响应：`200 OK`

```json
{
  "task_id": "ft-20260825-0001",
  "downsampled": true,
  "bucket_ms": 57600,
  "series": {
    "loss": [
      {
        "step": 12,
        "ts": 1756089600123,
        "v": 1.23,
        "min": 1.1,
        "max": 1.4
      }
    ],
    "lr": [
      {
        "step": 12,
        "ts": 1756089600123,
        "v": 0.00003,
        "min": 0.00003,
        "max": 0.00003
      }
    ]
  }
}
```

| 字段 | 说明 |
| --- | --- |
| `downsampled` | 至少一条返回曲线使用时间桶聚合时为 `true`。|
| `bucket_ms` | 当前响应使用的时间桶宽度，原始点响应为 `0`。|
| `series` | key 为指标名，value 为时间升序点数组。筛选结果为空的指标不出现在对象中。|
| `series.{key}[].v` | 原始点值或聚合桶平均值。|

任务在保留窗口内存在、筛选条件未命中任何点时，服务返回 `200` 和空 `series`。保留窗口内未找到该任务时，服务返回 `404 TASK_NOT_FOUND`。

调用示例：

```bash
curl 'http://localhost:8080/api/v1/tasks/ft-20260825-0001/metrics?keys=loss,lr&from=1756089600000&to=1756093200000&step_from=0&step_to=1000&max_points=500'
```

## 6. 查询任务摘要

### `GET /api/v1/tasks/{task_id}/summary`

服务聚合任务在 168 小时保留窗口内的所有指标。`last_step` 与 `updated_at` 来自时间最新的指标点；时间相同时使用较大的 step。

成功响应：`200 OK`

```json
{
  "task_id": "ft-20260825-0001",
  "last_step": 1234,
  "updated_at": 1756099812345,
  "metrics": {
    "loss": {
      "last": 0.87,
      "min": 0.79,
      "max": 2.31,
      "avg": 1.42
    },
    "lr": {
      "last": 0.000029,
      "min": 0,
      "max": 0.00005,
      "avg": 0.000031
    }
  }
}
```

| 字段 | 说明 |
| --- | --- |
| `last_step` | 最近指标点的训练步数。|
| `updated_at` | 最近指标点的 Unix 毫秒时间戳。|
| `metrics.{key}.last` | 该 key 按 `ts DESC, step DESC` 排序后的最新值。|
| `metrics.{key}.min` / `max` / `avg` | 该 key 在保留窗口内的最小值、最大值与平均值。|

无有效任务数据时返回 `404 TASK_NOT_FOUND`；非法 `task_id` 返回 `400 INVALID_PARAMS`。

## 7. 实时指标流

### `GET /api/v1/tasks/{task_id}/metrics/stream`

该接口返回 Server-Sent Events 长连接。服务设置以下响应头：

```text
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
```

请求头：

| 请求头 | 必填 | 格式 | 语义 |
| --- | --- | --- | --- |
| `Last-Event-ID` | 否 | `{task_id}:{event_seq}`，如 `ft-20260825-0001:128` | 续传游标。`event_seq` 为大于等于 `0` 的整数。|

未提供 `Last-Event-ID` 时，连接从建立后的新事件开始推送。服务为该连接注册订阅后读取当前事件高水位，连接建立期间产生的事件也会按序进入流。

提供有效游标时，服务按事件序号补发该游标之后、保留窗口内的事件，再继续推送实时事件。游标必须属于请求路径中的任务。当保留窗口内存在事件时，游标最小值为最早保留事件序号减一；更早的游标返回 `400 INVALID_PARAMS`。保留窗口已清空且任务序号仍存在时，游标需大于等于该任务的最新序号。大于最新序号的游标可建立连接，服务从后续更大的事件序号开始推送。

任务尚未产生数据时，服务保持 SSE 连接，后续落库产生的指标事件会进入该连接。

指标事件示例：

```text
event: metrics
id: ft-20260825-0001:128
data: {"points":[{"loss":1.234,"lr":0.00003,"step":120,"ts":1756089600123}]}

```

| 字段 | 说明 |
| --- | --- |
| `event` | 指标事件固定为 `metrics`。|
| `id` | `{task_id}:{event_seq}`；序号以成功写入的任务级批次递增。|
| `data.points` | 本次成功落库的一个或多个采样点。每个点包含 `step`、`ts` 及扁平化的指标键值。|

重复指标未产生新的持久化记录时，该批次不会产生 `metrics` 事件。

服务每 15 秒发送一次心跳：

```text
event: ping
data: {"ts":1756089615000}

```

调用示例：

```bash
curl --no-buffer \
  --header 'Accept: text/event-stream' \
  --header 'Last-Event-ID: ft-20260825-0001:128' \
  'http://localhost:8080/api/v1/tasks/ft-20260825-0001/metrics/stream'
```

## 8. 任务数据审计

### `GET /api/v1/admin/tasks/{task_id}/audit`

该接口用于模拟器对账与验收。统计范围为该任务在 `metric_points` 中的全部当前存储点。

成功响应：`200 OK`

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

| 字段 | 说明 |
| --- | --- |
| `point_count` | 去重后持久化的指标点数量；每个 `(task_id, key, step, ts)` 组合对应一个点。|
| `distinct_steps` | 跨全部 key 去重后的 step 数量。|
| `first_step` / `last_step` | 该任务的最小与最大 step。|
| `keys` | 指标 key 列表，字典序排列。|
| `missing_steps` | `first_step` 至 `last_step` 区间内的缺失 step。step 区间超过 `100000` 时返回空数组。|

无有效任务数据时返回 `404 TASK_NOT_FOUND`；非法 `task_id` 返回 `400 INVALID_PARAMS`。

## 9. 健康检查

健康检查运行在管理端口。API 默认端口为 `8081`，Worker 默认端口为 `8091`。

### `GET /healthz`

用于进程存活检查。

成功响应：`200 OK`

```json
{"status":"ok"}
```

### `GET /readyz`

用于依赖就绪检查。服务依次探测 PostgreSQL 和 RabbitMQ，每项探测的超时时间为 2 秒。

成功响应：`200 OK`

```json
{
  "status": "ready",
  "checks": {
    "postgres": "ok",
    "rabbitmq": "ok"
  }
}
```

服务启动完成前返回 `503 Service Unavailable`：

```json
{"status":"not_ready"}
```

任一依赖探测失败时返回 `503 Service Unavailable`：

```json
{
  "status": "not_ready",
  "checks": {
    "postgres": "ok",
    "rabbitmq": "failed"
  }
}
```

调用示例：

```bash
curl 'http://localhost:8081/healthz'
curl 'http://localhost:8081/readyz'
```
