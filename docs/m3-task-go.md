# TRAINING METRICS INGEST & QUERY SERVICE · GO EDITION

**训练指标采集与查询服务**

入职任务书

从指标管道切入 M3 训练域 ——交付一条完整可验收的数据链路。

Go 1.22+  ·  Gin  ·  PostgreSQL · pgx  ·  RabbitMQ · amqp091-go  ·  SSE  ·  docker-compose  ·  无需 GPU

| 任务周期<br>小于2 周 | 难度<br>★★★☆☆ | 导师 / 评审<br>M3 组导师（待填） | 答辩时间<br>30 分钟 |
| --- | --- | --- | --- |

| 目录 · CONTENTS | 目录 · CONTENTS |
| --- | --- |
| 01  任务背景 | 02  任务目标与范围 |
| 03  参考架构与技术栈（Go） | 04  功能需求（F1–F7） |
| 05  接口契约 | 06  数据模型与幂等设计 |
| 07  非功能需求与验收指标 |  |

# 01  任务背景

## 1.1 你要加入的团队

M3 是智算一体化平台的训练域，负责从数据集准备、微调任务提交、GPU 调度、训练监控到产出模型注册的完整链路。训练核心链路（K8s + Volcano 调度、LLaMA-Factory 训练进程）由正式工程师负责；本任务位于训练链路的观测侧，不碰核心调度路径，但产出会被正式产品直接使用。

## 1.2 任务在系统中的位置

训练中心的「监控」页需要实时展示每个微调任务的 loss 曲线、学习率变化。训练进程通过 callback 把指标吐出来，需要有一个后端服务负责接收 → 缓冲 → 落库 → 查询 / 实时推送。这个服务就是本任务要交付的东西（代号 metrics-pipeline）。

① 写入链路 —— 本任务 F1 / F2

| 指标生产者<br>LF Callback · 模拟器 | → | 指标接入 API<br>Gin · 校验 · 发 MQ | → | RabbitMQ<br>metrics.exchange | → | 消费者 worker<br>手动 ack · 批量幂等落库 | → | PostgreSQL<br>metric_points 分区表 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

② 读取链路 —— 本任务 F3 / F4 / F5

| PostgreSQL<br>分区表 · 降采样聚合 | → | 查询 / 摘要 / SSE API<br>Gin · 历史曲线 · 摘要 · 实时流 | → | 训练中心 · 监控页<br>前端既有原型 · 不在范围内 |
| --- | --- | --- | --- | --- |

## 1.3 为什么给你这道题

- 技术栈完全对齐：Go + Gin + pgx + RabbitMQ 就是 M3 后端日常开发栈，做完即完成技术预热；
- 管道型问题是 M3 的日常：训练域后端最重的活就是「状态与指标的数据管道」，这道题把 API → MQ → 批量落库 → 查询/推送 整条管道走了一遍；
- 有真实产品出口：交付合格的服务可直接并入训练中心监控页的后端，不是一次性作业；
- 风险隔离：不碰 K8s 调度、不碰 GPU、不碰 LLaMA-Factory 源码，出错不影响任何真实训练任务。
# 02  任务目标与范围

## 2.1 一句话目标

> 任务目标
> 交付一个可独立部署的 metrics-pipeline 服务（Go）：接收训练指标批量上报，经消息队列可靠落库，对外提供历史查询（自动降采样）、任务摘要与 SSE 实时推送，并通过 50 并发任务的压测与对账验收。

## 2.2 范围内 / 范围外

| ✅ 范围内（必须做） | ❌ 范围外（明确不做） |
| --- | --- |
| •  指标接入 API（批量上报、参数校验、写入 MQ）<br>•  MQ 消费者（手动 ack、批量聚合落库、幂等）<br>•  历史查询 API（时间/步数过滤、服务端降采样）<br>•  任务摘要 API（最新值 / min / max / avg）<br>•  SSE 实时推送流<br>•  数据模拟器（造数 + 对账，验收的核心工具）<br>•  docker-compose 一键起全链路 | •  任何 K8s / Volcano / GPU 相关工作<br>•  修改 LLaMA-Factory 源码（只模拟它的行为）<br>•  前端页面（loss 曲线已有原型，只需按契约对接）<br>•  用户体系 / 鉴权 / 多租户（内网服务，v1 无鉴权）<br>•  训练任务生命周期管理（任务注册在别的服务） |

## 2.3 指标 Key 约定

| key | 要求 | 说明 | 模拟器生成 |
| --- | --- | --- | --- |
| loss | 必做 | 训练损失 | ✓ 指数衰减 + 噪声 |
| lr | 必做 | 学习率 | ✓ warmup + 余弦衰减 |
| eval_loss | 建议 | 验证损失（稀疏，每 N step 一条） | ✓ 可选开关 |
| gpu_util / gpu_mem | 加分 | 资源利用率（%） | ✓ 随机游走 |
| throughput | 加分 | tokens/s | ✓ 可选开关 |

# 03  参考架构与技术栈（Go）

## 3.1 技术栈

| 组件 | 选型 | 说明 |
| --- | --- | --- |
| 语言 / 运行时 | Go 1.22+ | 与 M3 后端一致；统一 go module，禁用 GOPATH 模式 |
| Web 框架 | Gin v1.10 | 路由 / 中间件 / JSON 绑定；如团队允许，标准库 net/http + chi 亦可（需在设计文档说明） |
| PG 驱动 | jackc/pgx v5（pgxpool） | 批量写库用 pgx.Batch；不强制 ORM，GORM 可选但批量场景仍建议 pgx 直写 |
| MQ 客户端 | rabbitmq/amqp091-go | Channel 非并发安全——见第 12 节坑 2 |
| 缓存（可选） | redis/go-redis v9 | 仅用于 summary 加速 / SSE 扇出，不作为数据主存 |
| 日志 | log/slog（标准库） | 结构化 JSON 日志；禁止 fmt.Println 打日志 |
| 配置 | 环境变量 + godotenv | 见 11.3 节；禁止硬编码 |
| 部署 | docker-compose + 多阶段 Dockerfile | distroless / scratch 基础镜像为加分项 |
| 单测 | 标准 testing + testify | go test -cover 统计覆盖率；golangci-lint 静态检查 |

## 3.2 存储方案建议

> 基础方案（推荐，先跑通）
> PostgreSQL 原生分区表（按天 RANGE 分区）+ 查询侧等间隔桶降采样（每桶输出 min / max / avg）。实现简单，无额外依赖，性能足以通过验收。

> 进阶方案（设计文档中论证后可选）
> TimescaleDB hypertable + continuous aggregate；或自建预聚合表（消费时同步写 1min 聚合）。允许替换基础方案，但必须在设计文档中给出对比依据（写入吞吐、查询延迟、运维成本三个维度），否则按基础方案执行。

## 3.3 部署形态

- 单仓库双入口：同一个 go module 下 cmd/api 与 cmd/worker 各自 main，编译为两个二进制；也可共用同一镜像、不同启动命令；
- api 与 worker 必须可独立重启——验收演练需要单独 kill worker；
- 优雅退出：监听 SIGTERM，先停止接新数据、flush 缓冲、ack 完成后退出（加分项）；
- 配置全部走环境变量（见第 11.3 节），禁止硬编码。
# 04  功能需求（F1–F7）

### MUST   F1 · 指标接入 API

接收批量上报。每条消息含 task_id、step、时间戳与多指标键值。要求：参数校验（非法 batch 返回 400 且整批拒绝，不做部分接受；Gin 侧用 binding tag 或自定义校验器均可）；校验通过后写入 MQ 并立即返回（不直接写库）；写入失败（含重试后仍失败）返回 503。

### MUST   F2 · 消费者与批量落库

订阅队列，按条数或时间双阈值（如 500 点或 1 秒，先到先触发）聚合为批量 INSERT ... ON CONFLICT DO NOTHING（pgx.Batch 一次往返提交整批），成功后手动 ack；失败 nack 重回队列。消费者崩溃重启后不丢不重（第 7 节验收）。参考骨架：

```go
// 双阈值聚合：攒够 batchMax 点或满 1 秒，先到先触发
for {
    select {
    case p := <-pointsCh:        // 来自 amqp Delivery 的解析结果
        buf = append(buf, p)
        if len(buf) >= batchMax {
            flush(ctx, &buf)     // pgx.Batch + INSERT ... ON CONFLICT DO NOTHING
        }
    case <-ticker.C:             // time.NewTicker(1 * time.Second)
        if len(buf) > 0 {
            flush(ctx, &buf)
        }
    case <-ctx.Done():           // SIGTERM：优雅退出
        flush(ctx, &buf)
        return
    }
}
```

### MUST   F3 · 历史查询（服务端降采样）

按 task_id + keys + 时间/步数区间查询。当返回点数超过 max_points（默认 500）时服务端必须降采样——按等时间 / 等 step 桶聚合，每桶输出 min / max / avg 三值，保证曲线波形不丢尖峰。禁止把全量原始点直接塞给前端；降采样应在 SQL 层完成（GROUP BY 桶宽），不在应用层内存裁剪。

### MUST   F4 · SSE 实时推送

按 task_id 建立长连接，新指标落库后 1 秒内推送至订阅方；每 15 秒发送心跳；客户端断线重连可通过 Last-Event-ID 续传（续传为 SHOULD）。实时扇出路径（订阅 MQ vs 轮询 DB）自选，设计文档论证。Go 实现建议：Gin 的 c.Stream + c.SSEvent；推送循环必须监听 r.Context().Done()，客户端断开后立即退出，防止 goroutine 泄漏（见第 12 节）。

### MUST   F5 · 任务指标摘要

返回单任务各 key 的最新值、min / max / avg、最后 step 与最后更新时间。供工作台 KPI 卡与任务列表使用，要求 P95 < 100ms。

### MUST   F6 · 数据模拟器

验收的核心工具，Go CLI（建议 cmd/sim 独立入口，flag 标准库即可），规格见附录 A。能模拟 N 个任务的 loss/lr 曲线按目标速率上报，支持重复上报注入、断连注入与结束对账（输出 PASS/FAIL 报告）。模拟器写不好，验收无法进行，等同功能未完成。

### SHOULD   F7 · 健康检查与运维端点

/healthz（进程存活）与 /readyz（依赖检查：PG / MQ 可达）；对账端点 GET /api/v1/admin/tasks/{task_id}/audit 见第 5.5 节。

# 05  接口契约

> 契约不可自行变更
> 以下字段名、错误码、幂等语义在验收时逐条核对。若你认为契约有设计缺陷，可以提出来。

## 5.1 批量上报 · POST /api/v1/ingest/metrics

请求（Content-Type: application/json）：

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

## 5.2 历史查询 · GET /api/v1/tasks/{task_id}/metrics

| 参数 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| keys | 否 | 全部 key | 逗号分隔，如 loss,lr |
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

## 5.3 实时推送 · GET /api/v1/tasks/{task_id}/metrics/stream（SSE）

```text
event: metrics
id: ft-20260825-0001:00128
data: {"points":[{"step":120,"ts":1756089600123,"loss":1.234,"lr":3e-05}]}
 
event: ping
data: {"ts":1756089615000}
```

id 格式为 {task_id}:{递增序号}，用于 Last-Event-ID 续传；ping 为 15 秒心跳。task_id 尚无任何数据时连接保持建立，不报错；该任务之后开始上报时数据能推过来。

## 5.4 任务摘要 · GET /api/v1/tasks/{task_id}/summary

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

## 5.5 对账端点（管理面，验收用） · GET /api/v1/admin/tasks/{task_id}/audit

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

point_count 为该任务去重后的落库点数；missing_steps 为可选实现（应有 step 区间内的缺口）。

## 5.6 错误码规范

| HTTP | code | 场景 |
| --- | --- | --- |
| 400 | INVALID_PARAMS | 字段缺失/类型错误/step 为负/batch 超长——整批拒绝 |
| 404 | TASK_NOT_FOUND | 查询/摘要的 task_id 无任何数据 |
| 503 | MQ_UNAVAILABLE | 接入层写 MQ 失败（含重试后仍失败） |
| 500 | INTERNAL | 未预期异常，响应体不得泄露堆栈 |

所有错误响应统一格式：{"error": {"code": "...", "message": "人类可读描述"}}

# 06  数据模型与幂等设计

## 6.1 参考表结构（推荐方案，允许在设计文档中给出替代并论证）

```sql
-- 指标点主表：按天 RANGE 分区，天然支持保留策略（滚动 DROP 分区）
CREATE TABLE metric_points (
  task_id  VARCHAR(64)       NOT NULL,
  key      VARCHAR(32)       NOT NULL,
  step     INTEGER           NOT NULL,
  ts       TIMESTAMPTZ       NOT NULL,
  value    DOUBLE PRECISION  NOT NULL,
  PRIMARY KEY (task_id, key, step, ts)   -- 分区表主键必须包含分区键 ts
) PARTITION BY RANGE (ts);
 
-- 示例：按天建分区（实现方式任选：手动 DDL / pg_partman / 代码自动预建）
CREATE TABLE metric_points_2026_08_25 PARTITION OF metric_points
  FOR VALUES FROM ('2026-08-25') TO ('2026-08-26');
 
-- 任务元数据（可选，summary 的快路径；不建也可接受——summary 直接聚合查询）
CREATE TABLE task_metric_meta (
  task_id    VARCHAR(64) PRIMARY KEY,
  first_seen TIMESTAMPTZ NOT NULL,
  last_seen  TIMESTAMPTZ NOT NULL,
  last_step  INTEGER     NOT NULL DEFAULT 0
);
 
-- 查询辅助索引（按你的查询路径验证后调整）
CREATE INDEX idx_mp_task_step ON metric_points (task_id, key, step);
```

## 6.2 幂等设计（本题的核心考点之一）

- 契约层：上报方重试必须携带与首次相同的 ts（第 5.1 节），于是 (task_id, key, step, ts) 天然构成业务唯一键；
- 写入层：批量 INSERT ... ON CONFLICT DO NOTHING（Go 侧用 pgx.Batch 一次往返提交整批），重复点被静默吸收，不需要先查后写；
- 语义层：MQ 至少一次投递 + 落库幂等 = 端到端恰好一次效果。这条链路请务必在答辩时能白板讲清楚。
> 已知边界（设计文档里请讨论）
> 若同一点以不同 ts 重复到达（契约之外的脏数据），主键无法拦截，会产生「重复 step」。可选对策：按分区建 (task_id, key, step) 唯一索引，或接受并在 audit 中报告 distinct_steps ≠ point_count/keys。两种都算正确答案，讲清取舍即可。

## 6.3 保留策略

原始数据保留 7 天（滚动清理，分区 DROP 或 DELETE 均可）。清理任务为加分项；不做清理不扣基础分，但答辩会被追问「表无限膨胀怎么办」——请提前想好答案。

# 07  非功能需求与验收指标

## 7.1 性能与可靠性指标（验收时逐项实测）

| 编号 | 指标 | 目标值 | 验证方式 |
| --- | --- | --- | --- |
| N1 | 写入吞吐：50 并发任务 × 10 点/秒（合计 500 点/秒），持续 ≥10 分钟 | 零丢失 | 模拟器 --audit 对账，point_count 与理论值一致 |
| N2 | 重复上报（注入 2% 重复批次） | 零重复落库 | 对账 distinct_steps 与 point_count 比对 |
| N3 | 消费者 kill -9 后重启 | 不丢不重 | 演练后对账仍 PASS |
| N4 | 查询 8 小时曲线（约 28,800 原始点/key） | P95 < 200ms | 压测脚本计时，max_points=500 |
| N5 | 摘要接口（单任务全量聚合） | P95 < 100ms | 压测脚本计时 |
| N6 | 新指标落库 → SSE 订阅方收到 | < 1 秒 | 模拟器上报侧与订阅侧双端打点 |
