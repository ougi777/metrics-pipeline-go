# metrics-pipeline PostgreSQL 表结构与存储方案选型

> 面向 MT 评审的存储设计决策记录  
> 状态：已对齐  
> 日期：2026-08-26  
> 关联：[任务书](./m3-task-go.md)、[Issue 003](../issues/issue-003-database-schema-and-partitions.md)

## 1. 已敲定表结构

v1 采用 PostgreSQL 原生日分区与 SQL 聚合，业务结构锁定为以下四张逻辑表：

| 逻辑表 | 职责 | 关键约束与索引 | 生命周期 |
| --- | --- | --- | --- |
| `metric_points` | 保存展开、去重后的原始指标点 | 主键 `(task_id, key, step, ts)`；索引 `(task_id, key, ts, step)`；按 `ts` 日分区 | 七天保留窗口 |
| `task_event_counters` | 为每个任务分配连续递增的 SSE 事件序号 | `task_id` 主键；`last_event_seq >= 0` | 随任务 ID 长期保留 |
| `metric_events` | 保存可补发的任务级 SSE 事件日志 | 主键包含分区键；索引 `(task_id, event_seq)`；按 `created_at` 日分区 | 七天保留窗口 |
| `metric_outbox` | 保存已提交事件的可靠发布意图和 relay 状态 | `(task_id, event_seq)` 唯一；待发布与过期 claim 使用部分索引 | 待发布记录持续保留，已发布记录按维护策略清理 |

`schema_migrations` 属于迁移执行器元数据。每日分区属于两个父表的物理子表。本文“四张表”指四张业务逻辑表。

### 1.1 当前 DDL

以下代码突出核心结构，完整约束、部分索引和分区函数以 [`migrations/000001_initial_schema.sql`](../migrations/000001_initial_schema.sql) 为准。

```sql
CREATE TABLE metric_points (
    task_id varchar(64) NOT NULL,
    key varchar(32) NOT NULL,
    step integer NOT NULL,
    ts timestamptz NOT NULL,
    value double precision NOT NULL,
    PRIMARY KEY (task_id, key, step, ts),
    CONSTRAINT metric_points_step_nonnegative CHECK (step >= 0)
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
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (task_id, event_seq)
);
```

## 2. 已对齐结论

| 议题 | 结论 |
| --- | --- |
| 指标主存 | `metric_points` 按 `ts` 的 UTC 自然日进行 RANGE 分区 |
| 历史曲线 | 直接读取原始分区，在 PostgreSQL 中完成等时间桶降采样 |
| 任务摘要 | 直接聚合最近七天的 `metric_points`，由 N5 压测决定后续优化 |
| 实时可靠性 | 任务级事件序号、事件日志与 Outbox 共同支持有序发布和断线补发 |
| 演进边界 | N5 P95 持续超过 100ms 后评估预聚合；N4 P95 持续超过 200ms 后评估历史预聚合或 TimescaleDB |

## 3. 写入与查询路径

### 3.1 写入路径

1. API 校验批次并等待 RabbitMQ publisher confirm。
2. worker 按条数或时间阈值聚合 delivery。
3. PostgreSQL 单事务写入真实新增的 `metric_points`、任务事件和 Outbox。
4. 数据库提交成功后，worker 对 RabbitMQ 执行 ack。
5. Outbox relay 发布已提交事件，SSE 消费方按 `event_seq` 去重和补发。

`ON CONFLICT DO NOTHING` 通过 `(task_id, key, step, ts)` 吸收 MQ 重投，形成端到端 exactly-once effect。

### 3.2 读取路径

- 历史曲线：先按 task、key、时间和 step 过滤，再在 SQL 层执行等时间桶降采样。
- 任务摘要：直接聚合最近七天 `metric_points`，按 key 计算 `last`、`min`、`max` 和 `avg`。
- SSE 补发：按 `task_id` 和 `event_seq` 从 `metric_events` 读取保留窗口内的遗漏事件。

## 4. 候选方案对比

任务书要求从写入吞吐、查询延迟和运维成本三个维度论证存储方案。

| 方案 | 写入吞吐 | 查询延迟 | 运维成本 | 一致性与当前适配度 |
| --- | --- | --- | --- | --- |
| **原生日分区 + SQL 聚合**（当前选择） | 每个真实指标写入一次；索引和事件链路产生固定写放大 | 历史查询受分区裁剪与时间桶约束；摘要延迟随单任务七天数据量增长 | 仅依赖标准 PostgreSQL；迁移、备份和排障路径清晰 | 单一事实表天然避免聚合漂移，符合任务书基础方案 |
| **自建预聚合表** | 消费事务增加按 key/时间桶 upsert；热点任务可能形成行锁竞争 | 摘要或历史聚合读取行数稳定，P95 更容易控制 | 增加回填、边界日重算、保留清理和对账逻辑 | 事务内维护可获得强一致，实现与测试范围扩大 |
| **TimescaleDB** | hypertable 提供成熟分片写入能力，实际吞吐依赖扩展配置 | continuous aggregate 与时序函数适合大规模聚合查询 | 增加扩展镜像、版本兼容、升级和故障排查要求 | 能力完整，当前验收规模缺少引入依据 |

### 4.1 PostgreSQL 原生日分区 + SQL 聚合

优点：

- 与任务书推荐的基础方案一致。
- 数据模型简单，原始数据是唯一事实来源。
- 写入路径短，便于验证幂等、事务和 MQ 确认顺序。
- 使用标准 PostgreSQL，部署和排障成本低。

代价：

- summary 扫描量随单任务七天数据量增长。
- 查询性能依赖分区裁剪、索引和 SQL 执行计划。
- 性能结论需要通过 N4/N5 压测确认。

### 4.2 自建预聚合表

优点：

- summary 或历史聚合的读取成本稳定。
- 可以针对日摘要、分钟曲线等具体查询独立优化。
- 聚合结果可与指标写入放入同一事务。

代价：

- 每批写入增加聚合 upsert 和潜在行锁竞争。
- 需要处理历史回填、边界日重算和数据对账。
- 七天保留策略需要同步维护原始表与聚合表。

### 4.3 TimescaleDB

优点：

- hypertable、continuous aggregate 和保留策略能力成熟。
- 适合更高数据规模和更多时间聚合查询。
- 减少自建分区维护与聚合调度代码。

代价：

- 增加 PostgreSQL 扩展及定制镜像依赖。
- 增加版本升级、兼容性和故障排查成本。
- 团队需要补充 TimescaleDB 运维经验和性能基线。

## 5. 当前选择及理由

v1 选择 **PostgreSQL 原生日分区 + 查询侧 SQL 聚合**。

### 5.1 与任务书保持一致

任务书将 PostgreSQL 原生 RANGE 分区和查询侧等间隔桶降采样列为推荐基础方案，并说明其性能足以覆盖当前验收。当前实现沿用这条路径，便于 MT 按任务书逐项核对。

### 5.2 优先完成端到端闭环

两周周期内需要完成接入、RabbitMQ、幂等落库、历史查询、summary、SSE、模拟器和对账。基础方案减少预聚合的一致性、回填和清理工作，使开发重点集中在核心链路与验收指标。

### 5.3 使用压测证据驱动优化

N4 和 N5 提供明确的性能门槛。先完成真实负载压测并记录 `EXPLAIN (ANALYZE, BUFFERS)`，可以定位瓶颈来自扫描行数、索引、分区裁剪、SQL 聚合或数据库资源，再选择对应优化方案。

### 5.4 保持标准运行环境

当前 Compose 仅依赖标准 PostgreSQL、RabbitMQ 和 Go 服务。该组合的迁移、备份、升级和排障路径清晰，符合 v1 的交付边界。

## 6. 为什么保留事件相关三张表

`task_event_counters`、`metric_events` 和 `metric_outbox` 服务于实时链路可靠性，与查询预聚合承担不同职责：

| 表 | 解决的问题 |
| --- | --- |
| `task_event_counters` | 为每个任务生成连续 `event_seq`，支持 SSE 排序、去重和游标校验 |
| `metric_events` | 保存七天事件日志，支持 Last-Event-ID 补发和 API 重启恢复 |
| `metric_outbox` | 将指标提交和事件发布意图放入同一事务，避免数据库与 RabbitMQ 双写形成永久缺口 |

Issue 003、Issue 007、Issue 012、Issue 013 和 Issue 014 均依赖这些结构。当前简化范围聚焦查询预聚合表，实时可靠性结构保持稳定。

## 7. 风险与演进条件

| 风险 | 当前缓解 | 演进触发条件 | 候选动作 |
| --- | --- | --- | --- |
| 单任务七天摘要扫描量增长 | UTC 日分区裁剪；`(task_id, key, ts, step)` 索引；SQL 聚合 | N5 在索引和 SQL 调优后 P95 持续超过 100ms | 评估日级预聚合、分钟级预聚合或缓存 |
| 八小时历史曲线查询变慢 | 先过滤 task、key、时间与 step，再执行有界时间桶聚合 | N4 在 SQL 调优后 P95 持续超过 200ms | 评估历史预聚合表或 TimescaleDB |
| 未来预聚合与原始数据漂移 | 当前由原始表直接计算，事实来源唯一 | 压测证明预聚合收益后进入独立设计评审 | 定义事务更新、回填、对账和保留边界 |
| 事件与 Outbox 存储增长 | 事件按日分区；已发布 Outbox 进入维护清理范围 | 七天事件容量或 relay 积压超过部署预算 | 评估 payload 合并、压缩或 relay 分片 |

当前文档记录设计目标和验证路径。N4/N5 的最终性能结论以完整压测报告为准。

## 8. 验证清单

| 用例 | 前置 | 步骤 | 预期 |
| --- | --- | --- | --- |
| TC-001 全新迁移 | 空 PostgreSQL 数据库 | 执行 `cmd/migrate` | 创建四张业务逻辑表，迁移返回 `applied: 1` |
| TC-002 重复迁移 | 版本 1 已应用 | 再次执行迁移 | 校验和一致，返回 `applied: 0` |
| TC-003 UTC 分区边界 | 当天及次日分区存在 | 分别插入 23:59:59 UTC 与次日 00:00:00 UTC 指标 | 两条记录路由到各自 UTC 日期分区 |
| TC-004 摘要正确性 | 同一任务包含多个 key 和 step | 直接聚合七天窗口内 `metric_points` | 每个 key 的 last/min/max/avg 与原始数据一致 |
| TC-005 N5 性能 | 50 个任务按目标负载持续写入 | 压测 summary 并记录 PostgreSQL 执行计划 | P95 小于 100ms，结果决定预聚合评审状态 |
| TC-006 事件序号回滚 | 任务计数器可写 | 事务内分配序号后回滚，再次分配并提交 | 已提交事件序号连续递增 |

## 9. 相关代码索引

| 项目 | 路径 | 用途 |
| --- | --- | --- |
| 初始 DDL | `migrations/000001_initial_schema.sql:1` | `metric_points`、索引与约束 |
| 事件结构 | `migrations/000001_initial_schema.sql:17` | 任务计数器、事件日志与 Outbox |
| 分区函数 | `migrations/000001_initial_schema.sql:73` | 指标与事件 UTC 日分区预建 |
| 迁移执行器 | `internal/storage/postgres/migrate.go:47` | advisory lock、版本与校验和控制 |
| 分区入口 | `internal/storage/postgres/migrate.go:101` | 按基准日期初始化近期分区 |
| Issue 003 | `issues/issue-003-database-schema-and-partitions.md:19` | 基础方案与四表决策记录 |
| 总体设计 | `tasks/design-metrics-pipeline.md:336` | 原生 PostgreSQL、摘要聚合与演进理由 |

## 10. 变更记录

### 2026-08-26 · v1.0

- 锁定四张业务逻辑表及两个 UTC 日分区父表。
- 选择 PostgreSQL 原生分区与 SQL 聚合作为 v1 基础方案。
- 移除 `task_metric_daily` 日级预聚合表。
- 定义 N4/N5 性能阈值驱动的后续演进条件。
