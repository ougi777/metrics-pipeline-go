# [US-001/US-004] 建立数据库迁移与分区存储结构

## Description

采用任务书推荐的 PostgreSQL 原生分区基础方案，建立指标、实时事件和 Outbox 存储模型，并提供可重复执行的迁移及日分区初始化机制。任务摘要直接聚合保留窗口内的 `metric_points`，预聚合表根据 N5 压测结果决定后续演进。

## Acceptance Criteria

- [x] 创建数据库迁移目录和基础迁移执行机制。
- [x] `metric_points` 使用 UTC 自然日 RANGE 分区。
- [x] `metric_points` 主键为 `(task_id, key, step, ts)`。
- [x] 创建支持任务、key、时间和摘要查询的索引。
- [x] 摘要查询基于 `metric_points` 原始分区聚合，保持基础方案的数据模型。
- [x] 创建 `metric_events`，并为每个任务维护递增 `event_seq`。
- [x] 创建支持可靠事件发布的 Outbox 表及必要索引。
- [x] 初始化当前日期及近期所需的日分区。
- [x] Compose 环境启动后可自动完成迁移和分区初始化。

## Design Decision

- 当前迁移包含 `metric_points`、`task_event_counters`、`metric_events` 和 `metric_outbox` 四张逻辑表。
- 历史查询使用 PostgreSQL SQL 时间桶完成降采样。
- 任务摘要直接从最近七天的 `metric_points` 计算最新值、最小值、最大值和平均值。
- N5 摘要接口 P95 持续超过 100ms 时，再评估日级预聚合、分钟级预聚合或缓存方案。

## Dependencies

Issue #1

## Type

backend

## Priority

high
