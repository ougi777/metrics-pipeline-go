# [US-001/US-004] 建立数据库迁移与分区存储结构

## Description

建立指标、实时事件和 Outbox 的 PostgreSQL 存储模型，并提供可重复执行的迁移及日分区初始化机制。

## Acceptance Criteria

- [ ] 创建数据库迁移目录和基础迁移执行机制。
- [ ] `metric_points` 使用 UTC 自然日 RANGE 分区。
- [ ] `metric_points` 主键为 `(task_id, key, step, ts)`。
- [ ] 创建支持任务、key、时间和摘要查询的索引。
- [ ] 创建 `metric_events`，并为每个任务维护递增 `event_seq`。
- [ ] 创建支持可靠事件发布的 Outbox 表及必要索引。
- [ ] 初始化当前日期及近期所需的日分区。
- [ ] Compose 环境启动后可自动完成迁移和分区初始化。

## Dependencies

Issue #1

## Type

backend

## Priority

high
