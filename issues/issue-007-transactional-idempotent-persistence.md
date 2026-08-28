# [US-003] 实现事务批量落库与幂等恢复

## Description

使用 PostgreSQL 事务批量持久化指标，并通过业务唯一键、事件日志和 Outbox 获得可恢复的 exactly-once effect。

## Acceptance Criteria

- [x] 每个 flush 使用一个 PostgreSQL 事务。
- [x] 使用 `pgx.Batch` 在一次往返中提交批量写入。
- [x] 指标写入使用 `ON CONFLICT DO NOTHING` 消除相同四字段重复数据。
- [x] 仅针对实际新增指标生成任务级事件内容。
- [x] `metric_points`、`metric_events` 和 Outbox 在同一事务中提交。
- [x] 事务成功后向消费层报告成功并触发关联 delivery ack。
- [x] 事务失败时向消费层返回可重试错误并触发 requeue。
- [x] worker 重启后重新消费未确认消息，并保持零重复落库。

## Dependencies

Issue #3, Issue #6

## Type

backend

## Priority

high
