# [US-004] 实现七天保留与分区维护任务

## Description

为指标及事件数据提供七天自动保留策略，通过分区维护和定期清理控制存储规模。

## Acceptance Criteria

- [x] 定期预建后续 UTC 日分区。
- [x] 指标数据保留窗口为 7×24 小时。
- [x] 事件日志数据保留窗口为 7×24 小时。
- [x] 过期指标优先通过分区操作清理。
- [x] 清理关联的事件及已处理 Outbox 数据。
- [x] 清理任务可安全重复执行。
- [x] 分区创建和清理结果写入结构化日志。

## Dependencies

Issue #3

## Type

infra

## Priority

medium
