# [US-006] 实现任务指标摘要接口

## Description

实现单任务各指标的最新值和统计摘要查询，并利用合适索引满足低延迟访问目标。

## Acceptance Criteria

- [ ] 实现 `GET /api/v1/tasks/{task_id}/summary`。
- [ ] 每个 key 返回 `last`、`min`、`max` 和 `avg`。
- [ ] 任务级响应返回 `last_step` 和 `updated_at`。
- [ ] 最新值选择规则在相同步数存在多个时间戳时保持确定性。
- [ ] 无数据任务返回 `404 TASK_NOT_FOUND`。
- [ ] 聚合查询使用任务、key 和时间索引。
- [ ] 提供可测量摘要查询延迟的基准或集成测试。

## Dependencies

Issue #3

## Type

backend

## Priority

high
