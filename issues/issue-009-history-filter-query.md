# [US-005] 实现历史指标过滤查询

## Description

实现按任务、指标 key、时间和 step 范围查询历史曲线的 HTTP 接口，并保持固定的任务存在性语义。

## Acceptance Criteria

- [ ] 实现 `GET /api/v1/tasks/{task_id}/metrics`。
- [ ] 支持 `keys`、`from`、`to`、`step_from`、`step_to` 和 `max_points`。
- [ ] `max_points` 默认值为 500。
- [ ] `from/to` 使用左闭右开的毫秒时间范围。
- [ ] 时间条件和 step 条件取交集。
- [ ] 参数错误返回 `400 INVALID_PARAMS`。
- [ ] 任务存在且过滤结果为空时返回 `200` 和空 `series`。
- [ ] 任务从未产生落库数据时返回 `404 TASK_NOT_FOUND`。

## Dependencies

Issue #3

## Type

backend

## Priority

high
