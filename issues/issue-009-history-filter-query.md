# [US-005] 实现历史指标过滤查询

## Description

实现按任务、指标 key、时间和 step 范围查询历史曲线的 HTTP 接口，并保持固定的任务存在性语义。

## Acceptance Criteria

- [x] 实现 `GET /api/v1/tasks/{task_id}/metrics`。
- [x] 支持 `keys`、`from`、`to`、`step_from`、`step_to` 和 `max_points`。
- [x] `max_points` 默认值为 500。
- [x] `from/to` 使用左闭右开的毫秒时间范围。
- [x] 时间条件和 step 条件取交集。
- [x] 参数错误返回 `400 INVALID_PARAMS`。
- [x] 任务存在且过滤结果为空时返回 `200` 和空 `series`。
- [x] 任务从未产生落库数据时返回 `404 TASK_NOT_FOUND`。

## Dependencies

Issue #3

## Type

backend

## Priority

high
