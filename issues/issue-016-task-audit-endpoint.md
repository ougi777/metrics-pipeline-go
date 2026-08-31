# [US-008] 实现任务审计接口

## Description

实现任务数据对账接口，提供去重行数、step 范围、指标 key 和缺口信息，并记录契约允许的脏数据证据。

## Acceptance Criteria

- [x] 实现 `GET /api/v1/admin/tasks/{task_id}/audit`。
- [x] `point_count` 统计去重后的实际指标行数。
- [x] `distinct_steps` 统计任务内不同 step 值。
- [x] 返回 `first_step`、`last_step`、排序稳定的 `keys` 和 `missing_steps`。
- [x] 相同 `(task_id,key,step)` 携带不同 ts 时输出结构化日志。
- [x] v1 响应字段严格限定为任务书 6.5 定义的集合。
- [x] 为重复四字段、不同 ts 重复 step 和 step 缺口编写测试。

## 接口契约
5.5 对账端点（管理面，验收用） · GET /api/v1/admin/tasks/{task_id}/audit
{
  "task_id": "ft-20260825-0001",
  "point_count": 40218,
  "distinct_steps": 20011,
  "first_step": 0,
  "last_step": 5000,
  "keys": ["loss", "lr"],
  "missing_steps": []
}
point_count 为该任务去重后的落库点数；missing_steps 为可选实现（应有 step 区间内的缺口）。


## Dependencies

Issue #3, Issue #7

## Type

backend

## Priority

medium
