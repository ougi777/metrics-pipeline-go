# [US-002] 实现批量指标接入与整批校验

## Description

实现训练指标批量接入 API，按照固定 v1 契约完成请求解析、整批校验和统一响应处理。

## Acceptance Criteria

- [x] 实现 `POST /api/v1/ingest/metrics`。
- [x] 接受规范定义的 `task_id` 和 `batch` JSON 结构。
- [x] 单个 batch 最多包含 500 个采样点。
- [x] 校验必填字段、字段类型、非负 step、时间戳和数值字段边界。
- [x] 任一采样点校验失败时整批拒绝并返回 `400 INVALID_PARAMS`。
- [x] 错误响应统一使用 `{"error":{"code":"...","message":"..."}}`。
- [x] 合法请求返回规范定义的 `accepted` 和 `task_id`。
- [x] 为请求边界、整批拒绝和错误响应编写单元测试。

## Dependencies

Issue #1

## Type

backend

## Priority

high
