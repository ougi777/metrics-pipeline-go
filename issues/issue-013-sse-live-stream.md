# [US-007] 实现 SSE 实时订阅与连接管理

## Description

实现任务级 SSE 实时订阅、实例内 Hub 广播、心跳和连接资源管理。

## Acceptance Criteria

- [x] 实现 `GET /api/v1/tasks/{task_id}/metrics/stream`。
- [x] 输出规范定义的 `metrics` 事件结构。
- [x] SSE ID 使用 `{task_id}:{event_seq}`。
- [x] 无游标连接从连接建立时的事件高水位开始接收新事件。
- [x] 尚无数据的任务保持 SSE 连接，并在后续上报时推送事件。
- [x] 每 15 秒输出规范定义的 `ping` 心跳。
- [x] 客户端断开后释放 Hub 订阅和关联 goroutine。
- [x] 为多订阅者广播、心跳和连接取消编写测试。

## Dependencies

Issue #12

## Type

backend

## Priority

high
