# [US-007] 实现 Last-Event-ID 断点续传

## Description

基于 PostgreSQL 事件日志恢复 SSE 客户端在七天保留窗口内遗漏的任务事件，并协调补发与实时广播顺序。

## Acceptance Criteria

- [x] 解析 `{task_id}:{event_seq}` 格式的 `Last-Event-ID`。
- [x] 校验游标任务与请求路径任务一致。
- [x] 在七天保留窗口内按 `event_seq` 顺序补发遗漏事件。
- [x] 格式错误或超出保留窗口的游标返回 `400 INVALID_PARAMS`。
- [x] SSE 连接先注册 Hub，再读取高水位和事件日志。
- [x] 补发结束后发送连接期间缓存的实时事件。
- [x] API 重启或实例切换后可从事件日志继续恢复。
- [x] 补发与实时切换过程保持事件有序且单次发送。

## Dependencies

Issue #3, Issue #13

## Type

backend

## Priority

high
