# [US-007] 实现 Outbox relay 与多实例事件广播

## Description

将事务内生成的任务事件通过 Outbox 可靠发布到实时 RabbitMQ fanout Exchange，使所有在线 API 实例接收事件。

## Acceptance Criteria

- [x] 每个任务每次成功 flush 生成一个递增 `event_seq`。
- [x] 事件 payload 可包含多个 step 和 key。
- [x] 纯重复 flush 保持事件序列不变。
- [x] Outbox relay 按序读取待发布事件。
- [x] RabbitMQ publisher confirm 成功后标记 Outbox 已发布。
- [x] 临时发布失败持续重试并保留发布意图。
- [x] 实时链路使用 fanout Exchange 和每个 API 实例的独立临时队列。
- [x] 多 API 实例均可接收同一任务事件。

## Dependencies

Issue #5, Issue #7

## Type

backend

## Priority

high
