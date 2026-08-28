# [US-003] 实现 worker 批量消费与 delivery 生命周期管理

## Description

实现业务队列消费者，将批次展开为指标记录并按照数量或时间阈值触发 flush，同时正确管理 AMQP delivery 生命周期。

## Acceptance Criteria

- [x] worker 使用手动 ack 模式消费业务队列。
- [x] 每个 `metrics` key-value 展开为一条指标记录。
- [x] 累计 500 条展开指标时触发 flush。
- [x] 等待 1 秒后触发未满批次的 flush。
- [x] 维护每个 delivery 与一个或多个 flush 分片的关联状态。
- [x] delivery 的全部分片成功后执行 ack。
- [x] flush 失败时对关联 delivery 执行 `nack(requeue=true)`。
- [x] JSON 或消息协议错误进入 DLQ并输出结构化日志。

## Dependencies

Issue #2, Issue #5

## Type

backend

## Priority

high
