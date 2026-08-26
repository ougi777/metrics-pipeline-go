# [US-002] 实现 RabbitMQ 可靠发布链路

## Description

将通过校验的指标批次可靠发布到 RabbitMQ，并在消息获得发布确认后向调用方返回成功。

## Acceptance Criteria

- [ ] 声明持久化业务 Exchange、Queue、绑定关系和 DLQ。
- [ ] 发布持久化消息并启用 publisher confirms。
- [ ] 每个发布 goroutine 独占 AMQP Channel。
- [ ] 连接恢复后重新创建 Channel、声明拓扑并恢复 confirms。
- [ ] API 在可靠发布确认后返回成功响应。
- [ ] MQ 发布失败时执行有界重试，重试耗尽后返回 `503 MQ_UNAVAILABLE`。
- [ ] API 接入链路仅向 RabbitMQ 提交指标数据。
- [ ] 为发布确认、重试和错误映射编写测试。

## Dependencies

Issue #2, Issue #4

## Type

backend

## Priority

high
