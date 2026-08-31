# [US-010] 建立核心链路 E2E 测试

## Description

建立可重复执行的端到端测试，覆盖从 HTTP 接入到 MQ、数据库、查询和审计的核心指标链路。

## Acceptance Criteria

- [x] 测试可启动或连接 PostgreSQL、RabbitMQ、API 和 worker。
- [x] 提交指标并验证 API 成功响应、MQ 消费和数据库落库。
- [x] 验证历史查询、时间与 step 交集过滤及降采样结果。
- [x] 验证任务摘要和 audit 结果。
- [x] 重复提交同一批次后验证零重复落库。
- [x] 覆盖非法 batch、空过滤结果和无数据任务响应。
- [x] 测试可重复执行。
- [x] 测试结束时仅清理本次运行创建的数据。

## Dependencies

Issue #5, Issue #7, Issue #9, Issue #10, Issue #11, Issue #16

## Type

backend

## Priority

high
