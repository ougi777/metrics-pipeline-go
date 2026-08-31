# [US-008] 实现健康检查与优雅退出

## Description

提供服务存活、依赖就绪状态及 SIGTERM 生命周期处理，支持容器编排和安全维护。

## Acceptance Criteria

- [x] `/healthz` 返回进程存活状态。
- [x] `/readyz` 检查 PostgreSQL 可达性。
- [x] `/readyz` 检查 RabbitMQ 可达性。
- [x] 依赖异常时就绪检查返回对应的非就绪状态。
- [x] Compose 配置 API 和 worker 的健康检查。
- [x] API 收到 SIGTERM 后停止接受新请求并关闭连接。
- [x] worker 收到 SIGTERM 后停止拉取消息、完成当前 flush 并关闭 AMQP 与数据库连接。

## Dependencies

Issue #2, Issue #5, Issue #7

## Type

infra

## Priority

medium
