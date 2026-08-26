# [US-001] 建立容器化本地运行环境

## Description

提供可复现的本地容器环境，使 PostgreSQL、RabbitMQ、API 和 worker 能够一键启动，并支持服务独立维护。

## Acceptance Criteria

- [ ] 提供用于构建 Go 服务的多阶段 Dockerfile。
- [ ] Docker Compose 可启动 PostgreSQL、RabbitMQ、API 和 worker。
- [ ] API 和 worker 使用独立启动命令及独立容器。
- [ ] API 与 worker 均可独立重启。
- [ ] Compose 为持久化依赖配置稳定的数据卷和网络。
- [ ] 提供本地构建、启动、停止和测试命令说明。

## Dependencies

Issue #1

## Type

infra

## Priority

high
