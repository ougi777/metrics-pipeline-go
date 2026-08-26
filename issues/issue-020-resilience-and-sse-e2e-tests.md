# [US-010] 建立故障恢复与 SSE E2E 测试

## Description

建立覆盖 worker 故障、SSE 实时接收、断点续传及服务实例恢复的端到端测试。

## Acceptance Criteria

- [ ] 指标处理中强制退出 worker，并在重启后验证消息恢复。
- [ ] worker 恢复后验证数据完整且保持幂等。
- [ ] 验证 SSE 客户端收到新落库指标事件。
- [ ] 验证客户端断线后使用 `Last-Event-ID` 补发遗漏事件。
- [ ] 验证 SSE 事件 ID、顺序和 payload。
- [ ] 验证 API 重启或切换实例后的事件日志恢复。
- [ ] 验证客户端断开后的订阅资源释放。
- [ ] 测试结束时仅清理本次运行创建的数据。

## Dependencies

Issue #12, Issue #13, Issue #14, Issue #15, Issue #19

## Type

backend

## Priority

high
