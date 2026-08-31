# [US-009] 实现模拟器故障注入与自动对账

## Description

扩展模拟器以注入重复批次和网络断连，并通过服务端 audit 自动判断指标链路结果。

## Acceptance Criteria

- [x] 支持按 2% 比例重发完整批次。
- [x] 重复批次复用首次上报的 ts。
- [x] 支持可配置的断连或请求失败注入。
- [x] 记录理论 batch item 数、展开指标行数和任务范围。
- [x] `--audit` 调用服务端任务审计接口。
- [x] 比较理论值与服务端 `point_count`、step 范围、keys 和缺口。
- [x] 输出 PASS/FAIL、差异详情和服务端 audit 响应。
- [x] 进程退出码准确反映验收结果。

## Dependencies

Issue #16, Issue #17

## Type

infra

## Priority

medium
