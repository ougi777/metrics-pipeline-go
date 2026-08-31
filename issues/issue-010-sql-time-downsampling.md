# [US-005] 实现 SQL 时间降采样

## Description

在 PostgreSQL 查询层为超出点数限制的曲线执行逐 key 时间分桶，生成符合固定响应契约的聚合结果。

## Acceptance Criteria

- [x] 每个 key 独立统计过滤后的原始点数。
- [x] 仅对超过 `max_points` 的 key 执行降采样。
- [x] 降采样始终使用时间分桶。
- [x] 在 SQL 层计算每个桶的 `min`、`max` 和 `avg`。
- [x] 响应字段 `v` 使用桶内平均值。
- [x] 响应中的 `step` 和 `ts` 使用桶内最早指标点的值。
- [x] 响应返回现有 `bucket_ms` 和准确的 `downsampled` 状态。
- [x] 为多 key 独立降采样和边界桶编写测试。

## Dependencies

Issue #9

## Type

backend

## Priority

high
