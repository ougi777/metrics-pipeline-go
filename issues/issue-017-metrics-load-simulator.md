# [US-009] 实现指标负载模拟器

## Description

实现可配置的 LF callback 模拟器，生成符合训练过程特征的基础及可选指标。

## Acceptance Criteria

- [x] 生成稳定格式的 task_id、递增 step 和毫秒 ts。
- [x] 支持任务数量、持续时间、采样速率和 batch 大小参数。
- [x] 生成使用指数衰减加噪声模型的 `loss`。
- [x] 生成使用 warmup 加余弦衰减模型的 `lr`。
- [x] 按每 N step 稀疏生成 `eval_loss`。
- [x] 使用随机游走生成 `gpu_util` 和 `gpu_mem`。
- [x] 按开关生成 tokens/s 单位的 `throughput`。
- [x] 可选指标均可通过 CLI 参数独立启用。
- [x] 以配置速率和 batch 大小调用指标接入 API。

## Dependencies

Issue #4

## Type

infra

## Priority

medium
