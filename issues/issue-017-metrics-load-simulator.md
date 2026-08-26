# [US-009] 实现指标负载模拟器

## Description

实现可配置的 LF callback 模拟器，生成符合训练过程特征的基础及可选指标。

## Acceptance Criteria

- [ ] 生成稳定格式的 task_id、递增 step 和毫秒 ts。
- [ ] 支持任务数量、持续时间、采样速率和 batch 大小参数。
- [ ] 生成使用指数衰减加噪声模型的 `loss`。
- [ ] 生成使用 warmup 加余弦衰减模型的 `lr`。
- [ ] 按每 N step 稀疏生成 `eval_loss`。
- [ ] 使用随机游走生成 `gpu_util` 和 `gpu_mem`。
- [ ] 按开关生成 tokens/s 单位的 `throughput`。
- [ ] 可选指标均可通过 CLI 参数独立启用。
- [ ] 以配置速率和 batch 大小调用指标接入 API。

## Dependencies

Issue #4

## Type

infra

## Priority

medium
