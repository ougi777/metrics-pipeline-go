

设计文档



api文档

sql











数据保留与清理策略

## 七天滚动窗口

原始指标 `metric_points` 与事件日志 `metric_events` 保留最近 168 小时。worker 启动时执行一次维护，之后每小时执行；清理基准为当前时间减去 168 小时，全部时间计算使用 UTC。

## 分区与删除

两张表按 UTC 自然日 RANGE 分区。截止日之前的完整日分区直接 `DROP TABLE`，以元数据操作快速回收大量过期数据。截止日所在分区同时包含保留数据与过期数据，按 `ts < cutoff` 或 `created_at < cutoff` 分批 `DELETE`，每批由 `p_batch_size` 限制，精确维持 168 小时窗口并控制事务时长。

维护任务持有与分区创建一致的 PostgreSQL advisory lock，确保预建分区、写入时建分区与清理操作串行执行。

代码执行分区创建或清理前，先申请名为 metrics-pipeline-partitions 的锁：
SELECT pg_advisory_xact_lock(
  hashtextextended('metrics-pipeline-partitions', 0)
);
拿到锁的 worker 执行维护；其他 worker 申请同一把锁时等待。当前事务结束后锁自动释放，下一位继续。
它只约束主动申请这把锁的代码。普通 INSERT、SELECT 不会因为这把锁而被拦住。这里用于避免两个 worker 同时创建、删除同一日分区。



## 事件与 Outbox

`metric_events` 使用同一策略，保留七天内的 SSE 补发数据。`metric_outbox` 仅删除截止时间前且已发布的记录；待发布记录持续保留，避免清理任务丢失尚未投递的事件。`task_event_counters` 按任务长期保留，用于维持事件序号连续性。
