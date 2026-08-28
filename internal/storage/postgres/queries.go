package postgres

import _ "embed"

//go:embed sql/seed_task_event_counters.sql
var seedTaskEventCountersSQL string

//go:embed sql/persist_metric_flush.sql
var persistMetricFlushSQL string

//go:embed sql/ensure_daily_partitions.sql
var ensureDailyPartitionsSQL string

//go:embed sql/ensure_metric_point_partition.sql
var ensureMetricPointPartitionSQL string
