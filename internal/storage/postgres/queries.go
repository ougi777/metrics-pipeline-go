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

//go:embed sql/maintain_metric_retention.sql
var maintainMetricRetentionSQL string

//go:embed sql/query_metric_history.sql
var queryMetricHistorySQL string

//go:embed sql/query_metric_summary.sql
var queryMetricSummarySQL string

//go:embed sql/claim_metric_outbox.sql
var claimMetricOutboxSQL string

//go:embed sql/mark_metric_outbox_published.sql
var markMetricOutboxPublishedSQL string

//go:embed sql/mark_metric_outbox_failed.sql
var markMetricOutboxFailedSQL string

//go:embed sql/release_metric_outbox_claim.sql
var releaseMetricOutboxClaimSQL string

//go:embed sql/query_metric_events.sql
var queryMetricEventsSQL string

//go:embed sql/query_metric_event_bounds.sql
var queryMetricEventBoundsSQL string

//go:embed sql/query_metric_audit.sql
var queryMetricAuditSQL string
