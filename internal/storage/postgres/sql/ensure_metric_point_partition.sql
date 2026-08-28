WITH locked AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(hashtextextended('metrics-pipeline-partitions', 0))
)
SELECT ensure_metric_daily_partitions($1::date, 0, 0)
FROM locked
