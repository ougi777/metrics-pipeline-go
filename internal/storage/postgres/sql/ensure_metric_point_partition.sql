WITH locked AS MATERIALIZED (
    SELECT pg_advisory_xact_lock(
        hashtextextended('metrics-point-partition:' || $1::date::text, 0)
    )
)
SELECT ensure_metric_daily_partitions($1::date, 0, 0)
FROM locked
