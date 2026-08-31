WITH scoped AS (
    SELECT key, step, ts, value
    FROM metric_points
    WHERE task_id = $1
      AND ts >= $2::timestamptz
), key_stats AS (
    SELECT
        key,
        (array_agg(value ORDER BY ts DESC, step DESC))[1] AS last,
        min(value) AS min,
        max(value) AS max,
        avg(value) AS avg
    FROM scoped
    GROUP BY key
), task_latest AS (
    SELECT step, ts
    FROM scoped
    ORDER BY ts DESC, step DESC
    LIMIT 1
), meta AS (
    SELECT
        EXISTS (SELECT 1 FROM scoped) AS task_exists,
        (SELECT step FROM task_latest) AS last_step,
        (SELECT ts FROM task_latest) AS updated_at
)
SELECT
    meta.task_exists,
    meta.last_step,
    meta.updated_at,
    key_stats.key,
    key_stats.last,
    key_stats.min,
    key_stats.max,
    key_stats.avg
FROM meta
LEFT JOIN key_stats ON TRUE
ORDER BY key_stats.key;
