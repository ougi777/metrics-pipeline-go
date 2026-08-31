WITH task_points AS (
    SELECT key, step, ts
    FROM metric_points
    WHERE task_id = $1
), stats AS (
    SELECT count(*)::bigint AS point_count,
           count(DISTINCT step)::bigint AS distinct_steps,
           min(step)::integer AS first_step,
           max(step)::integer AS last_step,
           COALESCE(array_agg(DISTINCT key ORDER BY key), ARRAY[]::varchar[]) AS keys
    FROM task_points
), missing AS (
    SELECT COALESCE(array_agg(step ORDER BY step), ARRAY[]::integer[]) AS missing_steps
    FROM generate_series(
        (SELECT CASE WHEN last_step - first_step <= 100000 THEN first_step END FROM stats),
        (SELECT CASE WHEN last_step - first_step <= 100000 THEN last_step END FROM stats)
    ) AS expected(step)
    WHERE NOT EXISTS (SELECT 1 FROM task_points WHERE task_points.step = expected.step)
)
SELECT EXISTS (SELECT 1 FROM task_points), stats.point_count, stats.distinct_steps,
       stats.first_step, stats.last_step, stats.keys, missing.missing_steps
FROM stats CROSS JOIN missing;
