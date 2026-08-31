WITH filtered AS (
    SELECT task_id, key, step, ts, value
    FROM metric_points
    WHERE task_id = $1
      AND ts >= $2::timestamptz
      AND ($3::text[] IS NULL OR key = ANY($3))
      AND ($4::timestamptz IS NULL OR ts >= $4)
      AND ($5::timestamptz IS NULL OR ts < $5)
      AND ($6::integer IS NULL OR step >= $6)
      AND ($7::integer IS NULL OR step <= $7)
), stats AS (
    SELECT key, count(*) AS point_count, min(ts) AS min_ts, max(ts) AS max_ts
    FROM filtered
    GROUP BY key
), bucket_config AS (
    SELECT COALESCE(MAX(GREATEST(1, CEIL((EXTRACT(EPOCH FROM (max_ts - min_ts)) * 1000 + 1) / $8::numeric))), 0)::bigint AS bucket_ms
    FROM stats
    WHERE point_count > $8
), raw_points AS (
    SELECT f.key, f.step, f.ts, f.value AS v, f.value AS min, f.value AS max
    FROM filtered f
    JOIN stats s ON s.key = f.key
    WHERE s.point_count <= $8
), sampled_points AS (
    SELECT f.key,
           (array_agg(f.step ORDER BY f.ts, f.step))[1] AS step,
           min(f.ts) AS ts,
           avg(f.value) AS v,
           min(f.value) AS min,
           max(f.value) AS max
    FROM filtered f
    JOIN stats s ON s.key = f.key
    CROSS JOIN bucket_config b
    WHERE s.point_count > $8
    GROUP BY f.key, FLOOR(EXTRACT(EPOCH FROM (f.ts - s.min_ts)) * 1000 / b.bucket_ms)
), result_points AS (
    SELECT * FROM raw_points
    UNION ALL
    SELECT * FROM sampled_points
), meta AS (
    SELECT EXISTS(
        SELECT 1 FROM metric_points
        WHERE task_id = $1 AND ts >= $2::timestamptz
    ) AS task_exists,
    (SELECT bucket_ms FROM bucket_config) AS bucket_ms
)
SELECT meta.task_exists,
       meta.bucket_ms,
       result_points.key,
       result_points.step,
       result_points.ts,
       result_points.v,
       result_points.min,
       result_points.max
FROM meta
LEFT JOIN result_points ON TRUE
ORDER BY result_points.key, result_points.ts, result_points.step;
