WITH filtered AS NOT MATERIALIZED (
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

    //根据上表统计每个key的数量、最小时间戳、最大时间戳,存为stats表
    SELECT key, count(*) AS point_count, min(ts) AS min_ts, max(ts) AS max_ts
    FROM filtered
    GROUP BY key
), bucket_config AS (
    //计算每个key的时间间隔,如果数量大于$8,则计算bucket_ms,否则为0
    SELECT COALESCE(MAX(GREATEST(1, CEIL((EXTRACT(EPOCH FROM (max_ts - min_ts)) * 1000 + 1) / $8::numeric))), 0)::bigint AS bucket_ms
    FROM stats
    WHERE point_count > $8
), raw_points AS (
    //如果数量小于等于$8,则直接返回原始数据
    SELECT f.key, f.step, f.ts, f.value AS v, f.value AS min, f.value AS max
    FROM filtered f
    JOIN stats s ON s.key = f.key
    WHERE s.point_count <= $8
), sampled_points AS (
    //如果数量大于$8,则按bucket_ms进行采样,返回采样后的数据
    SELECT f.key,
            //这里取最早的step
           -- Array comparison is lexicographic, matching ORDER BY ts ASC, step ASC.
           (min(ARRAY[EXTRACT(EPOCH FROM f.ts), f.step::numeric]))[2]::integer AS step,
           min(f.ts) AS ts,
           avg(f.value) AS v,
           min(f.value) AS min,
           max(f.value) AS max
    FROM filtered f
    JOIN stats s ON s.key = f.key
    CROSS JOIN bucket_config b
    WHERE s.point_count > $8
    GROUP BY f.key, date_bin(b.bucket_ms * interval '1 millisecond', f.ts, s.min_ts)
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
