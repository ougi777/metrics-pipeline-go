WITH input AS (
    SELECT task_id, key, step, ts, value
    FROM unnest(
        $1::varchar[],
        $2::varchar[],
        $3::integer[],
        $4::timestamptz[],
        $5::double precision[]
    ) AS points(task_id, key, step, ts, value)
),
inserted AS (
    INSERT INTO metric_points (task_id, key, step, ts, value)
    SELECT task_id, key, step, ts, value
    FROM input
    ON CONFLICT (task_id, key, step, ts) DO NOTHING
    RETURNING task_id, key, step, ts, value
),
point_payloads AS (
    SELECT
        task_id,
        step,
        ts,
        jsonb_object_agg(key, to_jsonb(value) ORDER BY key) || jsonb_build_object(
            'step', step,
            'ts', (extract(epoch FROM ts) * 1000)::bigint
        ) AS point
    FROM inserted
    GROUP BY task_id, step, ts
),
task_payloads AS (
    SELECT
        task_id,
        jsonb_build_object(
            'points',
            jsonb_agg(point ORDER BY step, ts)
        ) AS payload
    FROM point_payloads
    GROUP BY task_id
),
locked_counters AS MATERIALIZED (
    SELECT counters.task_id
    FROM task_event_counters AS counters
    JOIN task_payloads AS payloads
        ON payloads.task_id = counters.task_id
    ORDER BY counters.task_id
    FOR UPDATE OF counters
),
sequenced AS (
    UPDATE task_event_counters AS counters
    SET last_event_seq = counters.last_event_seq + 1
    FROM task_payloads AS payloads
    JOIN locked_counters AS locked
        ON locked.task_id = payloads.task_id
    WHERE counters.task_id = payloads.task_id
    RETURNING
        counters.task_id,
        counters.last_event_seq AS event_seq,
        payloads.payload
),
events AS (
    INSERT INTO metric_events (task_id, event_seq, payload)
    SELECT task_id, event_seq, payload
    FROM sequenced
    RETURNING task_id, event_seq, payload
),
outbox AS (
    INSERT INTO metric_outbox (task_id, event_seq, payload)
    SELECT task_id, event_seq, payload
    FROM events
    RETURNING task_id, event_seq, payload
)
SELECT task_id, event_seq, payload
FROM outbox
ORDER BY task_id;
