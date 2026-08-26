CREATE TABLE metric_points (
    task_id varchar(64) NOT NULL,
    key varchar(32) NOT NULL,
    step integer NOT NULL,
    ts timestamptz NOT NULL,
    value double precision NOT NULL,
    PRIMARY KEY (task_id, key, step, ts),
    CONSTRAINT metric_points_step_nonnegative CHECK (step >= 0),
    CONSTRAINT metric_points_value_finite CHECK (
        value NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)
    )
) PARTITION BY RANGE (ts);

CREATE INDEX metric_points_task_time
    ON metric_points (task_id, key, ts, step);

CREATE TABLE task_event_counters (
    task_id varchar(64) PRIMARY KEY,
    last_event_seq bigint NOT NULL DEFAULT 0,
    CONSTRAINT task_event_counters_seq_nonnegative CHECK (last_event_seq >= 0)
);

CREATE TABLE metric_events (
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    task_id varchar(64) NOT NULL,
    event_seq bigint NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (created_at, task_id, event_seq),
    CONSTRAINT metric_events_seq_positive CHECK (event_seq > 0),
    CONSTRAINT metric_events_payload_object CHECK (jsonb_typeof(payload) = 'object')
) PARTITION BY RANGE (created_at);

CREATE INDEX metric_events_task_seq
    ON metric_events (task_id, event_seq);

CREATE TABLE metric_outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id varchar(64) NOT NULL,
    event_seq bigint NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_until timestamptz,
    claim_token uuid,
    published_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (task_id, event_seq),
    CONSTRAINT metric_outbox_seq_positive CHECK (event_seq > 0),
    CONSTRAINT metric_outbox_attempt_count_nonnegative CHECK (attempt_count >= 0),
    CONSTRAINT metric_outbox_payload_object CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX metric_outbox_pending_publish
    ON metric_outbox (next_attempt_at, id)
    INCLUDE (task_id, event_seq)
    WHERE published_at IS NULL;

CREATE INDEX metric_outbox_expired_claim
    ON metric_outbox (claimed_until, id)
    WHERE published_at IS NULL AND claim_token IS NOT NULL;

CREATE OR REPLACE FUNCTION next_task_event_seq(p_task_id varchar(64))
RETURNS bigint
LANGUAGE sql
AS $$
    INSERT INTO task_event_counters (task_id, last_event_seq)
    VALUES (p_task_id, 1)
    ON CONFLICT (task_id) DO UPDATE
        SET last_event_seq = task_event_counters.last_event_seq + 1
    RETURNING last_event_seq;
$$;

CREATE OR REPLACE FUNCTION ensure_metric_daily_partitions(
    p_reference_date date,
    p_past_days integer,
    p_future_days integer
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    partition_day date;
    partition_end date;
    partition_suffix text;
    lower_bound text;
    upper_bound text;
BEGIN
    IF p_reference_date IS NULL THEN
        RAISE EXCEPTION 'reference date is required';
    END IF;
    IF p_past_days < 0 OR p_future_days < 0 OR p_past_days > 366 OR p_future_days > 366 THEN
        RAISE EXCEPTION 'partition day ranges must be between 0 and 366';
    END IF;

    partition_day := p_reference_date - p_past_days;
    partition_end := p_reference_date + p_future_days;

    WHILE partition_day <= partition_end LOOP
        partition_suffix := to_char(partition_day, 'YYYY_MM_DD');
        lower_bound := partition_day::text || ' 00:00:00+00';
        upper_bound := (partition_day + 1)::text || ' 00:00:00+00';

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF metric_points FOR VALUES FROM (%L) TO (%L)',
            'metric_points_' || partition_suffix,
            lower_bound,
            upper_bound
        );
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF metric_events FOR VALUES FROM (%L) TO (%L)',
            'metric_events_' || partition_suffix,
            lower_bound,
            upper_bound
        );

        partition_day := partition_day + 1;
    END LOOP;
END;
$$;
