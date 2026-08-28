CREATE OR REPLACE FUNCTION maintain_metric_retention(
    p_cutoff timestamptz,
    p_batch_size integer
)
RETURNS TABLE (
    point_partitions_dropped integer,
    event_partitions_dropped integer,
    metric_points_deleted integer,
    metric_events_deleted integer,
    metric_outbox_deleted integer
)
LANGUAGE plpgsql
AS $$
DECLARE
    partition_record record;
    cutoff_day date;
BEGIN
    IF p_cutoff IS NULL THEN
        RAISE EXCEPTION 'retention cutoff is required';
    END IF;
    IF p_batch_size < 1 OR p_batch_size > 100000 THEN
        RAISE EXCEPTION 'retention batch size must be between 1 and 100000';
    END IF;

    PERFORM pg_advisory_xact_lock(hashtextextended('metrics-pipeline-partitions', 0));
    cutoff_day := (p_cutoff AT TIME ZONE 'UTC')::date;
    point_partitions_dropped := 0;
    event_partitions_dropped := 0;

    FOR partition_record IN
        SELECT child.relname
        FROM pg_inherits inheritance
        JOIN pg_class parent ON parent.oid = inheritance.inhparent
        JOIN pg_class child ON child.oid = inheritance.inhrelid
        JOIN pg_namespace namespace ON namespace.oid = child.relnamespace
        WHERE parent.relname = 'metric_points'
          AND namespace.nspname = current_schema()
          AND child.relname ~ '^metric_points_[0-9]{4}_[0-9]{2}_[0-9]{2}$'
          AND to_date(substring(child.relname FROM '[0-9]{4}_[0-9]{2}_[0-9]{2}$'), 'YYYY_MM_DD') < cutoff_day
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I', partition_record.relname);
        point_partitions_dropped := point_partitions_dropped + 1;
    END LOOP;

    FOR partition_record IN
        SELECT child.relname
        FROM pg_inherits inheritance
        JOIN pg_class parent ON parent.oid = inheritance.inhparent
        JOIN pg_class child ON child.oid = inheritance.inhrelid
        JOIN pg_namespace namespace ON namespace.oid = child.relnamespace
        WHERE parent.relname = 'metric_events'
          AND namespace.nspname = current_schema()
          AND child.relname ~ '^metric_events_[0-9]{4}_[0-9]{2}_[0-9]{2}$'
          AND to_date(substring(child.relname FROM '[0-9]{4}_[0-9]{2}_[0-9]{2}$'), 'YYYY_MM_DD') < cutoff_day
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I', partition_record.relname);
        event_partitions_dropped := event_partitions_dropped + 1;
    END LOOP;

    WITH expired AS (
        SELECT task_id, key, step, ts
        FROM metric_points
        WHERE ts < p_cutoff
        ORDER BY ts
        LIMIT p_batch_size
    ), deleted AS (
        DELETE FROM metric_points point
        USING expired
        WHERE (point.task_id, point.key, point.step, point.ts) =
              (expired.task_id, expired.key, expired.step, expired.ts)
        RETURNING 1
    )
    SELECT count(*) INTO metric_points_deleted FROM deleted;

    WITH expired AS (
        SELECT created_at, task_id, event_seq
        FROM metric_events
        WHERE created_at < p_cutoff
        ORDER BY created_at
        LIMIT p_batch_size
    ), deleted AS (
        DELETE FROM metric_events event
        USING expired
        WHERE (event.created_at, event.task_id, event.event_seq) =
              (expired.created_at, expired.task_id, expired.event_seq)
        RETURNING 1
    )
    SELECT count(*) INTO metric_events_deleted FROM deleted;

    WITH expired AS (
        SELECT id
        FROM metric_outbox
        WHERE created_at < p_cutoff
          AND published_at IS NOT NULL
        ORDER BY id
        LIMIT p_batch_size
    ), deleted AS (
        DELETE FROM metric_outbox outbox
        USING expired
        WHERE outbox.id = expired.id
        RETURNING 1
    )
    SELECT count(*) INTO metric_outbox_deleted FROM deleted;

    RETURN NEXT;
END;
$$;
