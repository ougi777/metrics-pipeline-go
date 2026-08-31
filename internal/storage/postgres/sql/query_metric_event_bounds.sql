SELECT
    COALESCE((
        SELECT MIN(event_seq)
        FROM metric_events
        WHERE task_id = $1
          AND created_at >= clock_timestamp() - interval '168 hours'
    ), 0),
    GREATEST(
        COALESCE((
            SELECT MAX(event_seq)
            FROM metric_events
            WHERE task_id = $1
              AND created_at >= clock_timestamp() - interval '168 hours'
        ), 0),
        COALESCE((SELECT last_event_seq FROM task_event_counters WHERE task_id = $1), 0)
    );
