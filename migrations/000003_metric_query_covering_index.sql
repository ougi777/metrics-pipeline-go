CREATE INDEX metric_points_task_key_time_desc_covering
    ON metric_points (task_id, key, ts DESC, step DESC)
    INCLUDE (value);
