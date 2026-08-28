INSERT INTO task_event_counters (task_id)
SELECT DISTINCT task_id
FROM unnest($1::varchar[]) AS tasks(task_id)
ON CONFLICT (task_id) DO NOTHING;
