SELECT event_seq, payload
FROM metric_events
WHERE task_id = $1
  AND created_at >= clock_timestamp() - interval '168 hours'
  AND event_seq > $2
ORDER BY event_seq;
