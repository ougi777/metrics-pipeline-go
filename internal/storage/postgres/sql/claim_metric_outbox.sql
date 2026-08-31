WITH candidates AS (
    SELECT id
    FROM metric_outbox
    WHERE published_at IS NULL
      AND next_attempt_at <= clock_timestamp()
      AND (claimed_until IS NULL OR claimed_until <= clock_timestamp())
      AND NOT EXISTS (
          SELECT 1
          FROM metric_outbox AS previous
          WHERE previous.task_id = metric_outbox.task_id
            AND previous.event_seq < metric_outbox.event_seq
            AND previous.published_at IS NULL
      )
    ORDER BY id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
), claimed AS (
    UPDATE metric_outbox AS outbox
    SET claim_token = $2::uuid,
        claimed_until = clock_timestamp() + $3::interval
    FROM candidates
    WHERE outbox.id = candidates.id
    RETURNING outbox.id, outbox.task_id, outbox.event_seq, outbox.payload
)
SELECT id, task_id, event_seq, payload
FROM claimed
ORDER BY id;
