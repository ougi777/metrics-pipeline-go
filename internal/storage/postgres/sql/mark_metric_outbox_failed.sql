UPDATE metric_outbox
SET attempt_count = attempt_count + 1,
    next_attempt_at = clock_timestamp() + $3::interval,
    claimed_until = NULL,
    claim_token = NULL
WHERE id = $1
  AND claim_token = $2::uuid
  AND published_at IS NULL;
