UPDATE metric_outbox
SET published_at = clock_timestamp(),
    claimed_until = NULL,
    claim_token = NULL
WHERE id = $1
  AND claim_token = $2::uuid
  AND published_at IS NULL;
