UPDATE metric_outbox
SET claimed_until = NULL,
    claim_token = NULL
WHERE id = $1
  AND claim_token = $2::uuid
  AND published_at IS NULL;
