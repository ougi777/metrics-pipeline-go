SELECT
    point_partitions_dropped,
    event_partitions_dropped,
    metric_points_deleted,
    metric_events_deleted,
    metric_outbox_deleted
FROM maintain_metric_retention($1::timestamptz, $2::integer)
