package domain

type MetricBatch struct {
	TaskID  string
	Samples []MetricSample
}

type MetricSample struct {
	Step            int64
	TimestampMillis int64
	Metrics         map[string]float64
}
