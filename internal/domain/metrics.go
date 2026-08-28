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

// MetricPoint 是从一个采样点的 metrics map 中展开出的单条指标记录。
type MetricPoint struct {
	TaskID          string
	Key             string
	Step            int64
	TimestampMillis int64
	Value           float64
}
