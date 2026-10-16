// Package history 定义实时指标历史存储的公共契约。
package history

import (
	"context"
	"encoding/json"
	"errors"
)

// Kind 描述子序列在时间桶内的聚合语义。
type Kind string

const (
	// Gauge 表示按持续时间加权的瞬时量。
	Gauge Kind = "gauge"
	// Rate 表示按持续时间加权且保留峰值的速率。
	Rate Kind = "rate"
	// Delta 表示在时间桶内求和的区间增量。
	Delta Kind = "delta"
	// Histogram 表示需要先对相邻累计值求差的直方图。
	Histogram Kind = "histogram"
	// Event 表示只需保留是否发生的事件标记。
	Event Kind = "event"
)

var (
	// ErrInvalidQuery 表示查询参数无效。
	ErrInvalidQuery = errors.New("history: invalid query")
	// ErrTooLarge 表示查询结果超过存储层的安全预算。
	ErrTooLarge = errors.New("history: result too large")
	// ErrClosed 表示存储已经关闭。
	ErrClosed = errors.New("history: store closed")
	// ErrQueueFull 表示入队时丢弃了最旧的待写样本。
	ErrQueueFull = errors.New("history: queue full; oldest sample dropped")
)

// SeriesSpec 描述一张图各子序列的聚合方式。
type SeriesSpec struct {
	Kinds   []Kind `json:"kinds"`
	Heatmap bool   `json:"heatmap"`
}

// RunConfig 是创建一次新运行所需的不可变配置。
type RunConfig struct {
	StartedAtMS int64                 `json:"started_at_ms"`
	IntervalNS  int64                 `json:"interval_ns"`
	Config      json.RawMessage       `json:"config"`
	Aggregation map[string]SeriesSpec `json:"aggregation"`
}

// Run 描述一次运行及其对应的历史配置。
type Run struct {
	ID          int64                 `json:"id"`
	StartedAtMS int64                 `json:"started_at_ms"`
	EndedAtMS   *int64                `json:"ended_at_ms"`
	IntervalNS  int64                 `json:"interval_ns"`
	ConfigHash  string                `json:"config_hash"`
	Config      json.RawMessage       `json:"config"`
	Aggregation map[string]SeriesSpec `json:"aggregation"`
}

// Sample 是一份不可拆分的原始采样快照。
type Sample struct {
	RunID      int64           `json:"run_id"`
	Seq        int64           `json:"seq"`
	Timestamp  int64           `json:"timestamp"`
	DurationNS int64           `json:"duration_ns"`
	Series     json.RawMessage `json:"series"`
}

// RangePoint 是区间查询返回的原始点或聚合点。
type RangePoint struct {
	RunID      int64           `json:"run_id"`
	Seq        int64           `json:"seq"`
	Timestamp  int64           `json:"timestamp"`
	DurationNS int64           `json:"duration_ns"`
	Series     json.RawMessage `json:"series"`
	Min        json.RawMessage `json:"min,omitempty"`
	Max        json.RawMessage `json:"max,omitempty"`
}

// Watermark 表示一次运行已经提交的最大序号。
type Watermark struct {
	RunID int64 `json:"run_id"`
	Seq   int64 `json:"seq"`
}

// Gap 描述一个已知的缺失序号与时间范围。
type Gap struct {
	RunID    int64  `json:"run_id"`
	From     int64  `json:"from"`
	To       int64  `json:"to"`
	FirstSeq int64  `json:"first_seq"`
	LastSeq  int64  `json:"last_seq"`
	Reason   string `json:"reason"`
}

// Status 描述历史写入器和数据库文件的当前状态。
type Status struct {
	LastCommitMS int64  `json:"last_commit_ms"`
	QueueDepth   int    `json:"queue_depth"`
	Dropped      int64  `json:"dropped"`
	LastError    string `json:"last_error"`
	DBBytes      int64  `json:"db_bytes"`
	WALBytes     int64  `json:"wal_bytes"`
}

// Meta 汇总历史范围、运行配置和持久化水位。
type Meta struct {
	Enabled     bool        `json:"enabled"`
	From        int64       `json:"from"`
	To          int64       `json:"to"`
	RetentionMS int64       `json:"retention_ms"`
	IntervalMS  int64       `json:"interval_ms"`
	Runs        []Run       `json:"runs"`
	Status      Status      `json:"status"`
	Watermarks  []Watermark `json:"watermarks"`
}

// RangeRequest 指定一个左闭右开的历史区间查询。
type RangeRequest struct {
	From      int64    `json:"from"`
	To        int64    `json:"to"`
	RunID     int64    `json:"run_id,omitempty"`
	MaxPoints int      `json:"max_points,omitempty"`
	Plots     []string `json:"plots,omitempty"`
}

// RangeResponse 返回按预算选择粒度后的历史点。
type RangeResponse struct {
	From       int64        `json:"from"`
	To         int64        `json:"to"`
	StepMS     int64        `json:"step_ms"`
	Points     []RangePoint `json:"points"`
	Runs       []Run        `json:"runs"`
	Gaps       []Gap        `json:"gaps"`
	Watermarks []Watermark  `json:"watermarks"`
}

// PointRequest 指定一个时间附近的原始快照查询。
type PointRequest struct {
	At          int64 `json:"at"`
	RunID       int64 `json:"run_id,omitempty"`
	ToleranceMS int64 `json:"tolerance_ms,omitempty"`
}

// PointResponse 返回距离请求时间最近的一份完整原始快照。
type PointResponse struct {
	Found       bool    `json:"found"`
	RequestedAt int64   `json:"requested_at"`
	OffsetMS    int64   `json:"offset_ms"`
	Sample      *Sample `json:"sample,omitempty"`
	Run         *Run    `json:"run,omitempty"`
}

// Store 是根包依赖的最小历史存储接口。
type Store interface {
	StartRun(context.Context, RunConfig) (Run, error)
	Enqueue(Sample) error
	Flush(context.Context) error
	Meta(context.Context) (Meta, error)
	Range(context.Context, RangeRequest) (RangeResponse, error)
	Point(context.Context, PointRequest) (PointResponse, error)
}
