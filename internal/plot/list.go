package plot

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"sync"
	"time"

	"github.com/PinableAgents/statsviz/history"
)

// IsReservedPlotName reports whether that name is reserved for Statsviz plots
// and thus can't be used for a user plot.
func IsReservedPlotName(name string) bool {
	if name == "timestamp" || name == "lastgc" {
		return true
	}
	registry := reg()
	return slices.ContainsFunc(registry.descriptions, func(pd description) bool {
		return nameFromLayout(pd.layout) == name
	})
}

func nameFromLayout(layout any) string {
	switch layout := layout.(type) {
	case Scatter:
		return layout.Name
	case Heatmap:
		return layout.Name
	default:
		panic(fmt.Sprintf("unknown plot layout type %T", layout))
	}
}

// getvalues extracts, from a sample of runtime metrics, a slice with all
// the metrics necessary for a single plot.
type getvalues func(time.Time, []metrics.Sample) any

// List holds all the plots that statsviz knows about. Some plots might be
// disabled, if they rely on metrics that are unknown to the current Go version.
type List struct {
	rtPlots   []runtimePlot
	userPlots []UserPlot

	once sync.Once // ensure Config is built once
	cfg  *Config

	reg *registry

	// samples 由当前 List 独占。runtime/metrics.Read 会改写传入的切片，
	// 多个 Server 共享 registry 的临时缓冲会产生数据竞争。
	samples []metrics.Sample

	collectMu   sync.Mutex
	runID       int64
	seq         int64
	lastCollect time.Time
}

type runtimePlot struct {
	name    string
	getvals getvalues
	layout  any // Scatter | Heatmap
}

func NewList(userPlots []UserPlot) (*List, error) {
	if name := hasDuplicatePlotNames(userPlots); name != "" {
		return nil, fmt.Errorf("duplicate plot name %s", name)
	}

	return &List{reg: reg(), userPlots: userPlots}, nil
}

func (pl *List) enabledPlots() []runtimePlot {
	plots := make([]runtimePlot, 0, len(pl.reg.descriptions))

	for _, plot := range pl.reg.descriptions {
		var layout any
		switch l := plot.layout.(type) {
		case Scatter:
			l.Metrics = plot.metrics
			layout = l
		case Heatmap:
			l.Metrics = plot.metrics
			layout = l
		default:
			layout = plot.layout
		}

		plots = append(plots, runtimePlot{
			name:    nameFromLayout(layout),
			getvals: plot.getvalues(),
			layout:  layout,
		})
	}

	return plots
}

func (pl *List) Config() *Config {
	pl.once.Do(func() {
		pl.rtPlots = pl.enabledPlots()
		pl.samples = pl.reg.newSamples()

		layouts := make([]any, len(pl.rtPlots))
		for i := range pl.rtPlots {
			layouts[i] = withDefaultAggregation(pl.rtPlots[i].layout)
		}

		pl.cfg = &Config{
			Events: []string{"lastgc"},
			Series: layouts,
		}

		// User plots go at the back.
		for i := range pl.userPlots {
			pl.cfg.Series = append(pl.cfg.Series, withDefaultAggregation(pl.userPlots[i].Layout()))
		}
	})
	return pl.cfg
}

func withDefaultAggregation(layout any) any {
	switch layout := layout.(type) {
	case Scatter:
		// Scatter 按值传递，但 Subplots 仍共享底层数组。填充默认值前先复制，
		// 确保多个 List 能安全复用同一份图表配置。
		layout.Subplots = slices.Clone(layout.Subplots)
		for i := range layout.Subplots {
			if layout.Subplots[i].Aggregation == "" {
				layout.Subplots[i].Aggregation = history.Gauge
			}
		}
		return layout
	case Heatmap:
		if layout.Aggregation == "" {
			layout.Aggregation = history.Histogram
		}
		return layout
	default:
		panic(fmt.Sprintf("unknown plot layout type %T", layout))
	}
}

// Aggregation 返回按实际 plot 名称索引的子序列聚合语义。
func (pl *List) Aggregation() map[string]history.SeriesSpec {
	cfg := pl.Config()
	aggregation := make(map[string]history.SeriesSpec, len(cfg.Series)+len(cfg.Events))
	for _, raw := range cfg.Series {
		switch layout := raw.(type) {
		case Scatter:
			kinds := make([]history.Kind, len(layout.Subplots))
			for i := range layout.Subplots {
				kinds[i] = layout.Subplots[i].Aggregation
			}
			aggregation[layout.Name] = history.SeriesSpec{Kinds: kinds}
		case Heatmap:
			aggregation[layout.Name] = history.SeriesSpec{
				Kinds:   []history.Kind{layout.Aggregation},
				Heatmap: true,
			}
		}
	}
	for _, event := range cfg.Events {
		aggregation[event] = history.SeriesSpec{Kinds: []history.Kind{history.Event}}
	}
	return aggregation
}

// SetRunID 设置后续快照的运行标识，并从新运行的首个序号开始计数。
func (pl *List) SetRunID(runID int64) {
	pl.collectMu.Lock()
	defer pl.collectMu.Unlock()
	pl.runID = runID
	pl.seq = 0
	pl.lastCollect = time.Time{}
}

// Snapshot 是一次采集得到的不可变 JSON 快照。
type Snapshot struct {
	RunID      int64
	Seq        int64
	Timestamp  int64
	DurationNS int64
	Series     json.RawMessage
}

// Collect 只读取一次所有指标，并保留整数的原始 JSON 表示。
func (pl *List) Collect() (Snapshot, error) {
	pl.Config()
	pl.collectMu.Lock()
	defer pl.collectMu.Unlock()

	metrics.Read(pl.samples)

	// lastgc time series is used as source to represent garbage collection
	// timestamps as vertical bars on certain plots.
	gcStats := debug.GCStats{}
	debug.ReadGCStats(&gcStats)

	// 在指标和 GC 状态读取完成后确定采样时间，避免并发发生的 GC 事件
	// 比当前快照的时间戳还晚。time.Time 的单调部分用于计算真实间隔。
	now := time.Now()
	durationNS := int64(0)
	if !pl.lastCollect.IsZero() {
		durationNS = now.Sub(pl.lastCollect).Nanoseconds()
		if durationNS < 0 {
			durationNS = 0
		}
	}
	pl.lastCollect = now
	pl.seq++

	m := map[string]any{
		// Javascript timestamps are in milliseconds.
		"lastgc": []int64{gcStats.LastGC.UnixMilli()},
	}
	for _, p := range pl.rtPlots {
		m[p.name] = p.getvals(now, pl.samples)
	}

	for i := range pl.userPlots {
		up := &pl.userPlots[i]
		switch {
		case up.Scatter != nil:
			vals := make([]float64, len(up.Scatter.Funcs))
			for i := range up.Scatter.Funcs {
				vals[i] = up.Scatter.Funcs[i]()
			}
			m[up.Scatter.Plot.Name] = vals
		case up.Heatmap != nil:
			panic("unimplemented")
		}
	}

	series, err := json.Marshal(m)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to convert metrics values to json: %w", err)
	}
	return Snapshot{
		RunID:      pl.runID,
		Seq:        pl.seq,
		Timestamp:  now.UnixMilli(),
		DurationNS: durationNS,
		Series:     series,
	}, nil
}

// WriteMetrics 将这个快照编码为兼容既有客户端的 metrics 消息。
func (s Snapshot) WriteMetrics(w io.Writer) error {
	type data struct {
		Series     json.RawMessage `json:"series"`
		Timestamp  int64           `json:"timestamp"`
		DurationNS int64           `json:"duration_ns"`
		RunID      int64           `json:"run_id"`
		Seq        int64           `json:"seq"`
	}
	if err := json.NewEncoder(w).Encode(struct {
		Event string `json:"event"`
		Data  data   `json:"data"`
	}{
		Event: "metrics",
		Data: data{
			Series:     s.Series,
			Timestamp:  s.Timestamp,
			DurationNS: s.DurationNS,
			RunID:      s.RunID,
			Seq:        s.Seq,
		},
	}); err != nil {
		return fmt.Errorf("failed to write metrics values as json: %w", err)
	}
	return nil
}

// WriteTo writes into w a JSON object containing the data points for all plots
// at the current instant. Return the number of written plots.
func (pl *List) WriteTo(w io.Writer) (int64, error) {
	snapshot, err := pl.Collect()
	if err != nil {
		return 0, err
	}
	if err := snapshot.WriteMetrics(w); err != nil {
		return 0, err
	}

	nplots := int64(len(pl.rtPlots) + len(pl.userPlots))
	return nplots, nil
}
