package sqlite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/PinableAgents/statsviz/history"
)

var rollupSteps = [...]int64{10_000, 60_000, 600_000}

type sampleRow struct {
	runID      int64
	seq        int64
	timestamp  int64
	durationNS int64
	series     json.RawMessage
}

type aggregateCell struct {
	Kind        history.Kind `json:"kind"`
	WeightedSum float64      `json:"weighted_sum,omitempty"`
	WeightNS    int64        `json:"weight_ns,omitempty"`
	Sum         float64      `json:"sum,omitempty"`
	Min         float64      `json:"min,omitempty"`
	Max         float64      `json:"max,omitempty"`
	Last        float64      `json:"last,omitempty"`
	Count       int64        `json:"count,omitempty"`
	HasValue    bool         `json:"has_value"`
}

type aggregatePayload struct {
	Plots map[string][]aggregateCell `json:"plots"`
}

type builtAggregate struct {
	payload     aggregatePayload
	firstSeq    int64
	lastSeq     int64
	sampleCount int64
	coveredNS   int64
}

func decodeSeries(raw json.RawMessage) (map[string][]*float64, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var source map[string][]any
	if err := decoder.Decode(&source); err != nil {
		return nil, err
	}
	result := make(map[string][]*float64, len(source))
	for plot, values := range source {
		converted := make([]*float64, len(values))
		for index, value := range values {
			if value == nil {
				continue
			}
			number, ok := value.(json.Number)
			if !ok {
				return nil, fmt.Errorf("图 %q 的第 %d 个值不是数字", plot, index)
			}
			parsed, err := strconv.ParseFloat(number.String(), 64)
			if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
				return nil, fmt.Errorf("图 %q 的第 %d 个值无效", plot, index)
			}
			converted[index] = &parsed
		}
		result[plot] = converted
	}
	return result, nil
}

func kindAt(spec history.SeriesSpec, index int) history.Kind {
	if len(spec.Kinds) == 0 {
		return history.Gauge
	}
	if len(spec.Kinds) == 1 {
		return normalizedKind(spec.Kinds[0])
	}
	if index < len(spec.Kinds) {
		return normalizedKind(spec.Kinds[index])
	}
	return history.Gauge
}

func normalizedKind(kind history.Kind) history.Kind {
	switch kind {
	case history.Gauge, history.Rate, history.Delta, history.Histogram, history.Event:
		return kind
	default:
		return history.Gauge
	}
}

func buildAggregate(rows []sampleRow, specs map[string]history.SeriesSpec, from, to int64) (builtAggregate, error) {
	result := builtAggregate{payload: aggregatePayload{Plots: map[string][]aggregateCell{}}}
	previous := map[string][]*float64{}
	var previousSeq int64
	for _, row := range rows {
		decoded, err := decodeSeries(row.series)
		if err != nil {
			return result, fmt.Errorf("解码运行 %d 样本 %d: %w", row.runID, row.seq, err)
		}
		inside := row.timestamp >= from && row.timestamp < to
		if inside {
			if result.sampleCount == 0 {
				result.firstSeq = row.seq
			}
			result.lastSeq = row.seq
			result.sampleCount++
			if row.durationNS > 0 {
				result.coveredNS += row.durationNS
			}
		}
		for plot, values := range decoded {
			spec := specs[plot]
			cells := result.payload.Plots[plot]
			if len(cells) < len(values) {
				cells = append(cells, make([]aggregateCell, len(values)-len(cells))...)
			}
			for index, pointer := range values {
				kind := kindAt(spec, index)
				if cells[index].Kind == "" {
					cells[index].Kind = kind
				}
				if !inside || pointer == nil {
					continue
				}
				value := *pointer
				if kind == history.Histogram {
					prior := previous[plot]
					if previousSeq+1 != row.seq || index >= len(prior) || prior[index] == nil || value < *prior[index] {
						continue
					}
					value -= *prior[index]
				}
				cell := &cells[index]
				if !cell.HasValue {
					cell.Min, cell.Max = value, value
				} else {
					cell.Min = math.Min(cell.Min, value)
					cell.Max = math.Max(cell.Max, value)
				}
				cell.HasValue = true
				cell.Count++
				cell.Last = value
				switch kind {
				case history.Gauge, history.Rate:
					weight := row.durationNS
					if weight <= 0 {
						weight = 1
					}
					cell.WeightedSum += value * float64(weight)
					cell.WeightNS += weight
				case history.Delta, history.Histogram:
					cell.Sum += value
				case history.Event:
					// 事件序列（例如 lastgc）携带的值本身就是真实事件时间。
					// Last 由时间有序的合并过程保留，不能替换成布尔标记。
				}
			}
			result.payload.Plots[plot] = cells
		}
		previous = decoded
		previousSeq = row.seq
	}
	maxCoverage := (to - from) * int64(1_000_000)
	if result.coveredNS > maxCoverage {
		result.coveredNS = maxCoverage
	}
	return result, nil
}

func mergePayload(target *aggregatePayload, source aggregatePayload) {
	if target.Plots == nil {
		target.Plots = map[string][]aggregateCell{}
	}
	for plot, incoming := range source.Plots {
		cells := target.Plots[plot]
		if len(cells) < len(incoming) {
			cells = append(cells, make([]aggregateCell, len(incoming)-len(cells))...)
		}
		for index, next := range incoming {
			if !next.HasValue {
				if cells[index].Kind == "" {
					cells[index].Kind = next.Kind
				}
				continue
			}
			current := &cells[index]
			if !current.HasValue {
				*current = next
				continue
			}
			current.WeightedSum += next.WeightedSum
			current.WeightNS += next.WeightNS
			current.Sum += next.Sum
			current.Min = math.Min(current.Min, next.Min)
			current.Max = math.Max(current.Max, next.Max)
			current.Last = next.Last
			current.Count += next.Count
		}
		target.Plots[plot] = cells
	}
}

func materialize(payload aggregatePayload, plots map[string]struct{}) (json.RawMessage, json.RawMessage, json.RawMessage, int, error) {
	series := make(map[string][]any)
	minimum := make(map[string][]any)
	maximum := make(map[string][]any)
	numericCells := 0
	for plot, cells := range payload.Plots {
		if len(plots) > 0 {
			if _, wanted := plots[plot]; !wanted {
				continue
			}
		}
		values := make([]any, len(cells))
		mins := make([]any, len(cells))
		maxs := make([]any, len(cells))
		for index, cell := range cells {
			if !cell.HasValue {
				continue
			}
			switch cell.Kind {
			case history.Gauge, history.Rate:
				if cell.WeightNS > 0 {
					values[index] = cell.WeightedSum / float64(cell.WeightNS)
				}
			case history.Delta, history.Histogram:
				values[index] = cell.Sum
			case history.Event:
				values[index] = cell.Last
			}
			mins[index], maxs[index] = cell.Min, cell.Max
			numericCells += 3
		}
		series[plot], minimum[plot], maximum[plot] = values, mins, maxs
	}
	seriesJSON, err := json.Marshal(series)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	minJSON, err := json.Marshal(minimum)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	maxJSON, err := json.Marshal(maximum)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	return seriesJSON, minJSON, maxJSON, numericCells, nil
}
