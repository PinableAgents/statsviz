package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/PinableAgents/statsviz/history"
)

const (
	maxCurvePoints      = 1500
	maxHeatmapPoints    = 400
	maxNumericCells     = 300_000
	maxResponseBytes    = 8 << 20
	maxMetadataRuns     = 4096
	maxMetadataGaps     = 10_000
	maxRunMetadataBytes = 1 << 20
	maxMetadataBytes    = maxResponseBytes
	metadataRowOverhead = 128
	rangeEnvelopeBytes  = 256
	queryTimeout        = 2 * time.Second
)

type readQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

const oldestRetainedSampleSQL = `
SELECT ts_ms FROM samples INDEXED BY samples_time
WHERE ts_ms >= ?
ORDER BY ts_ms, run_id, seq
LIMIT 1`

const newestRetainedSampleSQL = `
SELECT ts_ms FROM samples INDEXED BY samples_time
WHERE ts_ms >= ?
ORDER BY ts_ms DESC, run_id DESC, seq DESC
LIMIT 1`

const watermarkForRunSQL = `
SELECT seq FROM samples INDEXED BY sqlite_autoindex_samples_1
WHERE run_id = ? AND ts_ms >= ?
ORDER BY seq DESC
LIMIT 1`

// Meta 返回保留窗口内可查询的数据、仍被数据引用的历史配置和写入状态。
func (s *Store) Meta(ctx context.Context) (history.Meta, error) {
	if s.closed.Load() {
		return history.Meta{}, history.ErrClosed
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return history.Meta{}, fmt.Errorf("开始历史元信息查询: %w", err)
	}
	defer tx.Rollback()

	cutoff := s.now().Add(-s.retention).UnixMilli()
	meta := history.Meta{
		Enabled: true, RetentionMS: s.retention.Milliseconds(),
		Runs: []history.Run{}, Watermarks: []history.Watermark{},
	}
	if err = tx.QueryRowContext(ctx, oldestRetainedSampleSQL, cutoff).Scan(&meta.From); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return history.Meta{}, fmt.Errorf("读取历史起点: %w", err)
	}
	if err = tx.QueryRowContext(ctx, newestRetainedSampleSQL, cutoff).Scan(&meta.To); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return history.Meta{}, fmt.Errorf("读取历史终点: %w", err)
	}
	if meta.To != 0 {
		meta.To++
	}
	meta.Runs, err = readRuns(ctx, tx, cutoff, 0, 0, true)
	if err != nil {
		return history.Meta{}, err
	}
	if len(meta.Runs) > 0 {
		meta.IntervalMS = nanosecondsToMilliseconds(meta.Runs[len(meta.Runs)-1].IntervalNS)
	}
	meta.Watermarks, err = readWatermarks(ctx, tx, cutoff, meta.Runs)
	if err != nil {
		return history.Meta{}, err
	}
	if err = tx.Commit(); err != nil {
		return history.Meta{}, fmt.Errorf("完成历史元信息查询: %w", err)
	}
	meta.Status = s.status()
	encoded, err := json.Marshal(meta)
	if err != nil {
		return history.Meta{}, fmt.Errorf("编码历史元信息: %w", err)
	}
	if len(encoded) > maxMetadataBytes {
		return history.Meta{}, history.ErrTooLarge
	}
	return meta, nil
}

// Range 返回左闭右开区间；长窗口只读取持久化 rollup，最多在首尾各
// 读取一个 10 秒基础桶的原始样本，以免把范围外数据混入部分桶。
func (s *Store) Range(ctx context.Context, req history.RangeRequest) (history.RangeResponse, error) {
	empty := history.RangeResponse{Points: []history.RangePoint{}, Runs: []history.Run{}, Gaps: []history.Gap{}, Watermarks: []history.Watermark{}}
	if s.closed.Load() {
		return empty, history.ErrClosed
	}
	if req.From < 0 || req.To <= 0 || req.To > maxUnixMillis || req.From >= req.To || req.RunID < 0 || req.MaxPoints < 0 || req.MaxPoints > maxCurvePoints {
		return empty, fmt.Errorf("%w: 区间或点数无效", history.ErrInvalidQuery)
	}
	for _, plot := range req.Plots {
		if strings.TrimSpace(plot) == "" {
			return empty, fmt.Errorf("%w: 图名不能为空", history.ErrInvalidQuery)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	cutoff := s.now().Add(-s.retention).UnixMilli()
	from, to := req.From, req.To
	if from < cutoff {
		from = cutoff
	}
	if to <= cutoff {
		empty.From, empty.To = to, to
		return empty, nil
	}
	empty.From, empty.To = from, to

	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return empty, fmt.Errorf("开始历史区间查询: %w", err)
	}
	defer tx.Rollback()
	runs, err := readRuns(ctx, tx, cutoff, from, to, false)
	if err != nil {
		return empty, err
	}
	if req.RunID != 0 {
		runs = filterRuns(runs, req.RunID)
	}
	selected := makePlotSet(req.Plots)
	limit := req.MaxPoints
	if limit == 0 {
		limit = maxCurvePoints
	}
	if containsHeatmap(runs, req.Plots) && limit > maxHeatmapPoints {
		limit = maxHeatmapPoints
	}
	dimensions := estimateSeriesDimensions(runs, selected)
	rawLimit := pointBudgetForDimensions(limit, dimensions, 1)
	if rawLimit == 0 {
		return empty, history.ErrTooLarge
	}
	step := chooseStep(from, to, rawLimit, runs)
	limit = rawLimit
	if step != 0 {
		limit = pointBudgetForDimensions(limit, dimensions, 3)
		if limit == 0 {
			return empty, history.ErrTooLarge
		}
		step = chooseRollupStep(from, to, limit)
	}
	if step == 0 {
		var count int
		if req.RunID == 0 {
			err = tx.QueryRowContext(ctx, `
SELECT count(*) FROM samples
			WHERE ts_ms >= ? AND ts_ms < ?`, from, to).Scan(&count)
		} else {
			err = tx.QueryRowContext(ctx, `
SELECT count(*) FROM samples INDEXED BY samples_run_time
WHERE run_id = ? AND ts_ms >= ? AND ts_ms < ?`, req.RunID, from, to).Scan(&count)
		}
		if err != nil {
			return empty, fmt.Errorf("统计原始历史点: %w", err)
		}
		if count > limit {
			limit = pointBudgetForDimensions(limit, dimensions, 3)
			if limit == 0 {
				return empty, history.ErrTooLarge
			}
			step = chooseRollupStep(from, to, limit)
		}
	}

	gaps, err := readGaps(ctx, tx, from, to, req.RunID)
	if err != nil {
		return empty, err
	}
	watermarks, err := readWatermarks(ctx, tx, cutoff, runs)
	if err != nil {
		return empty, err
	}
	fixedMetadataBytes, err := rangeMetadataSize(runs, gaps, watermarks)
	if err != nil {
		return empty, err
	}
	if fixedMetadataBytes+rangeEnvelopeBytes > maxResponseBytes {
		return empty, history.ErrTooLarge
	}
	var response history.RangeResponse
	previousPoints := -1
	for attempts := 0; attempts < 8; attempts++ {
		var points []history.RangePoint
		var cells int
		if step == 0 {
			points, cells, err = readRawRange(ctx, tx, from, to, req.RunID, selected)
		} else {
			points, cells, err = s.readRollupRange(ctx, tx, from, to, req.RunID, step, selected, runs)
		}
		if err != nil {
			return empty, err
		}
		response = history.RangeResponse{From: from, To: to, StepMS: step, Points: points, Runs: runs, Gaps: gaps, Watermarks: watermarks}
		encoded, encodeErr := json.Marshal(points)
		if encodeErr != nil {
			return empty, fmt.Errorf("编码历史区间点: %w", encodeErr)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return empty, ctxErr
		}
		factor := budgetFactor(len(points), cells, fixedMetadataBytes+rangeEnvelopeBytes+len(encoded), limit)
		if factor == 1 {
			if err = tx.Commit(); err != nil {
				return empty, fmt.Errorf("完成历史区间查询: %w", err)
			}
			return response, nil
		}
		// 一个时间点仍超预算时，再加粗粒度也不能减少维度或响应体积。
		if previousPoints == len(points) && len(points) <= len(runs) {
			return empty, history.ErrTooLarge
		}
		previousPoints = len(points)
		step = nextCoarserStep(step, factor)
	}
	return empty, history.ErrTooLarge
}

func rangeMetadataSize(runs []history.Run, gaps []history.Gap, watermarks []history.Watermark) (int, error) {
	encoded, err := json.Marshal(struct {
		Runs       []history.Run       `json:"runs"`
		Gaps       []history.Gap       `json:"gaps"`
		Watermarks []history.Watermark `json:"watermarks"`
	}{Runs: runs, Gaps: gaps, Watermarks: watermarks})
	if err != nil {
		return 0, fmt.Errorf("编码历史区间元信息: %w", err)
	}
	return len(encoded), nil
}

func consumeMetadataBudget(used *int, sizes ...int) bool {
	remaining := maxMetadataBytes - *used
	for _, size := range sizes {
		if size < 0 || size > remaining {
			return false
		}
		remaining -= size
	}
	*used = maxMetadataBytes - remaining
	return true
}

func budgetFactor(points, cells, encodedBytes, pointLimit int) int64 {
	factor := int64(1)
	if points > pointLimit {
		factor = max64(factor, ceilDiv(int64(points), int64(pointLimit)))
	}
	if cells > maxNumericCells {
		factor = max64(factor, ceilDiv(int64(cells), maxNumericCells))
	}
	if encodedBytes > maxResponseBytes {
		factor = max64(factor, ceilDiv(int64(encodedBytes), maxResponseBytes))
	}
	return factor
}

const pointBeforeGlobalSQL = `
SELECT run_id, seq, ts_ms, duration_ns, payload
FROM samples INDEXED BY samples_time
WHERE ts_ms >= ? AND ts_ms <= ?
ORDER BY ts_ms DESC, run_id DESC, seq DESC
LIMIT 1`

const pointAfterGlobalSQL = `
SELECT run_id, seq, ts_ms, duration_ns, payload
FROM samples INDEXED BY samples_time
WHERE ts_ms >= ?
ORDER BY ts_ms, run_id, seq
LIMIT 1`

const pointBeforeRunSQL = `
SELECT run_id, seq, ts_ms, duration_ns, payload
FROM samples INDEXED BY samples_run_time
WHERE run_id = ? AND ts_ms >= ? AND ts_ms <= ?
ORDER BY ts_ms DESC, seq DESC
LIMIT 1`

const pointAfterRunSQL = `
SELECT run_id, seq, ts_ms, duration_ns, payload
FROM samples INDEXED BY samples_run_time
WHERE run_id = ? AND ts_ms >= ?
ORDER BY ts_ms, seq
LIMIT 1`

// Point 返回同一份原始快照，Series 不经过 float64 转换。
func (s *Store) Point(ctx context.Context, req history.PointRequest) (history.PointResponse, error) {
	response := history.PointResponse{RequestedAt: req.At}
	if s.closed.Load() {
		return response, history.ErrClosed
	}
	if req.At <= 0 || req.At > maxUnixMillis || req.RunID < 0 || req.ToleranceMS < 0 {
		return response, fmt.Errorf("%w: 时间点参数无效", history.ErrInvalidQuery)
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	cutoff := s.now().Add(-s.retention).UnixMilli()
	if req.At < cutoff {
		return response, nil
	}
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return response, fmt.Errorf("开始历史单点查询: %w", err)
	}
	defer tx.Rollback()
	candidates := make([]history.Sample, 0, 2)
	lowerBound := max64(cutoff, req.At)
	queries := []struct {
		statement string
		args      []any
	}{
		{pointBeforeGlobalSQL, []any{cutoff, req.At}},
		{pointAfterGlobalSQL, []any{lowerBound}},
	}
	if req.RunID != 0 {
		queries = []struct {
			statement string
			args      []any
		}{
			{pointBeforeRunSQL, []any{req.RunID, cutoff, req.At}},
			{pointAfterRunSQL, []any{req.RunID, lowerBound}},
		}
	}
	for _, query := range queries {
		var sample history.Sample
		err = tx.QueryRowContext(ctx, query.statement, query.args...).Scan(&sample.RunID, &sample.Seq, &sample.Timestamp, &sample.DurationNS, &sample.Series)
		if err == nil {
			candidates = append(candidates, sample)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return response, fmt.Errorf("读取历史单点候选: %w", err)
		}
	}
	if len(candidates) == 0 {
		return response, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		di, dj := abs64(candidates[i].Timestamp-req.At), abs64(candidates[j].Timestamp-req.At)
		if di != dj {
			return di < dj
		}
		return candidates[i].Timestamp < candidates[j].Timestamp
	})
	for _, sample := range candidates {
		run, runErr := readRun(ctx, tx, sample.RunID)
		if runErr != nil {
			return response, runErr
		}
		tolerance := req.ToleranceMS
		if tolerance == 0 {
			tolerance = saturatingMultiply(2, nanosecondsToMilliseconds(run.IntervalNS))
		}
		if abs64(sample.Timestamp-req.At) > tolerance {
			continue
		}
		var separates int
		lo, hi := req.At, sample.Timestamp
		if lo > hi {
			lo, hi = hi, lo
		}
		if err = tx.QueryRowContext(ctx, `
SELECT count(*) FROM gaps
WHERE run_id = ? AND started_at_ms < ? AND ended_at_ms > ?`, sample.RunID, hi+1, lo).Scan(&separates); err != nil {
			return response, fmt.Errorf("检查历史缺口: %w", err)
		}
		if separates != 0 {
			continue
		}
		response.Found = true
		response.OffsetMS = sample.Timestamp - req.At
		sample.Series = cloneBytes(sample.Series)
		response.Sample, response.Run = &sample, &run
		if err = tx.Commit(); err != nil {
			return history.PointResponse{RequestedAt: req.At}, err
		}
		return response, nil
	}
	return response, nil
}

func chooseStep(from, to int64, limit int, runs []history.Run) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	intervalMS := maxInt64
	for _, run := range runs {
		value := nanosecondsToMilliseconds(run.IntervalNS)
		if value < 1 {
			value = 1
		}
		if value < intervalMS {
			intervalMS = value
		}
	}
	if intervalMS == maxInt64 {
		intervalMS = 1000
	}
	if ceilDiv(to-from, intervalMS) <= int64(limit) {
		return 0
	}
	return chooseRollupStep(from, to, limit)
}

func chooseRollupStep(from, to int64, limit int) int64 {
	duration := to - from
	for _, step := range rollupSteps {
		if ceilDiv(duration, step) <= int64(limit) {
			return step
		}
	}
	multiple := ceilDiv(duration, int64(limit)*rollupSteps[len(rollupSteps)-1])
	if multiple < 1 {
		multiple = 1
	}
	return multiple * rollupSteps[len(rollupSteps)-1]
}

func nextCoarserStep(step, factor int64) int64 {
	if step == 0 {
		return rollupSteps[0]
	}
	if factor < 2 {
		factor = 2
	}
	target := saturatingMultiply(step, factor)
	for _, candidate := range rollupSteps {
		if candidate > step && candidate >= target {
			return candidate
		}
	}
	base := rollupSteps[len(rollupSteps)-1]
	return saturatingMultiply(ceilDiv(target, base), base)
}

type aggregateComponent struct {
	runID int64
	from  int64
	built builtAggregate
}

const aggregateComponentsGlobalSQL = `
SELECT run_id, bucket_start_ms, first_seq, last_seq, sample_count, covered_ns, payload
FROM rollups INDEXED BY rollups_time
WHERE step_ms = ? AND bucket_start_ms >= ? AND bucket_start_ms < ?
ORDER BY bucket_start_ms, run_id`

const aggregateComponentsRunSQL = `
SELECT run_id, bucket_start_ms, first_seq, last_seq, sample_count, covered_ns, payload
FROM rollups INDEXED BY sqlite_autoindex_rollups_1
WHERE run_id = ? AND step_ms = ? AND bucket_start_ms >= ? AND bucket_start_ms < ?
ORDER BY bucket_start_ms`

func effectiveAggregateRunID(runID int64, runs []history.Run) int64 {
	if runID == 0 && len(runs) == 1 {
		return runs[0].ID
	}
	return runID
}

func (s *Store) readRollupRange(ctx context.Context, tx *sql.Tx, from, to, runID, step int64, plots map[string]struct{}, runs []history.Run) ([]history.RangePoint, int, error) {
	level := len(rollupSteps) - 1
	for index, candidate := range rollupSteps {
		if step <= candidate {
			level = index
			break
		}
	}
	components, err := s.collectAggregateComponents(ctx, tx, from, to, runID, level, runs)
	if err != nil {
		return nil, 0, err
	}
	sort.Slice(components, func(i, j int) bool {
		if components[i].runID != components[j].runID {
			return components[i].runID < components[j].runID
		}
		return components[i].from < components[j].from
	})
	type pointKey struct{ runID, start int64 }
	aggregates := make(map[pointKey]builtAggregate)
	for _, c := range components {
		key := pointKey{runID: c.runID, start: alignTime(c.from, step)}
		value := aggregates[key]
		mergeBuilt(&value, c.built)
		aggregates[key] = value
	}
	keys := make([]pointKey, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].start != keys[j].start {
			return keys[i].start < keys[j].start
		}
		return keys[i].runID < keys[j].runID
	})
	points := make([]history.RangePoint, 0, len(keys))
	numericCells := 0
	for _, key := range keys {
		built := aggregates[key]
		series, minimum, maximum, cells, err := materialize(built.payload, plots)
		if err != nil {
			return nil, 0, err
		}
		timestamp := max64(key.start, from)
		points = append(points, history.RangePoint{RunID: key.runID, Seq: built.lastSeq, Timestamp: timestamp, DurationNS: built.coveredNS, Series: series, Min: minimum, Max: maximum})
		numericCells += cells
	}
	return points, numericCells, nil
}

// collectAggregateComponents 用当前层的完整桶覆盖中段，并把首尾部分递归
// 交给下一层。只有 10 秒层仍无法完整覆盖的不足 10 秒片段才读取 raw。
func (s *Store) collectAggregateComponents(ctx context.Context, tx *sql.Tx, from, to, runID int64, level int, runs []history.Run) ([]aggregateComponent, error) {
	if from >= to {
		return []aggregateComponent{}, nil
	}
	base := rollupSteps[level]
	fullFrom := ceilAligned(from, base)
	fullTo := alignTime(to, base)
	components := make([]aggregateComponent, 0)
	if fullFrom < fullTo {
		var rows *sql.Rows
		var err error
		effectiveRunID := effectiveAggregateRunID(runID, runs)
		if effectiveRunID == 0 {
			rows, err = tx.QueryContext(ctx, aggregateComponentsGlobalSQL, base, fullFrom, fullTo)
		} else {
			rows, err = tx.QueryContext(ctx, aggregateComponentsRunSQL, effectiveRunID, base, fullFrom, fullTo)
		}
		if err != nil {
			return nil, fmt.Errorf("读取持久化聚合: %w", err)
		}
		for rows.Next() {
			var component aggregateComponent
			var raw []byte
			if err = rows.Scan(&component.runID, &component.from, &component.built.firstSeq, &component.built.lastSeq, &component.built.sampleCount, &component.built.coveredNS, &raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			component.built.payload, err = decodeAggregatePayload(raw)
			if err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("解码持久化聚合: %w", err)
			}
			components = append(components, component)
		}
		if err = rows.Close(); err != nil {
			return nil, err
		}
	}
	segments := boundarySegments(from, to, fullFrom, fullTo)
	for _, segment := range segments {
		if level > 0 {
			lower, err := s.collectAggregateComponents(ctx, tx, segment[0], segment[1], runID, level-1, runs)
			if err != nil {
				return nil, err
			}
			components = append(components, lower...)
			continue
		}
		for _, run := range runs {
			if runID != 0 && run.ID != runID {
				continue
			}
			rows, err := readRowsForAggregate(ctx, tx, run.ID, segment[0], segment[1])
			if err != nil {
				return nil, err
			}
			built, err := buildAggregate(rows, run.Aggregation, segment[0], segment[1])
			if err != nil {
				return nil, err
			}
			if built.sampleCount > 0 {
				components = append(components, aggregateComponent{runID: run.ID, from: segment[0], built: built})
			}
		}
	}
	return components, nil
}

func boundarySegments(from, to, fullFrom, fullTo int64) [][2]int64 {
	segments := make([][2]int64, 0, 2)
	leftTo := min64(fullFrom, to)
	if from < leftTo {
		segments = append(segments, [2]int64{from, leftTo})
	}
	rightFrom := max64(fullTo, from)
	if rightFrom < to && (len(segments) == 0 || segments[len(segments)-1][1] <= rightFrom) {
		segments = append(segments, [2]int64{rightFrom, to})
	}
	return segments
}

func mergeBuilt(target *builtAggregate, source builtAggregate) {
	mergePayload(&target.payload, source.payload)
	if target.sampleCount == 0 || source.firstSeq < target.firstSeq {
		target.firstSeq = source.firstSeq
	}
	if source.lastSeq > target.lastSeq {
		target.lastSeq = source.lastSeq
	}
	target.sampleCount += source.sampleCount
	target.coveredNS += source.coveredNS
}

func readRawRange(ctx context.Context, queryer readQuerier, from, to, runID int64, plots map[string]struct{}) ([]history.RangePoint, int, error) {
	rows, err := queryer.QueryContext(ctx, `
SELECT run_id, seq, ts_ms, duration_ns, payload
FROM samples
WHERE ts_ms >= ? AND ts_ms < ? AND (? = 0 OR run_id = ?)
ORDER BY ts_ms, run_id, seq`, from, to, runID, runID)
	if err != nil {
		return nil, 0, fmt.Errorf("读取原始历史区间: %w", err)
	}
	defer rows.Close()
	points := []history.RangePoint{}
	cells := 0
	for rows.Next() {
		var point history.RangePoint
		if err = rows.Scan(&point.RunID, &point.Seq, &point.Timestamp, &point.DurationNS, &point.Series); err != nil {
			return nil, 0, err
		}
		point.Series, cells, err = filterRawSeries(point.Series, plots, cells)
		if err != nil {
			return nil, 0, err
		}
		points = append(points, point)
	}
	return points, cells, rows.Err()
}

func filterRawSeries(raw json.RawMessage, plots map[string]struct{}, cells int) (json.RawMessage, int, error) {
	var source map[string]json.RawMessage
	if err := json.Unmarshal(raw, &source); err != nil {
		return nil, cells, err
	}
	result := source
	if len(plots) > 0 {
		result = make(map[string]json.RawMessage, len(plots))
		for plot := range plots {
			if value, ok := source[plot]; ok {
				result[plot] = value
			}
		}
	}
	for _, value := range result {
		count, err := countNumbers(value)
		if err != nil {
			return nil, cells, err
		}
		cells += count
	}
	if len(plots) == 0 {
		return cloneBytes(raw), cells, nil
	}
	encoded, err := json.Marshal(result)
	return encoded, cells, err
}

func countNumbers(raw []byte) (int, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return 0, err
	}
	var walk func(any) int
	walk = func(current any) int {
		switch typed := current.(type) {
		case json.Number:
			return 1
		case []any:
			total := 0
			for _, item := range typed {
				total += walk(item)
			}
			return total
		case map[string]any:
			total := 0
			for _, item := range typed {
				total += walk(item)
			}
			return total
		default:
			return 0
		}
	}
	return walk(value), nil
}

func readRuns(ctx context.Context, queryer readQuerier, cutoff, from, to int64, meta bool) ([]history.Run, error) {
	condition := `
r.ended_at_ms IS NULL
OR EXISTS (SELECT 1 FROM samples s INDEXED BY samples_run_time WHERE s.run_id = r.id AND s.ts_ms >= ? LIMIT 1)
OR EXISTS (SELECT 1 FROM rollups u WHERE u.run_id = r.id AND u.bucket_start_ms + u.step_ms > ? LIMIT 1)
OR EXISTS (SELECT 1 FROM gaps g WHERE g.run_id = r.id AND g.ended_at_ms > ? LIMIT 1)`
	args := []any{cutoff, cutoff, cutoff}
	if !meta {
		// 运行的 started/ended 是墙钟元数据，发生时钟回拨时不能代表
		// 实际样本覆盖；按持久化样本、rollup 或 gap 的真实时间选择。
		condition = `
EXISTS (SELECT 1 FROM samples s INDEXED BY samples_run_time
        WHERE s.run_id = r.id AND s.ts_ms >= ? AND s.ts_ms < ? LIMIT 1)
OR EXISTS (SELECT 1 FROM rollups u
           WHERE u.run_id = r.id AND u.bucket_start_ms < ?
             AND u.bucket_start_ms + u.step_ms > ? LIMIT 1)
OR EXISTS (SELECT 1 FROM gaps g
           WHERE g.run_id = r.id AND g.started_at_ms < ? AND g.ended_at_ms > ? LIMIT 1)`
		args = []any{from, to, to, from, to, from}
	}
	args = append(args, maxMetadataRuns+1)
	rows, err := queryer.QueryContext(ctx, `
SELECT r.id, r.started_at_ms, r.ended_at_ms, r.sample_interval_ns,
       r.config_hash, r.config_json, r.aggregation_json
FROM runs r WHERE `+condition+` ORDER BY r.started_at_ms, r.id LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("读取历史运行配置: %w", err)
	}
	defer rows.Close()
	runs := []history.Run{}
	metadataBytes := 0
	for rows.Next() {
		var run history.Run
		var ended sql.NullInt64
		var config, aggregation []byte
		if err = rows.Scan(&run.ID, &run.StartedAtMS, &ended, &run.IntervalNS, &run.ConfigHash, &config, &aggregation); err != nil {
			return nil, err
		}
		if len(runs) >= maxMetadataRuns {
			return nil, history.ErrTooLarge
		}
		if len(config) > maxRunMetadataBytes || len(aggregation) > maxRunMetadataBytes-len(config) {
			return nil, history.ErrTooLarge
		}
		if !consumeMetadataBudget(&metadataBytes, len(config), len(aggregation), len(run.ConfigHash), metadataRowOverhead) {
			return nil, history.ErrTooLarge
		}
		if ended.Valid {
			value := ended.Int64
			run.EndedAtMS = &value
		}
		if err = json.Unmarshal(aggregation, &run.Aggregation); err != nil {
			return nil, fmt.Errorf("解码运行 %d 聚合配置: %w", run.ID, err)
		}
		run.Config = cloneBytes(config)
		run.Aggregation = cloneAggregation(run.Aggregation)
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func readRun(ctx context.Context, queryer readQuerier, id int64) (history.Run, error) {
	var run history.Run
	var ended sql.NullInt64
	var config, aggregation []byte
	err := queryer.QueryRowContext(ctx, `
SELECT id, started_at_ms, ended_at_ms, sample_interval_ns, config_hash, config_json, aggregation_json
FROM runs WHERE id = ?`, id).Scan(&run.ID, &run.StartedAtMS, &ended, &run.IntervalNS, &run.ConfigHash, &config, &aggregation)
	if err != nil {
		return run, fmt.Errorf("读取运行 %d: %w", id, err)
	}
	if ended.Valid {
		value := ended.Int64
		run.EndedAtMS = &value
	}
	if len(config) > maxRunMetadataBytes || len(aggregation) > maxRunMetadataBytes-len(config) {
		return run, history.ErrTooLarge
	}
	if err = json.Unmarshal(aggregation, &run.Aggregation); err != nil {
		return run, err
	}
	run.Config = cloneBytes(config)
	return run, nil
}

func readWatermarks(ctx context.Context, queryer readQuerier, cutoff int64, runs []history.Run) ([]history.Watermark, error) {
	result := make([]history.Watermark, 0, len(runs))
	for _, run := range runs {
		var seq int64
		err := queryer.QueryRowContext(ctx, watermarkForRunSQL, run.ID, cutoff).Scan(&seq)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("读取运行 %d 历史水位: %w", run.ID, err)
		}
		result = append(result, history.Watermark{RunID: run.ID, Seq: seq})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RunID < result[j].RunID })
	return result, nil
}

func readGaps(ctx context.Context, queryer readQuerier, from, to, runID int64) ([]history.Gap, error) {
	rows, err := queryer.QueryContext(ctx, `
SELECT run_id, first_seq, last_seq, max(started_at_ms, ?), min(ended_at_ms, ?), reason
FROM gaps
WHERE started_at_ms < ? AND ended_at_ms > ? AND (? = 0 OR run_id = ?)
ORDER BY started_at_ms, run_id, first_seq
LIMIT ?`, from, to, to, from, runID, runID, maxMetadataGaps+1)
	if err != nil {
		return nil, fmt.Errorf("读取历史缺口: %w", err)
	}
	defer rows.Close()
	result := []history.Gap{}
	metadataBytes := 0
	for rows.Next() {
		var gap history.Gap
		if err = rows.Scan(&gap.RunID, &gap.FirstSeq, &gap.LastSeq, &gap.From, &gap.To, &gap.Reason); err != nil {
			return nil, err
		}
		if len(result) >= maxMetadataGaps {
			return nil, history.ErrTooLarge
		}
		if !consumeMetadataBudget(&metadataBytes, len(gap.Reason), metadataRowOverhead) {
			return nil, history.ErrTooLarge
		}
		result = append(result, gap)
	}
	return result, rows.Err()
}

func (s *Store) status() history.Status {
	s.stateMu.RLock()
	status := history.Status{LastCommitMS: s.lastCommitMS, LastError: s.lastError}
	s.stateMu.RUnlock()
	s.queueMu.Lock()
	status.QueueDepth = len(s.queue)
	status.Dropped = s.dropped
	s.queueMu.Unlock()
	if info, err := os.Stat(s.path); err == nil {
		status.DBBytes = info.Size()
	}
	if info, err := os.Stat(s.path + "-wal"); err == nil {
		status.WALBytes = info.Size()
	}
	return status
}

func containsHeatmap(runs []history.Run, plots []string) bool {
	selected := makePlotSet(plots)
	for _, run := range runs {
		for name, spec := range run.Aggregation {
			if spec.Heatmap {
				if len(selected) == 0 {
					return true
				}
				if _, ok := selected[name]; ok {
					return true
				}
			}
		}
	}
	return false
}

type storedConfigShape struct {
	Series []struct {
		Name     string            `json:"name"`
		Subplots []json.RawMessage `json:"subplots"`
		Buckets  []json.RawMessage `json:"buckets"`
	} `json:"series"`
	Events []string `json:"events"`
}

// estimateSeriesDimensions 从持久化配置估算每个时间列的数值维度。
// 普通曲线使用 subplots，热力图使用 buckets；无法识别的旧配置回退到
// aggregation kinds。取各运行的最大值，避免配置切换时低估单点成本。
func estimateSeriesDimensions(runs []history.Run, selected map[string]struct{}) int {
	maximum := 0
	for _, run := range runs {
		dimensions := make(map[string]int, len(run.Aggregation))
		for name, spec := range run.Aggregation {
			count := len(spec.Kinds)
			if count == 0 {
				count = 1
			}
			dimensions[name] = count
		}
		var shape storedConfigShape
		if json.Unmarshal(run.Config, &shape) == nil {
			for _, series := range shape.Series {
				count := len(series.Subplots)
				if len(series.Buckets) > count {
					count = len(series.Buckets)
				}
				if count == 0 {
					count = 1
				}
				dimensions[series.Name] = count
			}
			for _, event := range shape.Events {
				dimensions[event] = 1
			}
		}
		total := 0
		for name, count := range dimensions {
			if len(selected) > 0 {
				if _, ok := selected[name]; !ok {
					continue
				}
			}
			total += count
		}
		if total > maximum {
			maximum = total
		}
	}
	return maximum
}

func pointBudgetForDimensions(limit, dimensions, valuesPerDimension int) int {
	if dimensions <= 0 {
		return limit
	}
	if dimensions > maxNumericCells/valuesPerDimension {
		return 0
	}
	cellLimit := maxNumericCells / (dimensions * valuesPerDimension)
	if cellLimit < limit {
		return cellLimit
	}
	return limit
}

func filterRuns(runs []history.Run, id int64) []history.Run {
	result := make([]history.Run, 0, 1)
	for _, run := range runs {
		if run.ID == id {
			result = append(result, run)
		}
	}
	return result
}

func makePlotSet(plots []string) map[string]struct{} {
	result := make(map[string]struct{}, len(plots))
	for _, plot := range plots {
		result[plot] = struct{}{}
	}
	return result
}

func ceilAligned(value, step int64) int64 {
	aligned := alignTime(value, step)
	if aligned < value {
		if aligned > int64(^uint64(0)>>1)-step {
			return int64(^uint64(0) >> 1)
		}
		aligned += step
	}
	return aligned
}

func ceilDiv(value, divisor int64) int64 {
	if value <= 0 || divisor <= 0 {
		return 0
	}
	quotient := value / divisor
	if value%divisor != 0 {
		quotient++
	}
	return quotient
}

func saturatingMultiply(left, right int64) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > maxInt64/right {
		return maxInt64
	}
	return left * right
}

func nanosecondsToMilliseconds(value int64) int64 {
	return ceilDiv(value, int64(time.Millisecond))
}
func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
func abs64(value int64) int64 {
	const minInt64 = -int64(^uint64(0)>>1) - 1
	if value == minInt64 {
		return int64(^uint64(0) >> 1)
	}
	if value < 0 {
		return -value
	}
	return value
}
