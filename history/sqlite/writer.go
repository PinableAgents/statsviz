package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/PinableAgents/statsviz/history"
)

type bucketKey struct {
	runID int64
	step  int64
	start int64
}

func (s *Store) writer() {
	flushTicker := time.NewTicker(s.flushInterval)
	cleanupTicker := time.NewTicker(time.Minute)
	defer flushTicker.Stop()
	defer cleanupTicker.Stop()
	defer close(s.done)

	var pending []history.Sample
	var pendingGaps []queuedGap
	for {
		select {
		case <-s.wake:
			if len(pending) == 0 && s.queueDepth() >= batchSize {
				pending = s.takeSamples(batchSize)
				pendingGaps = s.takeGaps()
			}
			if len(pending) > 0 || len(pendingGaps) > 0 {
				if err := s.commitBatch(context.Background(), pending, pendingGaps); err != nil {
					s.setWriteError(err)
				} else {
					pending, pendingGaps = nil, nil
					s.markCommit()
					if s.queueDepth() >= batchSize {
						s.signalWriter()
					}
				}
			}
		case <-flushTicker.C:
			if len(pending) == 0 {
				pending = s.takeSamples(batchSize)
				pendingGaps = s.takeGaps()
			}
			if len(pending) > 0 || len(pendingGaps) > 0 {
				if err := s.commitBatch(context.Background(), pending, pendingGaps); err != nil {
					s.setWriteError(err)
				} else {
					pending, pendingGaps = nil, nil
					s.markCommit()
				}
			}
		case <-cleanupTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := s.cleanup(ctx)
			if err == nil {
				_, _ = s.writeDB.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
				_, _ = s.writeDB.ExecContext(ctx, "PRAGMA incremental_vacuum(128)")
			} else {
				s.setWriteError(err)
			}
			cancel()
		case req := <-s.request:
			err := s.flushPending(&pending, &pendingGaps)
			req.done <- err
			if req.close {
				return
			}
		}
	}
}

func (s *Store) flushPending(pending *[]history.Sample, gaps *[]queuedGap) error {
	for {
		if len(*pending) == 0 {
			*pending = s.takeSamples(batchSize)
			*gaps = s.takeGaps()
		}
		if len(*pending) == 0 && len(*gaps) == 0 {
			return nil
		}
		if err := s.commitBatch(context.Background(), *pending, *gaps); err != nil {
			s.setWriteError(err)
			return err
		}
		*pending, *gaps = nil, nil
		s.markCommit()
	}
}

func (s *Store) queueDepth() int {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	return len(s.queue)
}

func (s *Store) takeSamples(limit int) []history.Sample {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if limit > len(s.queue) {
		limit = len(s.queue)
	}
	result := append([]history.Sample(nil), s.queue[:limit]...)
	copy(s.queue, s.queue[limit:])
	s.queue = s.queue[:len(s.queue)-limit]
	return result
}

func (s *Store) takeGaps() []queuedGap {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	result := append([]queuedGap(nil), s.gaps...)
	s.gaps = s.gaps[:0]
	return result
}

func (s *Store) commitBatch(ctx context.Context, samples []history.Sample, gaps []queuedGap) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开始历史写入事务: %w", err)
	}
	defer tx.Rollback()

	for _, sample := range samples {
		if _, err = tx.ExecContext(ctx, `
INSERT INTO samples(run_id, seq, ts_ms, duration_ns, codec, payload)
VALUES(?, ?, ?, ?, 'json', ?)
ON CONFLICT(run_id, seq) DO UPDATE SET
    ts_ms = excluded.ts_ms,
    duration_ns = excluded.duration_ns,
    codec = excluded.codec,
    payload = excluded.payload`, sample.RunID, sample.Seq, sample.Timestamp, sample.DurationNS, []byte(sample.Series)); err != nil {
			return fmt.Errorf("写入运行 %d 样本 %d: %w", sample.RunID, sample.Seq, err)
		}
	}
	for _, gap := range gaps {
		if _, err = tx.ExecContext(ctx, `
INSERT INTO gaps(run_id, first_seq, last_seq, started_at_ms, ended_at_ms, reason)
VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(run_id, first_seq) DO UPDATE SET
    last_seq = max(last_seq, excluded.last_seq),
    ended_at_ms = max(ended_at_ms, excluded.ended_at_ms),
    reason = excluded.reason`, gap.RunID, gap.FirstSeq, gap.LastSeq, gap.From, gap.To, gap.Reason); err != nil {
			return fmt.Errorf("写入运行 %d 缺口: %w", gap.RunID, err)
		}
	}
	if err = s.rebuildAffected(ctx, tx, samples); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("提交历史写入事务: %w", err)
	}
	return nil
}

func (s *Store) rebuildAffected(ctx context.Context, tx *sql.Tx, samples []history.Sample) error {
	// 只有 10 秒基础桶从原始样本重建。更大的桶从下一层 rollup 合并，
	// 避免每次提交都重新解码一分钟或十分钟的全部快照。
	keys := make(map[bucketKey]struct{}, len(samples))
	for _, sample := range samples {
		start := alignTime(sample.Timestamp, rollupSteps[0])
		keys[bucketKey{runID: sample.RunID, step: rollupSteps[0], start: start}] = struct{}{}
	}
	configCache := map[int64]map[string]history.SeriesSpec{}
	for level, step := range rollupSteps {
		ordered := orderedKeys(keys)
		next := make(map[bucketKey]struct{}, len(ordered))
		for _, key := range ordered {
			var built builtAggregate
			var err error
			if level == 0 {
				specs, ok := configCache[key.runID]
				if !ok {
					var raw []byte
					if err = tx.QueryRowContext(ctx, `SELECT aggregation_json FROM runs WHERE id = ?`, key.runID).Scan(&raw); err != nil {
						return fmt.Errorf("读取运行 %d 聚合配置: %w", key.runID, err)
					}
					if err = json.Unmarshal(raw, &specs); err != nil {
						return fmt.Errorf("解码运行 %d 聚合配置: %w", key.runID, err)
					}
					configCache[key.runID] = specs
				}
				var rows []sampleRow
				rows, err = readRowsForAggregate(ctx, tx, key.runID, key.start, key.start+step)
				if err == nil {
					built, err = buildAggregate(rows, specs, key.start, key.start+step)
				}
			} else {
				built, err = mergeStoredRollups(ctx, tx, key.runID, rollupSteps[level-1], key.start, key.start+step)
			}
			if err != nil {
				return err
			}
			if err = s.saveRollup(ctx, tx, key, built); err != nil {
				return err
			}
			if level+1 < len(rollupSteps) {
				parentStep := rollupSteps[level+1]
				next[bucketKey{runID: key.runID, step: parentStep, start: alignTime(key.start, parentStep)}] = struct{}{}
			}
		}
		keys = next
	}
	return nil
}

func orderedKeys(keys map[bucketKey]struct{}) []bucketKey {
	ordered := make([]bucketKey, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].runID != ordered[j].runID {
			return ordered[i].runID < ordered[j].runID
		}
		return ordered[i].start < ordered[j].start
	})
	return ordered
}

func mergeStoredRollups(ctx context.Context, queryer queryRower, runID, sourceStep, from, to int64) (builtAggregate, error) {
	result := builtAggregate{payload: aggregatePayload{Plots: map[string][]aggregateCell{}}}
	rows, err := queryer.QueryContext(ctx, `
SELECT first_seq, last_seq, sample_count, covered_ns, payload
FROM rollups
WHERE run_id = ? AND step_ms = ? AND bucket_start_ms >= ? AND bucket_start_ms < ?
ORDER BY bucket_start_ms`, runID, sourceStep, from, to)
	if err != nil {
		return result, fmt.Errorf("读取运行 %d 的 %d 毫秒聚合: %w", runID, sourceStep, err)
	}
	defer rows.Close()
	for rows.Next() {
		var first, last, count, covered int64
		var raw []byte
		if err = rows.Scan(&first, &last, &count, &covered, &raw); err != nil {
			return result, err
		}
		payload, decodeErr := decodeAggregatePayload(raw)
		if decodeErr != nil {
			return result, fmt.Errorf("解码运行 %d 聚合: %w", runID, decodeErr)
		}
		mergePayload(&result.payload, payload)
		if result.sampleCount == 0 || first < result.firstSeq {
			result.firstSeq = first
		}
		if last > result.lastSeq {
			result.lastSeq = last
		}
		result.sampleCount += count
		result.coveredNS += covered
	}
	if err = rows.Err(); err != nil {
		return result, err
	}
	maxCoverage := (to - from) * int64(time.Millisecond)
	if result.coveredNS > maxCoverage {
		result.coveredNS = maxCoverage
	}
	return result, nil
}

func (s *Store) saveRollup(ctx context.Context, tx *sql.Tx, key bucketKey, built builtAggregate) error {
	if built.sampleCount == 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM rollups WHERE run_id = ? AND step_ms = ? AND bucket_start_ms = ?`, key.runID, key.step, key.start); err != nil {
			return fmt.Errorf("删除空聚合桶: %w", err)
		}
		return nil
	}
	payload, err := encodeAggregatePayload(built.payload)
	if err != nil {
		return fmt.Errorf("编码运行 %d 聚合桶: %w", key.runID, err)
	}
	isFinal := 0
	if key.start+key.step <= s.now().UnixMilli() {
		isFinal = 1
	}
	if _, err = tx.ExecContext(ctx, `
INSERT INTO rollups(run_id, step_ms, bucket_start_ms, first_seq, last_seq, sample_count, covered_ns, is_final, codec, payload)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'json', ?)
ON CONFLICT(run_id, step_ms, bucket_start_ms) DO UPDATE SET
    first_seq = excluded.first_seq,
    last_seq = excluded.last_seq,
    sample_count = excluded.sample_count,
    covered_ns = excluded.covered_ns,
    is_final = excluded.is_final,
    codec = excluded.codec,
    payload = excluded.payload`, key.runID, key.step, key.start, built.firstSeq, built.lastSeq, built.sampleCount, built.coveredNS, isFinal, payload); err != nil {
		return fmt.Errorf("保存运行 %d 聚合桶: %w", key.runID, err)
	}
	return nil
}

type queryRower interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

const aggregateRangeSQL = `
SELECT run_id, seq, ts_ms, duration_ns, payload
FROM samples INDEXED BY samples_run_time
WHERE run_id = ? AND ts_ms >= ? AND ts_ms < ?
ORDER BY ts_ms, seq`

const aggregatePreviousTimeSQL = `
SELECT run_id, seq, ts_ms, duration_ns, payload
FROM samples INDEXED BY samples_run_time
WHERE run_id = ? AND ts_ms < ?
ORDER BY ts_ms DESC, seq DESC
LIMIT 1`

func readRowsForAggregate(ctx context.Context, queryer queryRower, runID, from, to int64) ([]sampleRow, error) {
	rows, err := queryer.QueryContext(ctx, aggregateRangeSQL, runID, from, to)
	if err != nil {
		return nil, fmt.Errorf("读取运行 %d 聚合原始样本: %w", runID, err)
	}
	result := make([]sampleRow, 0)
	for rows.Next() {
		var row sampleRow
		if err = rows.Scan(&row.runID, &row.seq, &row.timestamp, &row.durationNS, &row.series); err != nil {
			_ = rows.Close()
			return nil, err
		}
		row.series = cloneBytes(row.series)
		result = append(result, row)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return result, nil
	}

	// 每个不连续 seq 段都需要自己的相邻基线。常规路径先用时间索引
	// 取 from 前一行；若墙钟回拨使它不是 seq 前驱，再按联合主键点查，
	// 既保留正确的直方图差分，也不会退化为扫描整个 run。
	present := make(map[int64]struct{}, len(result))
	needed := make(map[int64]struct{})
	for _, row := range result {
		present[row.seq] = struct{}{}
	}
	for _, row := range result {
		if row.seq <= 1 {
			continue
		}
		if _, ok := present[row.seq-1]; !ok {
			needed[row.seq-1] = struct{}{}
		}
	}
	if len(needed) > 0 {
		if baseline, found, readErr := readOneAggregateRow(ctx, queryer, aggregatePreviousTimeSQL, runID, from); readErr != nil {
			return nil, readErr
		} else if found {
			if _, ok := needed[baseline.seq]; ok {
				result = append(result, baseline)
				delete(needed, baseline.seq)
			}
		}
	}
	for seq := range needed {
		baseline, found, readErr := readOneAggregateRow(ctx, queryer, `
SELECT run_id, seq, ts_ms, duration_ns, payload
FROM samples
WHERE run_id = ? AND seq = ?`, runID, seq)
		if readErr != nil {
			return nil, readErr
		}
		if found {
			result = append(result, baseline)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].seq < result[j].seq })
	return result, nil
}

func readOneAggregateRow(ctx context.Context, queryer queryRower, query string, args ...any) (sampleRow, bool, error) {
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return sampleRow{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return sampleRow{}, false, rows.Err()
	}
	var row sampleRow
	if err = rows.Scan(&row.runID, &row.seq, &row.timestamp, &row.durationNS, &row.series); err != nil {
		return sampleRow{}, false, err
	}
	row.series = cloneBytes(row.series)
	return row, true, nil
}

func alignTime(value, step int64) int64 {
	if step <= 0 {
		return value
	}
	if value >= 0 {
		return value / step * step
	}
	quotient := value / step
	if value%step != 0 {
		const minInt64 = -int64(^uint64(0)>>1) - 1
		if quotient <= minInt64/step {
			return minInt64
		}
		quotient--
	}
	return quotient * step
}

func (s *Store) cleanup(ctx context.Context) error {
	cutoff := s.now().Add(-s.retention).UnixMilli()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// 每次最多清理一批，让周期性清理之间的实时写入正常推进。查询层
	// 始终按 cutoff 过滤，因此物理删除落后若干轮不会暴露过期数据。
	if _, err := s.writeDB.ExecContext(ctx, `
DELETE FROM samples
WHERE (run_id, seq) IN (
    SELECT run_id, seq FROM samples
    WHERE ts_ms < ?
    ORDER BY ts_ms, run_id, seq
    LIMIT 5000
	)`, cutoff); err != nil {
		return fmt.Errorf("清理过期原始样本: %w", err)
	}
	if _, err := s.writeDB.ExecContext(ctx, `DELETE FROM rollups WHERE bucket_start_ms + step_ms <= ?`, cutoff); err != nil {
		return fmt.Errorf("清理过期聚合桶: %w", err)
	}
	if _, err := s.writeDB.ExecContext(ctx, `DELETE FROM gaps WHERE ended_at_ms <= ?`, cutoff); err != nil {
		return fmt.Errorf("清理过期缺口: %w", err)
	}
	if _, err := s.writeDB.ExecContext(ctx, `
DELETE FROM runs
WHERE NOT EXISTS (SELECT 1 FROM samples WHERE samples.run_id = runs.id)
  AND NOT EXISTS (SELECT 1 FROM rollups WHERE rollups.run_id = runs.id)
  AND NOT EXISTS (SELECT 1 FROM gaps WHERE gaps.run_id = runs.id)
  AND ended_at_ms IS NOT NULL AND ended_at_ms <= ?`, cutoff); err != nil {
		return fmt.Errorf("清理空运行: %w", err)
	}
	return nil
}
