package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PinableAgents/statsviz/history"
)

func openTestStore(t *testing.T, options Options) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history.db")
	store, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	return store, path
}

func startTestRun(t *testing.T, store *Store, started, intervalNS int64, aggregation map[string]history.SeriesSpec, config string) history.Run {
	t.Helper()
	run, err := store.StartRun(context.Background(), history.RunConfig{
		StartedAtMS: started,
		IntervalNS:  intervalNS,
		Config:      json.RawMessage(config),
		Aggregation: aggregation,
	})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func enqueue(t *testing.T, store *Store, runID, seq, timestamp, durationNS int64, series string) error {
	t.Helper()
	return store.Enqueue(history.Sample{RunID: runID, Seq: seq, Timestamp: timestamp, DurationNS: durationNS, Series: json.RawMessage(series)})
}

func flush(t *testing.T, store *Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

// abandonTestStore 模拟已持久化数据后进程异常退出：停止 writer 并释放
// 测试资源，但不调用 Close 中的 run 封存步骤。
func abandonTestStore(t *testing.T, store *Store) {
	t.Helper()
	store.closed.Store(true)
	req := flushRequest{close: true, done: make(chan error, 1)}
	store.request <- req
	if err := <-req.done; err != nil {
		t.Fatal(err)
	}
	<-store.done
	if err := errors.Join(store.readDB.Close(), store.writeDB.Close(), store.fileLock.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestPersistentReopenPointPreservesIntegerAndOldConfigs(t *testing.T) {
	store, path := openTestStore(t, Options{FlushInterval: time.Hour})
	now := time.Now().UnixMilli()
	run1 := startTestRun(t, store, now-2000, int64(time.Second), map[string]history.SeriesSpec{"counter": {Kinds: []history.Kind{history.Gauge}}}, `{"name":"old"}`)
	wantSeries := `{"counter":[9007199254740993]}`
	if err := enqueue(t, store, run1.ID, 1, now-1000, int64(time.Second), wantSeries); err != nil {
		t.Fatal(err)
	}
	flush(t, store)
	run2 := startTestRun(t, store, now, int64(time.Second), map[string]history.SeriesSpec{"new": {Kinds: []history.Kind{history.Rate}}}, `{"name":"new"}`)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path, Options{FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	point, err := store.Point(context.Background(), history.PointRequest{At: now - 1000, RunID: run1.ID})
	if err != nil {
		t.Fatal(err)
	}
	if !point.Found || point.Sample == nil || !bytes.Equal(point.Sample.Series, []byte(wantSeries)) {
		t.Fatalf("重开后的原始点 = %+v, series=%s", point, point.Sample.Series)
	}
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Runs) != 1 || meta.Runs[0].ID != run1.ID {
		t.Fatalf("runs = %+v, want persisted old config", meta.Runs)
	}
	if meta.Runs[0].EndedAtMS == nil || *meta.Runs[0].EndedAtMS != now-1000 {
		t.Fatalf("old run ended_at = %v, want last sample %d", meta.Runs[0].EndedAtMS, now-1000)
	}
	if len(meta.Watermarks) != 1 || meta.Watermarks[0] != (history.Watermark{RunID: run1.ID, Seq: 1}) {
		t.Fatalf("watermarks = %+v", meta.Watermarks)
	}
	emptyRun, err := readRun(context.Background(), store.readDB, run2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if emptyRun.EndedAtMS == nil || *emptyRun.EndedAtMS != run2.StartedAtMS {
		t.Fatalf("empty run ended_at = %v, want started_at %d", emptyRun.EndedAtMS, run2.StartedAtMS)
	}
}

func TestStartRunSealsCrashRunAtHighestSequenceTimestamp(t *testing.T) {
	store, path := openTestStore(t, Options{FlushInterval: time.Hour})
	started := time.Now().UnixMilli()
	oldRun := startTestRun(t, store, started, int64(time.Second), nil, `{}`)
	if err := enqueue(t, store, oldRun.ID, 1, started+1000, int64(time.Second), `{"x":[1]}`); err != nil {
		t.Fatal(err)
	}
	// 最高 seq 的墙钟时间早于启动时间，封存不得改用时间最大值。
	lastTimestamp := started - 1000
	if err := enqueue(t, store, oldRun.ID, 2, lastTimestamp, int64(time.Second), `{"x":[2]}`); err != nil {
		t.Fatal(err)
	}
	flush(t, store)
	abandonTestStore(t, store)

	store, err := Open(path, Options{FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	newStarted := started + int64(time.Hour/time.Millisecond)
	_ = startTestRun(t, store, newStarted, int64(time.Second), nil, `{}`)
	sealed, err := readRun(context.Background(), store.readDB, oldRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	if sealed.EndedAtMS == nil || *sealed.EndedAtMS != lastTimestamp {
		t.Fatalf("crash run ended_at = %v, want highest-seq timestamp %d (not next start %d)", sealed.EndedAtMS, lastTimestamp, newStarted)
	}
}

func TestAggregateSemanticsBoundaryAndHistogramGap(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour, QueueCapacity: 20})
	defer store.Close()
	base := alignTime(time.Now().UnixMilli(), 10_000)
	run := startTestRun(t, store, base, int64(time.Second), map[string]history.SeriesSpec{
		"g":     {Kinds: []history.Kind{history.Gauge}},
		"d":     {Kinds: []history.Kind{history.Delta}},
		"h":     {Kinds: []history.Kind{history.Histogram}, Heatmap: true},
		"size":  {Kinds: []history.Kind{history.Gauge}, Heatmap: true},
		"event": {Kinds: []history.Kind{history.Event}},
	}, `{}`)
	samples := []struct {
		seq, offset, duration int64
		series                string
	}{
		{1, 1000, int64(time.Second), `{"g":[10],"d":[2],"h":[100,200],"size":[1,10],"event":[1111]}`},
		{2, 2000, int64(2 * time.Second), `{"g":[20],"d":[3],"h":[103,205],"size":[3,30],"event":[2222]}`},
		// seq=3 缺失，累计直方图不得把 2→4 的差值算入桶。
		{4, 3000, int64(time.Second), `{"g":[40],"d":[4],"h":[110,215],"size":[5,50],"event":[4444]}`},
	}
	for _, sample := range samples {
		if err := enqueue(t, store, run.ID, sample.seq, base+sample.offset, sample.duration, sample.series); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, store)

	response, err := store.Range(context.Background(), history.RangeRequest{From: base, To: base + 10_000, MaxPoints: 1})
	if err != nil {
		t.Fatal(err)
	}
	if response.StepMS != 10_000 || len(response.Points) != 1 {
		t.Fatalf("step=%d points=%d", response.StepMS, len(response.Points))
	}
	values := decodeFloatSeries(t, response.Points[0].Series)
	assertNear(t, values["g"][0], 22.5)
	assertNear(t, values["d"][0], 9)
	assertNear(t, values["h"][0], 3)
	assertNear(t, values["h"][1], 5)
	assertNear(t, values["size"][0], 3)
	assertNear(t, values["size"][1], 30)
	assertNear(t, values["event"][0], 4444)

	partial, err := store.Range(context.Background(), history.RangeRequest{From: base + 1500, To: base + 9500, MaxPoints: 1})
	if err != nil {
		t.Fatal(err)
	}
	if partial.StepMS != 10_000 || len(partial.Points) != 1 {
		t.Fatalf("partial step=%d points=%d", partial.StepMS, len(partial.Points))
	}
	values = decodeFloatSeries(t, partial.Points[0].Series)
	assertNear(t, values["g"][0], 80.0/3.0)
	assertNear(t, values["d"][0], 7)
	assertNear(t, values["h"][0], 3)
	assertNear(t, values["event"][0], 4444)
}

func TestLongRangeUsesPersistentRollups(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour, QueueCapacity: 300})
	defer store.Close()
	base := alignTime(time.Now().Add(-30*time.Minute).UnixMilli(), 600_000)
	run := startTestRun(t, store, base, int64(10*time.Second), map[string]history.SeriesSpec{"g": {Kinds: []history.Kind{history.Gauge}}}, `{}`)
	for index := int64(0); index < 120; index++ {
		if err := enqueue(t, store, run.ID, index+1, base+index*10_000, int64(10*time.Second), fmt.Sprintf(`{"g":[%d]}`, index)); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, store)
	// 留下一行 raw 供运行筛选使用；中间数据只能来自已持久化 rollup。
	if _, err := store.writeDB.Exec(`DELETE FROM samples WHERE seq > 1`); err != nil {
		t.Fatal(err)
	}
	response, err := store.Range(context.Background(), history.RangeRequest{From: base, To: base + 1_200_000, RunID: run.ID, MaxPoints: 2})
	if err != nil {
		t.Fatal(err)
	}
	if response.StepMS != 600_000 || len(response.Points) != 2 {
		t.Fatalf("step=%d points=%d, want two persisted 10m buckets", response.StepMS, len(response.Points))
	}
}

func TestQueueOverflowGapAndCloseFlush(t *testing.T) {
	store, path := openTestStore(t, Options{FlushInterval: time.Hour, QueueCapacity: 2})
	now := time.Now().UnixMilli()
	run := startTestRun(t, store, now, int64(time.Second), nil, `{}`)
	for seq := int64(1); seq <= 3; seq++ {
		err := enqueue(t, store, run.ID, seq, now+seq, int64(time.Second), fmt.Sprintf(`{"x":[%d]}`, seq))
		if seq == 3 && !errors.Is(err, history.ErrQueueFull) {
			t.Fatalf("third enqueue error = %v", err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path, Options{FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	response, err := store.Range(context.Background(), history.RangeRequest{From: now, To: now + 10, RunID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Points) != 2 || response.Points[0].Seq != 2 || len(response.Gaps) != 1 {
		t.Fatalf("points=%+v gaps=%+v", response.Points, response.Gaps)
	}
	gap := response.Gaps[0]
	if gap.FirstSeq != 1 || gap.LastSeq != 1 || gap.Reason != "queue_overflow" {
		t.Fatalf("gap = %+v", gap)
	}
}

func TestFailedBatchRetriesIdempotently(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour})
	defer store.Close()
	now := time.Now().UnixMilli()
	if err := enqueue(t, store, 999, 1, now, int64(time.Second), `{"x":[1]}`); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(context.Background()); err == nil {
		t.Fatal("missing foreign-key error")
	}
	if _, err := store.writeDB.Exec(`
INSERT INTO runs(id, started_at_ms, sample_interval_ns, config_hash, config_json, aggregation_json)
VALUES(999, ?, ?, 'manual', '{}', '{}')`, now, int64(time.Second)); err != nil {
		t.Fatal(err)
	}
	flush(t, store)
	// 再提交同一身份，UPSERT 和完整桶替换不能重复累计。
	if err := store.commitBatch(context.Background(), []history.Sample{{RunID: 999, Seq: 1, Timestamp: now, DurationNS: int64(time.Second), Series: json.RawMessage(`{"x":[1]}`)}}, nil); err != nil {
		t.Fatal(err)
	}
	var samples, rollups int
	if err := store.writeDB.QueryRow(`SELECT count(*) FROM samples WHERE run_id=999`).Scan(&samples); err != nil {
		t.Fatal(err)
	}
	if err := store.writeDB.QueryRow(`SELECT count(*) FROM rollups WHERE run_id=999`).Scan(&rollups); err != nil {
		t.Fatal(err)
	}
	if samples != 1 || rollups != 3 {
		t.Fatalf("samples=%d rollups=%d", samples, rollups)
	}
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status.LastError != "" {
		t.Fatalf("last error was not cleared: %q", meta.Status.LastError)
	}
}

func TestRetentionBoundaryAndPhysicalCleanup(t *testing.T) {
	store, _ := openTestStore(t, Options{Retention: time.Hour, FlushInterval: time.Hour})
	defer store.Close()
	now := time.Now().Truncate(time.Second)
	store.now = func() time.Time { return now }
	cutoff := now.Add(-time.Hour).UnixMilli()
	run := startTestRun(t, store, cutoff-1000, int64(time.Second), nil, `{}`)
	for seq, timestamp := range []int64{cutoff - 1, cutoff, cutoff + 1} {
		if err := enqueue(t, store, run.ID, int64(seq+1), timestamp, int64(time.Second), `{"x":[1]}`); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, store)
	response, err := store.Range(context.Background(), history.RangeRequest{From: cutoff - 10_000, To: cutoff + 10})
	if err != nil {
		t.Fatal(err)
	}
	if response.From != cutoff || len(response.Points) != 2 {
		t.Fatalf("from=%d points=%+v", response.From, response.Points)
	}
	if err := store.cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	var old, boundary int
	if err := store.writeDB.QueryRow(`SELECT count(*) FROM samples WHERE ts_ms < ?`, cutoff).Scan(&old); err != nil {
		t.Fatal(err)
	}
	if err := store.writeDB.QueryRow(`SELECT count(*) FROM samples WHERE ts_ms = ?`, cutoff).Scan(&boundary); err != nil {
		t.Fatal(err)
	}
	if old != 0 || boundary != 1 {
		t.Fatalf("old=%d boundary=%d", old, boundary)
	}
}

func TestFileLockAndTailRecovery(t *testing.T) {
	store, path := openTestStore(t, Options{FlushInterval: time.Hour})
	if _, err := Open(path, Options{}); err == nil {
		t.Fatal("second Open unexpectedly acquired the same path lock")
	}
	now := time.Now().UnixMilli()
	run := startTestRun(t, store, now, int64(time.Second), nil, `{}`)
	if err := enqueue(t, store, run.ID, 1, now, int64(time.Second), `{"x":[1]}`); err != nil {
		t.Fatal(err)
	}
	flush(t, store)
	if _, err := store.writeDB.Exec(`DELETE FROM rollups WHERE run_id=? AND step_ms=10000`, run.ID); err != nil {
		t.Fatal(err)
	}
	abandonTestStore(t, store)
	store, err := Open(path, Options{FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var count int
	if err := store.writeDB.QueryRow(`SELECT count(*) FROM rollups WHERE run_id=? AND step_ms=10000`, run.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("recovered 10s rollups = %d", count)
	}
}

func TestLimitsHeatmapAndCancellation(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour, QueueCapacity: 500})
	defer store.Close()
	now := time.Now().UnixMilli()
	run := startTestRun(t, store, now, int64(time.Millisecond), map[string]history.SeriesSpec{"heat": {Kinds: []history.Kind{history.Gauge}, Heatmap: true}}, `{}`)
	for seq := int64(1); seq <= 401; seq++ {
		if err := enqueue(t, store, run.ID, seq, now+seq, int64(time.Millisecond), `{"heat":[1]}`); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, store)
	response, err := store.Range(context.Background(), history.RangeRequest{From: now, To: now + 402, RunID: run.ID, MaxPoints: 1500})
	if err != nil {
		t.Fatal(err)
	}
	if response.StepMS == 0 || len(response.Points) > maxHeatmapPoints {
		t.Fatalf("heatmap step=%d points=%d", response.StepMS, len(response.Points))
	}
	if _, err = store.Range(context.Background(), history.RangeRequest{From: now, To: now + 1, MaxPoints: maxCurvePoints + 1}); !errors.Is(err, history.ErrInvalidQuery) {
		t.Fatalf("oversized max_points error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = store.Range(canceled, history.RangeRequest{From: now, To: now + 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled query error = %v", err)
	}
}

func TestStartRunRejectsOversizedMetadata(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour, QueueCapacity: 5})
	defer store.Close()
	base := time.Now().UnixMilli()
	largeConfig := `{"padding":"` + strings.Repeat("a", maxRunMetadataBytes) + `"}`
	_, err := store.StartRun(context.Background(), history.RunConfig{
		StartedAtMS: base,
		IntervalNS:  int64(time.Second),
		Config:      json.RawMessage(largeConfig),
	})
	if !errors.Is(err, history.ErrTooLarge) {
		t.Fatalf("oversized run metadata error = %v", err)
	}
	var count int
	if err = store.readDB.QueryRow(`SELECT count(*) FROM runs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("runs = %d after rejected config", count)
	}
}

func TestMetaRejectsRunRowAndByteBudgets(t *testing.T) {
	t.Run("rows", func(t *testing.T) {
		store, _ := openTestStore(t, Options{FlushInterval: time.Hour})
		defer store.Close()
		tx, err := store.writeDB.Begin()
		if err != nil {
			t.Fatal(err)
		}
		stmt, err := tx.Prepare(`
INSERT INTO runs(started_at_ms, sample_interval_ns, config_hash, config_json, aggregation_json)
VALUES(?, ?, 'manual', '{}', '{}')`)
		if err != nil {
			t.Fatal(err)
		}
		for index := 0; index <= maxMetadataRuns; index++ {
			if _, err = stmt.Exec(time.Now().UnixMilli()+int64(index), int64(time.Second)); err != nil {
				t.Fatal(err)
			}
		}
		if err = stmt.Close(); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		queryer := &metadataRowTrackingQuerier{readQuerier: store.readDB}
		if _, err = readRuns(context.Background(), queryer, 0, 0, 0, true); !errors.Is(err, history.ErrTooLarge) {
			t.Fatalf("run row budget preflight error = %v", err)
		}
		if queryer.rowQueries != 0 {
			t.Fatalf("oversized run metadata was materialized with %d row queries", queryer.rowQueries)
		}
		if _, err = store.Meta(context.Background()); !errors.Is(err, history.ErrTooLarge) {
			t.Fatalf("run row budget error = %v", err)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		store, _ := openTestStore(t, Options{FlushInterval: time.Hour})
		defer store.Close()
		padding := maxRunMetadataBytes - len(`{"padding":""}`) - len(`{}`)
		// 多字节内容验证预算按 UTF-8 字节数计算。
		content := strings.Repeat("测", padding/len("测")) + strings.Repeat("a", padding%len("测"))
		config := `{"padding":"` + content + `"}`
		for index := 0; index < maxMetadataBytes/maxRunMetadataBytes+1; index++ {
			if _, err := store.writeDB.Exec(`
INSERT INTO runs(started_at_ms, sample_interval_ns, config_hash, config_json, aggregation_json)
VALUES(?, ?, 'manual', ?, '{}')`, time.Now().UnixMilli()+int64(index), int64(time.Second), config); err != nil {
				t.Fatal(err)
			}
		}
		queryer := &metadataRowTrackingQuerier{readQuerier: store.readDB}
		if _, err := readRuns(context.Background(), queryer, 0, 0, 0, true); !errors.Is(err, history.ErrTooLarge) {
			t.Fatalf("run byte budget preflight error = %v", err)
		}
		if queryer.rowQueries != 0 {
			t.Fatalf("oversized run metadata was materialized with %d row queries", queryer.rowQueries)
		}
		if _, err := store.Meta(context.Background()); !errors.Is(err, history.ErrTooLarge) {
			t.Fatalf("run byte budget error = %v", err)
		}
	})
}

type metadataRowTrackingQuerier struct {
	readQuerier
	rowQueries int
}

func (q *metadataRowTrackingQuerier) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.rowQueries++
	return q.readQuerier.QueryContext(ctx, query, args...)
}

func TestRangeRejectsGapRowBudget(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour})
	defer store.Close()
	base := time.Now().UnixMilli()
	run := startTestRun(t, store, base, int64(time.Second), nil, `{}`)
	tx, err := store.writeDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`
INSERT INTO gaps(run_id, first_seq, last_seq, started_at_ms, ended_at_ms, reason)
VALUES(?, ?, ?, ?, ?, 'test')`)
	if err != nil {
		t.Fatal(err)
	}
	for seq := int64(1); seq <= maxMetadataGaps+1; seq++ {
		if _, err = stmt.Exec(run.ID, seq, seq, base, base+10); err != nil {
			t.Fatal(err)
		}
	}
	if err = stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, runID := range []int64{0, run.ID} {
		t.Run(fmt.Sprintf("preflight/run-%d", runID), func(t *testing.T) {
			queryer := &metadataRowTrackingQuerier{readQuerier: store.readDB}
			if _, err := readGaps(context.Background(), queryer, base, base+10, runID); !errors.Is(err, history.ErrTooLarge) {
				t.Fatalf("gap row budget error = %v", err)
			}
			if queryer.rowQueries != 0 {
				t.Fatalf("oversized gaps were materialized with %d row queries", queryer.rowQueries)
			}
		})
	}
	for _, query := range []struct {
		name     string
		from, to int64
		runID    int64
	}{
		{"other-run", base, base + 10, run.ID + 1},
		{"outside-range", base + 10, base + 20, 0},
	} {
		t.Run(query.name, func(t *testing.T) {
			gaps, err := readGaps(context.Background(), store.readDB, query.from, query.to, query.runID)
			if err != nil || len(gaps) != 0 {
				t.Fatalf("unrelated gaps affected the budget: len=%d err=%v", len(gaps), err)
			}
		})
	}
	if _, err = store.Range(context.Background(), history.RangeRequest{From: base, To: base + 10, RunID: run.ID}); !errors.Is(err, history.ErrTooLarge) {
		t.Fatalf("gap row budget error = %v", err)
	}
}

func TestRangeRejectsGapByteBudget(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour})
	defer store.Close()
	base := time.Now().UnixMilli()
	run := startTestRun(t, store, base, int64(time.Second), nil, `{}`)
	size := maxMetadataBytes - metadataRowOverhead + 1
	reason := strings.Repeat("缺", size/len("缺")) + strings.Repeat("a", size%len("缺"))
	if _, err := store.writeDB.Exec(`
INSERT INTO gaps(run_id, first_seq, last_seq, started_at_ms, ended_at_ms, reason)
VALUES(?, 1, 1, ?, ?, ?)`, run.ID, base, base+10, reason); err != nil {
		t.Fatal(err)
	}
	queryer := &metadataRowTrackingQuerier{readQuerier: store.readDB}
	if _, err := readGaps(context.Background(), queryer, base, base+10, run.ID); !errors.Is(err, history.ErrTooLarge) {
		t.Fatalf("gap byte budget preflight error = %v", err)
	}
	if queryer.rowQueries != 0 {
		t.Fatalf("oversized gap metadata was materialized with %d row queries", queryer.rowQueries)
	}
	if _, err := store.Range(context.Background(), history.RangeRequest{From: base, To: base + 10, RunID: run.ID}); !errors.Is(err, history.ErrTooLarge) {
		t.Fatalf("gap byte budget error = %v", err)
	}
}

func TestBudgetFactorCoversCellAndByteLimits(t *testing.T) {
	if got := budgetFactor(1, maxNumericCells+1, 1, maxCurvePoints); got <= 1 {
		t.Fatalf("numeric cell factor = %d", got)
	}
	if got := budgetFactor(1, 1, maxResponseBytes+1, maxCurvePoints); got <= 1 {
		t.Fatalf("response byte factor = %d", got)
	}
}

func TestPointNearestToleranceAndGap(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour})
	defer store.Close()
	now := time.Now().UnixMilli()
	run := startTestRun(t, store, now, int64(time.Second), nil, `{}`)
	if err := enqueue(t, store, run.ID, 1, now, int64(time.Second), `{"x":[1]}`); err != nil {
		t.Fatal(err)
	}
	if err := enqueue(t, store, run.ID, 2, now+2000, int64(time.Second), `{"x":[2]}`); err != nil {
		t.Fatal(err)
	}
	flush(t, store)
	point, err := store.Point(context.Background(), history.PointRequest{At: now + 1200, RunID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	if !point.Found || point.Sample.Seq != 2 || point.OffsetMS != 800 {
		t.Fatalf("nearest point = %+v", point)
	}
	if _, err := store.writeDB.Exec(`INSERT INTO gaps(run_id, first_seq, last_seq, started_at_ms, ended_at_ms, reason) VALUES(?, 3, 3, ?, ?, 'test')`, run.ID, now+1000, now+1500); err != nil {
		t.Fatal(err)
	}
	point, err = store.Point(context.Background(), history.PointRequest{At: now + 1200, RunID: run.ID, ToleranceMS: 5000})
	if err != nil {
		t.Fatal(err)
	}
	if point.Found {
		t.Fatalf("point crossed an explicit gap: %+v", point)
	}
}

func TestAggregateQueriesUseBoundedIndexes(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour})
	defer store.Close()
	assertPlanContains(t, store, aggregateRangeSQL, []any{1, 1000, 2000}, "samples_run_time", "ts_ms>?", "ts_ms<?")
	assertPlanContains(t, store, aggregatePreviousTimeSQL, []any{1, 1000}, "samples_run_time", "ts_ms<?")
	assertPlanContains(t, store, oldestRetainedSampleSQL, []any{1000}, "samples_time", "ts_ms>?")
	assertPlanContains(t, store, newestRetainedSampleSQL, []any{1000}, "samples_time", "ts_ms>?")
	plan := explainPlan(t, store, watermarkForRunSQL, 1, 1000)
	if !strings.Contains(plan, "PRIMARY KEY (run_id=?)") || strings.Contains(plan, "TEMP B-TREE") || strings.Contains(plan, "SCAN samples") {
		t.Fatalf("watermark plan is not a primary-key tail lookup:\n%s", plan)
	}
}

func TestAggregateBaselineHandlesClockRollback(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour})
	defer store.Close()
	base := alignTime(time.Now().Add(-time.Minute).UnixMilli(), 10_000)
	run := startTestRun(t, store, base+20_000, int64(time.Second), map[string]history.SeriesSpec{"h": {Kinds: []history.Kind{history.Histogram}}}, `{}`)
	if err := enqueue(t, store, run.ID, 1, base+20_000, int64(time.Second), `{"h":[100]}`); err != nil {
		t.Fatal(err)
	}
	if err := enqueue(t, store, run.ID, 2, base+1000, int64(time.Second), `{"h":[105]}`); err != nil {
		t.Fatal(err)
	}
	flush(t, store)
	tx, err := store.readDB.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	rows, err := readRowsForAggregate(context.Background(), tx, run.ID, base, base+10_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].seq != 1 || rows[1].seq != 2 {
		t.Fatalf("rows = %+v, want seq baseline 1 and in-range seq 2", rows)
	}
	built, err := buildAggregate(rows, run.Aggregation, base, base+10_000)
	if err != nil {
		t.Fatal(err)
	}
	series, _, _, _, err := materialize(built.payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	values := decodeFloatSeries(t, series)
	assertNear(t, values["h"][0], 5)

	response, err := store.Range(context.Background(), history.RangeRequest{From: base, To: base + 10_000, RunID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Runs) != 1 || len(response.Points) != 1 {
		t.Fatalf("clock-rollback range omitted actual data: runs=%d points=%d", len(response.Runs), len(response.Points))
	}
}

func TestRollupBoundaryDescendsBeforeReadingRaw(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour, QueueCapacity: 300})
	defer store.Close()
	base := alignTime(time.Now().Add(-time.Hour).UnixMilli(), 600_000)
	run := startTestRun(t, store, base, int64(10*time.Second), map[string]history.SeriesSpec{"g": {Kinds: []history.Kind{history.Gauge}}}, `{}`)
	for seq := int64(1); seq <= 120; seq++ {
		if err := enqueue(t, store, run.ID, seq, base+seq*10_000, int64(10*time.Second), fmt.Sprintf(`{"g":[%d]}`, seq)); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, store)
	// 删除两个不足 10 秒 raw 边界之外的原始数据。查询仍必须由
	// 10m→1m→10s rollup 拼出结果，不能回退读取近 20 分钟 raw。
	if _, err := store.writeDB.Exec(`DELETE FROM samples WHERE run_id=? AND seq NOT IN (1,119)`, run.ID); err != nil {
		t.Fatal(err)
	}
	response, err := store.Range(context.Background(), history.RangeRequest{
		From: base + 5000, To: base + 1_195_000, RunID: run.ID, MaxPoints: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.StepMS != 600_000 || len(response.Points) != 2 {
		t.Fatalf("step=%d points=%d", response.StepMS, len(response.Points))
	}
	first := decodeFloatSeries(t, response.Points[0].Series)
	second := decodeFloatSeries(t, response.Points[1].Series)
	assertNear(t, first["g"][0], 30)
	assertNear(t, second["g"][0], 89.5)
}

func TestDimensionEstimateSelectsBudgetBeforeDecode(t *testing.T) {
	config := json.RawMessage(`{
"series":[
 {"name":"line","subplots":[{},{}]},
 {"name":"heat","buckets":[{},{},{},{},{}]}
],"events":["lastgc"]}`)
	runs := []history.Run{{Config: config, Aggregation: map[string]history.SeriesSpec{
		"line": {Kinds: []history.Kind{history.Gauge, history.Gauge}},
		"heat": {Kinds: []history.Kind{history.Histogram}, Heatmap: true},
	}}}
	if got := estimateSeriesDimensions(runs, nil); got != 8 {
		t.Fatalf("dimensions = %d, want 8", got)
	}
	fixtureDimensions := 523
	limit := pointBudgetForDimensions(maxHeatmapPoints, fixtureDimensions, 3)
	if limit != 191 {
		t.Fatalf("fixture rollup point budget = %d, want 191", limit)
	}
	if step := chooseRollupStep(0, int64(7*24*time.Hour/time.Millisecond), limit); step != 3_600_000 {
		t.Fatalf("fixture initial step = %d, want 3600000", step)
	}
}

func TestWatermarkTailLookupHonorsRetentionWithClockRollback(t *testing.T) {
	store, _ := openTestStore(t, Options{Retention: time.Hour, FlushInterval: time.Hour})
	defer store.Close()
	now := time.Now().Truncate(time.Second)
	store.now = func() time.Time { return now }
	cutoff := now.Add(-time.Hour).UnixMilli()
	run := startTestRun(t, store, cutoff, int64(time.Second), nil, `{}`)
	if err := enqueue(t, store, run.ID, 1, now.UnixMilli(), int64(time.Second), `{"x":[1]}`); err != nil {
		t.Fatal(err)
	}
	if err := enqueue(t, store, run.ID, 2, cutoff-1, int64(time.Second), `{"x":[2]}`); err != nil {
		t.Fatal(err)
	}
	flush(t, store)
	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Watermarks) != 1 || meta.Watermarks[0].Seq != 1 {
		t.Fatalf("watermarks = %+v, want retained seq 1", meta.Watermarks)
	}
}

func TestFlushConcurrentWithClose(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour, QueueCapacity: 100})
	now := time.Now().UnixMilli()
	run := startTestRun(t, store, now, int64(time.Second), nil, `{}`)
	for seq := int64(1); seq <= 20; seq++ {
		if err := enqueue(t, store, run.ID, seq, now+seq, int64(time.Second), `{"x":[1]}`); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	errorsCh := make(chan error, 33)
	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			errorsCh <- store.Flush(ctx)
		}()
	}
	close(start)
	closeErr := store.Close()
	wait.Wait()
	close(errorsCh)
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	for err := range errorsCh {
		if err != nil && !errors.Is(err, history.ErrClosed) {
			t.Fatalf("concurrent Flush error = %v", err)
		}
	}
}

func TestExtremeTimeInputsAreRejectedOrSaturated(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	defer store.Close()
	if _, err := store.Range(context.Background(), history.RangeRequest{From: 0, To: math.MaxInt64}); !errors.Is(err, history.ErrInvalidQuery) {
		t.Fatalf("extreme range error = %v", err)
	}
	if _, err := store.Point(context.Background(), history.PointRequest{At: math.MaxInt64}); !errors.Is(err, history.ErrInvalidQuery) {
		t.Fatalf("extreme point error = %v", err)
	}
	if got := ceilDiv(math.MaxInt64, 2); got != math.MaxInt64/2+1 {
		t.Fatalf("ceilDiv overflowed: %d", got)
	}
	if got := saturatingMultiply(math.MaxInt64, 2); got != math.MaxInt64 {
		t.Fatalf("saturatingMultiply = %d", got)
	}
	if got := alignTime(math.MinInt64, 600_000); got != math.MinInt64 {
		t.Fatalf("alignTime(min) = %d", got)
	}
}

func assertPlanContains(t *testing.T, store *Store, query string, args []any, fragments ...string) {
	t.Helper()
	plan := explainPlan(t, store, query, args...)
	for _, fragment := range fragments {
		if !strings.Contains(plan, fragment) {
			t.Fatalf("query plan does not contain %q:\n%s", fragment, plan)
		}
	}
}

func explainPlan(t *testing.T, store *Store, query string, args ...any) string {
	t.Helper()
	rows, err := store.readDB.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	details := []string{}
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(details)
	return strings.Join(details, "\n")
}

func decodeFloatSeries(t *testing.T, raw json.RawMessage) map[string][]float64 {
	t.Helper()
	var result map[string][]float64
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return result
}

func assertNear(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("got %v, want %v", got, want)
	}
}
