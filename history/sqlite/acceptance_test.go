package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/PinableAgents/statsviz/history"
	"github.com/PinableAgents/statsviz/internal/plot"
)

// TestHistoryFullVolumeAcceptance 是显式启用的容量/性能验收工具。默认 go test
// 必须快速跳过，不得把 604800 条 fixture 带入常规 CI。
func TestHistoryFullVolumeAcceptance(t *testing.T) {
	if os.Getenv("STATSVIZ_HISTORY_ACCEPTANCE") != "1" {
		t.Skip("设置 STATSVIZ_HISTORY_ACCEPTANCE=1 后才运行 SQLite 全量验收")
	}
	settings := loadAcceptanceSettings(t)
	acceptanceEnsureReportOwned(t, settings.ReportPath)

	plots, err := plot.NewList(nil)
	if err != nil {
		t.Fatal(err)
	}
	config := plots.Config()
	config.History = plot.HistoryConfig{IntervalMS: acceptanceIntervalMS}
	configJSON, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	aggregation := plots.Aggregation()
	realSnapshot, err := plots.Collect()
	if err != nil {
		t.Fatal(err)
	}
	template := newAcceptanceTemplate(t, realSnapshot.Series, aggregation)
	configHash := acceptanceConfigHash(configJSON, aggregation)
	fixtureID := acceptanceFixtureID(settings, configHash, template.shapeHash)
	expectedMarker := acceptanceMarker{
		ToolID: acceptanceToolID, FormatVersion: acceptanceFormatVersion,
		FixtureID: fixtureID, State: "complete", SampleCount: settings.Samples,
		IntervalMS: acceptanceIntervalMS, FixedNowMS: acceptanceFixedNowMS,
		ConfigHash: configHash, ShapeHash: template.shapeHash, NumericCells: template.numericCells,
	}
	reuse := acceptancePrepareDB(t, settings, expectedMarker)
	report := &acceptanceReport{
		ToolID: acceptanceToolID, FormatVersion: acceptanceFormatVersion,
		FixtureID: fixtureID, Phase: "starting", GoVersion: runtime.Version(),
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, DBPath: settings.DBPath,
		ReportPath: settings.ReportPath, SampleCount: settings.Samples,
		IntervalMS: acceptanceIntervalMS, SpanMS: settings.EndMS - settings.StartMS,
		FixedNowMS: acceptanceFixedNowMS, ConfigHash: configHash, ShapeHash: template.shapeHash,
		ConfigBytes: len(configJSON), SnapshotBytes: len(realSnapshot.Series),
		PlotCount: len(template.plots), NumericCells: template.numericCells,
		Kinds: template.kinds, Queries: map[string]acceptanceQueryReport{},
		Build: acceptanceBuildReport{Memory: map[string]acceptanceMemStats{}},
		Notes: []string{
			"查询首次读取与预热单独记录，p50/p95/max 不包含这两次。",
			"查询延迟与分配量包含 Store 调用和返回值 JSON 编码。",
			"没有清理操作系统文件缓存，os_cache_explicitly_cleared 始终为 false。",
			"保留边界与分批 cleanup 由常规 TestRetentionBoundaryAndPhysicalCleanup 覆盖，本工具不重复造大数据。",
		},
	}
	defer acceptanceWriteReport(t, report)

	fixedClock := func() time.Time { return time.UnixMilli(settings.EndMS) }
	store, err := openWithClock(settings.DBPath, Options{
		Retention: 7 * 24 * time.Hour, FlushInterval: time.Hour, QueueCapacity: batchSize,
	}, fixedClock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()

	runID := int64(0)
	if reuse {
		marker, markerErr := acceptanceReadMarker(settings.DBPath)
		if markerErr != nil {
			t.Fatal(markerErr)
		}
		runID = marker.RunID
		var seed []byte
		if err = store.readDB.QueryRow(`
SELECT payload FROM samples WHERE run_id = ? ORDER BY ts_ms, seq LIMIT 1`, runID).Scan(&seed); err != nil {
			t.Fatalf("读取复用 fixture 基准样本: %v", err)
		}
		template = rebaseAcceptanceTemplate(t, template, seed, 1, settings.StartMS)
		report.Build.Reused = true
	} else {
		report.Phase = "creating_marker"
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = acceptanceCreateMarker(ctx, store, expectedMarker)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		runID = buildAcceptanceFixture(t, store, settings, configJSON, aggregation, template, report)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		err = acceptanceSetMarkerRun(ctx, store, runID)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	if runID <= 0 {
		t.Fatalf("fixture run_id=%d", runID)
	}

	report.Phase = "functional_validation"
	validateAcceptanceFixture(t, store, settings, runID, template, report)
	report.Phase = "read_write_concurrency"
	report.Concurrency = validateAcceptanceConcurrency(t, store, settings, runID, template)
	assertAcceptanceSampleCount(t, store, runID, int64(settings.Samples))

	// 先正常关闭并重开，验证旧点后才将 marker 置为 complete。
	report.Phase = "close_reopen"
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	store, err = openWithClock(settings.DBPath, Options{
		Retention: 7 * 24 * time.Hour, FlushInterval: time.Hour, QueueCapacity: batchSize,
	}, fixedClock)
	if err != nil {
		t.Fatal(err)
	}
	validateAcceptanceReopenPoint(t, store, settings, runID, template)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = acceptanceCompleteMarker(ctx, store, fixtureID)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	report.Phase = "query_measurement"
	runAcceptanceQueries(t, store, settings, runID, report)
	refreshAcceptanceStorageReport(t, store, settings, runID, report)
	report.Phase = "complete"
	t.Logf("验收完成: samples=%d cells=%d db=%s report=%s", settings.Samples, template.numericCells, settings.DBPath, settings.ReportPath)
}

func buildAcceptanceFixture(t *testing.T, store *Store, settings acceptanceSettings, config json.RawMessage, aggregation map[string]history.SeriesSpec, template acceptanceTemplate, report *acceptanceReport) int64 {
	t.Helper()
	report.Phase = "building_fixture"
	report.Build.ProgressEvery = 10_000
	report.Build.Memory = map[string]acceptanceMemStats{"before_build": acceptanceMemory()}
	run, err := store.StartRun(context.Background(), history.RunConfig{
		StartedAtMS: settings.StartMS, IntervalNS: int64(time.Second),
		Config: config, Aggregation: aggregation,
	})
	if err != nil {
		t.Fatal(err)
	}
	plotsStarted := time.Now()
	var payloadBytes int64
	maxRSS := report.Build.Memory["before_build"].RSSBytes
	for index := 0; index < settings.Samples; index++ {
		seq := int64(index + 1)
		timestamp := settings.StartMS + int64(index)*acceptanceIntervalMS
		payload := template.render(seq, timestamp)
		payloadBytes += int64(len(payload))
		err = store.Enqueue(history.Sample{
			RunID: run.ID, Seq: seq, Timestamp: timestamp,
			DurationNS: int64(time.Second), Series: payload,
		})
		if err != nil {
			t.Fatalf("入队 seq=%d: %v", seq, err)
		}
		if seq%batchSize == 0 || index+1 == settings.Samples {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			err = store.Flush(ctx)
			cancel()
			if err != nil {
				t.Fatalf("刷新 seq=%d: %v", seq, err)
			}
			depth, dropped, lastError := acceptanceWriterState(store)
			if depth != 0 || dropped != 0 || lastError != "" {
				t.Fatalf("写入器状态 seq=%d depth=%d dropped=%d last_error=%q", seq, depth, dropped, lastError)
			}
		}
		if seq%10_000 == 0 || index+1 == settings.Samples {
			memory := acceptanceMemory()
			if memory.RSSBytes > maxRSS {
				maxRSS = memory.RSSBytes
			}
			t.Logf("生成进度 %d/%d (%.1f%%), elapsed=%s rss=%d", index+1, settings.Samples,
				100*float64(index+1)/float64(settings.Samples), time.Since(plotsStarted).Round(time.Second), memory.RSSBytes)
		}
	}
	elapsed := time.Since(plotsStarted)
	report.Build.DurationNS = elapsed.Nanoseconds()
	report.Build.SamplesPerSecond = float64(settings.Samples) / elapsed.Seconds()
	report.Build.SamplePayloadBytes = payloadBytes
	report.Build.AveragePayloadBytes = float64(payloadBytes) / float64(settings.Samples)
	report.Build.ProjectedFullPayload = int64(report.Build.AveragePayloadBytes * acceptanceDefaultCount)
	report.Build.Memory["after_build"] = acceptanceMemory()
	if report.Build.Memory["after_build"].RSSBytes > maxRSS {
		maxRSS = report.Build.Memory["after_build"].RSSBytes
	}
	report.Build.MaxSampledRSSBytes = maxRSS
	return run.ID
}

func validateAcceptanceFixture(t *testing.T, store *Store, settings acceptanceSettings, runID int64, template acceptanceTemplate, report *acceptanceReport) {
	t.Helper()
	assertAcceptanceSampleCount(t, store, runID, int64(settings.Samples))
	var oldestRun, oldestSeq, oldestTS int64
	var oldestPayload []byte
	err := store.readDB.QueryRow(`
SELECT run_id, seq, ts_ms, payload FROM samples
WHERE run_id = ? ORDER BY ts_ms, seq LIMIT 1`, runID).Scan(&oldestRun, &oldestSeq, &oldestTS, &oldestPayload)
	if err != nil {
		t.Fatal(err)
	}
	if oldestRun != runID || oldestSeq != 1 || oldestTS != settings.StartMS ||
		!bytes.Equal(oldestPayload, template.render(1, settings.StartMS)) {
		t.Fatalf("最旧样本不匹配: run=%d seq=%d ts=%d", oldestRun, oldestSeq, oldestTS)
	}
	var newestSeq, newestTS int64
	var newestPayload []byte
	err = store.readDB.QueryRow(`
SELECT seq, ts_ms, payload FROM samples
WHERE run_id = ? ORDER BY ts_ms DESC, seq DESC LIMIT 1`, runID).Scan(&newestSeq, &newestTS, &newestPayload)
	wantNewestTS := settings.EndMS - acceptanceIntervalMS
	if err != nil || newestSeq != int64(settings.Samples) || newestTS != wantNewestTS ||
		!bytes.Equal(newestPayload, template.render(int64(settings.Samples), wantNewestTS)) {
		t.Fatalf("最新样本不匹配: seq=%d ts=%d err=%v", newestSeq, newestTS, err)
	}

	for _, step := range rollupSteps {
		var count int64
		if err = store.readDB.QueryRow(`SELECT count(*) FROM rollups WHERE run_id = ? AND step_ms = ?`, runID, step).Scan(&count); err != nil {
			t.Fatal(err)
		}
		expected := (wantNewestTS / step) - (settings.StartMS / step) + 1
		if count != expected {
			t.Fatalf("step=%d rollups=%d, want %d", step, count, expected)
		}
	}

	meta, err := store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if meta.From != settings.StartMS || meta.To != wantNewestTS+1 || len(meta.Watermarks) != 1 ||
		meta.Watermarks[0] != (history.Watermark{RunID: runID, Seq: int64(settings.Samples)}) {
		t.Fatalf("meta from=%d to=%d watermarks=%+v", meta.From, meta.To, meta.Watermarks)
	}
	middleSeq := int64(settings.Samples/2 + 1)
	middleTS := settings.StartMS + (middleSeq-1)*acceptanceIntervalMS
	point, err := store.Point(context.Background(), history.PointRequest{At: middleTS, RunID: runID})
	if err != nil || !point.Found || point.Sample == nil || point.Sample.Seq != middleSeq || point.Sample.Timestamp != middleTS ||
		!bytes.Equal(point.Sample.Series, template.render(middleSeq, middleTS)) {
		t.Fatalf("中点身份校验失败: point=%+v err=%v", point, err)
	}

	overview, err := store.Range(context.Background(), history.RangeRequest{
		From: settings.StartMS, To: settings.EndMS, RunID: runID, MaxPoints: maxCurvePoints,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(overview)
	if err != nil {
		t.Fatal(err)
	}
	if overview.StepMS == 0 || len(overview.Points) > maxHeatmapPoints || len(encoded) > maxResponseBytes {
		t.Fatalf("全跨度概览 step=%d points=%d bytes=%d", overview.StepMS, len(overview.Points), len(encoded))
	}
	validateAcceptanceBoundary(t, store, settings, runID, template)
	refreshAcceptanceStorageReport(t, store, settings, runID, report)
}

func validateAcceptanceBoundary(t *testing.T, store *Store, settings acceptanceSettings, runID int64, template acceptanceTemplate) {
	t.Helper()
	name, index, cell, ok := template.firstCell(history.Gauge)
	if !ok {
		t.Fatal("未找到 gauge 边界校验单元")
	}
	from, to := settings.StartMS+500, settings.StartMS+2500
	response, err := store.Range(context.Background(), history.RangeRequest{
		From: from, To: to, RunID: runID, MaxPoints: 1, Plots: []string{name},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.From != from || response.To != to || response.StepMS != 10_000 || len(response.Points) != 1 || response.Points[0].Timestamp != from {
		t.Fatalf("边界裁剪结果 from=%d to=%d step=%d points=%+v", response.From, response.To, response.StepMS, response.Points)
	}
	var values map[string][]*float64
	if err = json.Unmarshal(response.Points[0].Series, &values); err != nil {
		t.Fatal(err)
	}
	want := (template.value(cell, 2, settings.StartMS+1000) + template.value(cell, 3, settings.StartMS+2000)) / 2
	if index >= len(values[name]) || values[name][index] == nil || math.Abs(*values[name][index]-want) > 1e-9 {
		t.Fatalf("边界 gauge %s[%d]=%v, want %v", name, index, values[name], want)
	}
}

func validateAcceptanceConcurrency(t *testing.T, store *Store, settings acceptanceSettings, runID int64, template acceptanceTemplate) acceptanceConcurrencyReport {
	t.Helper()
	const batches = 3
	const reads = 5
	started := make(chan struct{})
	result := make(chan error, 1)
	begin := time.Now()
	go func() {
		<-started
		for batch := 0; batch < batches; batch++ {
			for index := 0; index < batchSize; index++ {
				seq := int64(index + 1)
				timestamp := settings.StartMS + int64(index)*acceptanceIntervalMS
				if err := store.Enqueue(history.Sample{
					RunID: runID, Seq: seq, Timestamp: timestamp, DurationNS: int64(time.Second),
					Series: template.render(seq, timestamp),
				}); err != nil {
					result <- err
					return
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			err := store.Flush(ctx)
			cancel()
			if err != nil {
				result <- err
				return
			}
		}
		result <- nil
	}()
	close(started)
	name, _, _, _ := template.firstCell(history.Gauge)
	from := settings.EndMS - int64(time.Hour/time.Millisecond)
	if from < settings.StartMS {
		from = settings.StartMS
	}
	for index := 0; index < reads; index++ {
		if _, err := store.Range(context.Background(), history.RangeRequest{
			From: from, To: settings.EndMS, RunID: runID, MaxPoints: maxCurvePoints, Plots: []string{name},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-result; err != nil {
		t.Fatalf("并发重写失败: %v", err)
	}
	depth, dropped, lastError := acceptanceWriterState(store)
	if depth != 0 || dropped != 0 || lastError != "" {
		t.Fatalf("并发后 writer depth=%d dropped=%d last_error=%q", depth, dropped, lastError)
	}
	return acceptanceConcurrencyReport{RewriteSamples: batches * batchSize, ReadQueries: reads, DurationNS: time.Since(begin).Nanoseconds()}
}

func validateAcceptanceReopenPoint(t *testing.T, store *Store, settings acceptanceSettings, runID int64, template acceptanceTemplate) {
	t.Helper()
	point, err := store.Point(context.Background(), history.PointRequest{At: settings.StartMS, RunID: runID})
	if err != nil || !point.Found || point.Sample == nil || point.Sample.Seq != 1 ||
		!bytes.Equal(point.Sample.Series, template.render(1, settings.StartMS)) {
		t.Fatalf("关闭重开后旧点校验失败: point=%+v err=%v", point, err)
	}
}

func runAcceptanceQueries(t *testing.T, store *Store, settings acceptanceSettings, runID int64, report *acceptanceReport) {
	t.Helper()
	report.Queries["meta"] = measureAcceptanceQuery(t, settings.Rounds, func() (acceptanceObservation, error) {
		value, err := store.Meta(context.Background())
		return acceptanceObservation{value: value}, err
	})
	middle := settings.StartMS + int64(settings.Samples/2)*acceptanceIntervalMS
	perRunPoint, err := store.Point(context.Background(), history.PointRequest{At: middle, RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	globalPoint, err := store.Point(context.Background(), history.PointRequest{At: middle})
	if err != nil {
		t.Fatal(err)
	}
	if !globalPoint.Found || globalPoint.Sample == nil || globalPoint.Sample.RunID != runID || !reflect.DeepEqual(globalPoint, perRunPoint) {
		t.Fatalf("global point=%+v, want per-run point=%+v", globalPoint, perRunPoint)
	}
	report.Queries["point"] = measureAcceptanceQuery(t, settings.Rounds, func() (acceptanceObservation, error) {
		value, err := store.Point(context.Background(), history.PointRequest{At: middle, RunID: runID})
		points := 0
		if value.Found {
			points = 1
		}
		return acceptanceObservation{value: value, points: points}, err
	})
	report.Queries["point_global"] = measureAcceptanceQuery(t, settings.Rounds, func() (acceptanceObservation, error) {
		value, err := store.Point(context.Background(), history.PointRequest{At: middle})
		points := 0
		if value.Found {
			points = 1
		}
		return acceptanceObservation{value: value, points: points}, err
	})
	for _, window := range []struct {
		name     string
		duration time.Duration
	}{
		{"1m", time.Minute}, {"1h", time.Hour}, {"24h", 24 * time.Hour}, {"7d", 7 * 24 * time.Hour},
	} {
		from := settings.EndMS - window.duration.Milliseconds()
		if from < settings.StartMS {
			from = settings.StartMS
		}
		effective := settings.EndMS - from
		report.Queries[window.name] = measureAcceptanceQuery(t, settings.Rounds, func() (acceptanceObservation, error) {
			value, err := store.Range(context.Background(), history.RangeRequest{
				From: from, To: settings.EndMS, RunID: runID, MaxPoints: maxCurvePoints,
			})
			return acceptanceObservation{
				value: value, effectiveRangeMS: effective,
				stepMS: value.StepMS, points: len(value.Points),
			}, err
		})
	}
	globalFrom := settings.EndMS - (7 * 24 * time.Hour).Milliseconds()
	if globalFrom < settings.StartMS {
		globalFrom = settings.StartMS
	}
	perRunRange, err := store.Range(context.Background(), history.RangeRequest{
		From: globalFrom, To: settings.EndMS, RunID: runID, MaxPoints: maxCurvePoints,
	})
	if err != nil {
		t.Fatal(err)
	}
	globalRange, err := store.Range(context.Background(), history.RangeRequest{
		From: globalFrom, To: settings.EndMS, MaxPoints: maxCurvePoints,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(globalRange, perRunRange) {
		t.Fatalf("global 7d range semantics differ: global step=%d points=%d, per-run step=%d points=%d",
			globalRange.StepMS, len(globalRange.Points), perRunRange.StepMS, len(perRunRange.Points))
	}
	report.Queries["7d_global"] = measureAcceptanceQuery(t, settings.Rounds, func() (acceptanceObservation, error) {
		value, err := store.Range(context.Background(), history.RangeRequest{
			From: globalFrom, To: settings.EndMS, MaxPoints: maxCurvePoints,
		})
		return acceptanceObservation{
			value: value, effectiveRangeMS: settings.EndMS - globalFrom,
			stepMS: value.StepMS, points: len(value.Points),
		}, err
	})
	afterQueries := acceptanceMemory()
	report.Build.Memory["after_queries"] = afterQueries
	if afterQueries.RSSBytes > report.Build.MaxSampledRSSBytes {
		report.Build.MaxSampledRSSBytes = afterQueries.RSSBytes
	}
}

func refreshAcceptanceStorageReport(t *testing.T, store *Store, settings acceptanceSettings, runID int64, report *acceptanceReport) {
	t.Helper()
	if report.Build.Memory == nil {
		report.Build.Memory = map[string]acceptanceMemStats{}
	}
	if err := store.readDB.QueryRow(`
SELECT count(*), coalesce(sum(length(payload)), 0) FROM samples WHERE run_id = ?`, runID).Scan(
		&report.Build.SampleCount, &report.Build.SamplePayloadBytes); err != nil {
		t.Fatal(err)
	}
	report.Build.RollupRows = map[string]int64{}
	report.Build.RollupPayloadBytes = map[string]int64{}
	for _, step := range rollupSteps {
		key := fmt.Sprintf("%d", step)
		var rows, payloadBytes int64
		if err := store.readDB.QueryRow(`
SELECT count(*), coalesce(sum(length(payload)), 0)
FROM rollups WHERE run_id = ? AND step_ms = ?`, runID, step).Scan(&rows, &payloadBytes); err != nil {
			t.Fatal(err)
		}
		report.Build.RollupRows[key], report.Build.RollupPayloadBytes[key] = rows, payloadBytes
	}
	report.Build.DBBytes = acceptanceFileSize(settings.DBPath)
	report.Build.WALBytes = acceptanceFileSize(settings.DBPath + "-wal")
	if report.Build.SampleCount > 0 {
		report.Build.AveragePayloadBytes = float64(report.Build.SamplePayloadBytes) / float64(report.Build.SampleCount)
		report.Build.ProjectedFullPayload = int64(report.Build.AveragePayloadBytes * acceptanceDefaultCount)
		report.Build.ProjectedFullDBAndWAL = (report.Build.DBBytes + report.Build.WALBytes) * int64(acceptanceDefaultCount) / report.Build.SampleCount
	}
	depth, dropped, lastError := acceptanceWriterState(store)
	report.Build.WriterFinalQueueDepth = depth
	report.Build.WriterDropped = dropped
	report.Build.WriterLastError = lastError
}

func assertAcceptanceSampleCount(t *testing.T, store *Store, runID, want int64) {
	t.Helper()
	var count int64
	if err := store.readDB.QueryRow(`SELECT count(*) FROM samples WHERE run_id = ?`, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("samples=%d, want %d", count, want)
	}
}
