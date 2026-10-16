package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/PinableAgents/statsviz/history"
	"github.com/gofrs/flock"
)

const (
	acceptanceToolID        = "statsviz.sqlite.history.acceptance"
	acceptanceFormatVersion = 2
	acceptanceDefaultCount  = 7 * 24 * 60 * 60
	acceptanceMinimumCount  = 3600
	acceptanceIntervalMS    = int64(1000)
	// 2033 年的固定 UTC 桶边界，避免加速生成时保留窗口随墙钟漂移。
	acceptanceFixedNowMS = int64(2_000_000_400_000)
	acceptanceDBName     = "statsviz-history-acceptance-v2.db"
	acceptanceReportName = "statsviz-history-acceptance-v2.json"
)

type acceptanceSettings struct {
	Dir        string
	DBPath     string
	ReportPath string
	Samples    int
	Rounds     int
	StartMS    int64
	EndMS      int64
}

type acceptanceMarker struct {
	ToolID        string
	FormatVersion int
	FixtureID     string
	State         string
	SampleCount   int
	IntervalMS    int64
	FixedNowMS    int64
	ConfigHash    string
	ShapeHash     string
	NumericCells  int
	RunID         int64
}

type acceptanceMemStats struct {
	AllocBytes      uint64 `json:"alloc_bytes"`
	TotalAllocBytes uint64 `json:"total_alloc_bytes"`
	SysBytes        uint64 `json:"sys_bytes"`
	HeapAllocBytes  uint64 `json:"heap_alloc_bytes"`
	HeapInuseBytes  uint64 `json:"heap_inuse_bytes"`
	NumGC           uint32 `json:"num_gc"`
	RSSBytes        int64  `json:"rss_bytes,omitempty"`
}

type acceptanceBuildReport struct {
	Reused                bool                          `json:"reused"`
	DurationNS            int64                         `json:"duration_ns"`
	SamplesPerSecond      float64                       `json:"samples_per_second"`
	SampleCount           int64                         `json:"sample_count"`
	SamplePayloadBytes    int64                         `json:"sample_payload_bytes"`
	AveragePayloadBytes   float64                       `json:"average_payload_bytes"`
	RollupRows            map[string]int64              `json:"rollup_rows"`
	RollupPayloadBytes    map[string]int64              `json:"rollup_payload_bytes"`
	DBBytes               int64                         `json:"db_bytes"`
	WALBytes              int64                         `json:"wal_bytes"`
	ProjectedFullPayload  int64                         `json:"projected_full_payload_bytes"`
	ProjectedFullDBAndWAL int64                         `json:"projected_full_db_and_wal_bytes"`
	Memory                map[string]acceptanceMemStats `json:"memory"`
	MaxSampledRSSBytes    int64                         `json:"max_sampled_rss_bytes,omitempty"`
	ProgressEvery         int                           `json:"progress_every"`
	WriterDropped         int64                         `json:"writer_dropped"`
	WriterLastError       string                        `json:"writer_last_error"`
	WriterFinalQueueDepth int                           `json:"writer_final_queue_depth"`
}

type acceptanceQueryReport struct {
	Rounds                   int     `json:"rounds"`
	EffectiveRangeMS         int64   `json:"effective_range_ms,omitempty"`
	FirstReadNS              int64   `json:"first_read_ns"`
	WarmupNS                 int64   `json:"warmup_ns"`
	P50NS                    int64   `json:"p50_ns"`
	P95NS                    int64   `json:"p95_ns"`
	MaxNS                    int64   `json:"max_ns"`
	ResponseBytes            int     `json:"response_bytes"`
	MaxResponseBytes         int     `json:"max_response_bytes"`
	AllocBytesTotal          uint64  `json:"alloc_bytes_total"`
	AllocBytesPerOp          float64 `json:"alloc_bytes_per_op"`
	SelectedStepMS           int64   `json:"selected_step_ms,omitempty"`
	PointCount               int     `json:"point_count,omitempty"`
	OSCacheExplicitlyCleared bool    `json:"os_cache_explicitly_cleared"`
}

type acceptanceConcurrencyReport struct {
	RewriteSamples int   `json:"rewrite_samples"`
	ReadQueries    int   `json:"read_queries"`
	DurationNS     int64 `json:"duration_ns"`
}

type acceptanceReport struct {
	ToolID        string                           `json:"tool_id"`
	FormatVersion int                              `json:"format_version"`
	FixtureID     string                           `json:"fixture_id"`
	Passed        bool                             `json:"passed"`
	Phase         string                           `json:"phase"`
	GeneratedAt   string                           `json:"generated_at_utc"`
	GoVersion     string                           `json:"go_version"`
	GOOS          string                           `json:"goos"`
	GOARCH        string                           `json:"goarch"`
	DBPath        string                           `json:"db_path"`
	ReportPath    string                           `json:"report_path"`
	SampleCount   int                              `json:"sample_count"`
	IntervalMS    int64                            `json:"interval_ms"`
	SpanMS        int64                            `json:"span_ms"`
	FixedNowMS    int64                            `json:"fixed_now_ms"`
	ConfigHash    string                           `json:"config_hash"`
	ShapeHash     string                           `json:"snapshot_shape_hash"`
	ConfigBytes   int                              `json:"config_bytes"`
	SnapshotBytes int                              `json:"snapshot_bytes"`
	PlotCount     int                              `json:"plot_count"`
	NumericCells  int                              `json:"numeric_cells"`
	Kinds         map[history.Kind]int             `json:"kind_cells"`
	Build         acceptanceBuildReport            `json:"build"`
	Queries       map[string]acceptanceQueryReport `json:"queries"`
	Concurrency   acceptanceConcurrencyReport      `json:"concurrency"`
	Notes         []string                         `json:"notes"`
}

type acceptanceCell struct {
	base  float64
	kind  history.Kind
	null  bool
	index int
}

type acceptancePlot struct {
	name        string
	encodedName []byte
	cells       []acceptanceCell
}

type acceptanceTemplate struct {
	plots        []acceptancePlot
	numericCells int
	kinds        map[history.Kind]int
	shapeHash    string
	baseBytes    int
}

type acceptanceObservation struct {
	value            any
	effectiveRangeMS int64
	stepMS           int64
	points           int
}

func loadAcceptanceSettings(t *testing.T) acceptanceSettings {
	t.Helper()
	dir := strings.TrimSpace(os.Getenv("STATSVIZ_HISTORY_ACCEPTANCE_DIR"))
	if dir == "" {
		t.Fatal("STATSVIZ_HISTORY_ACCEPTANCE_DIR 必须指向 /tmp 下的专用目录")
	}
	dir = acceptanceTempDir(t, dir)
	samples := acceptanceEnvInt(t, "STATSVIZ_HISTORY_ACCEPTANCE_SAMPLES", acceptanceDefaultCount)
	if samples < acceptanceMinimumCount || samples > acceptanceDefaultCount {
		t.Fatalf("STATSVIZ_HISTORY_ACCEPTANCE_SAMPLES=%d，允许范围为 %d..%d", samples, acceptanceMinimumCount, acceptanceDefaultCount)
	}
	rounds := acceptanceEnvInt(t, "STATSVIZ_HISTORY_ACCEPTANCE_ROUNDS", 20)
	if rounds < 3 || rounds > 1000 {
		t.Fatalf("STATSVIZ_HISTORY_ACCEPTANCE_ROUNDS=%d，允许范围为 3..1000", rounds)
	}
	if samples == acceptanceDefaultCount && rounds < 20 {
		t.Fatal("全量 604800 验收至少需要 20 轮查询")
	}
	span := int64(samples) * acceptanceIntervalMS
	return acceptanceSettings{
		Dir: dir, DBPath: filepath.Join(dir, acceptanceDBName),
		ReportPath: filepath.Join(dir, acceptanceReportName),
		Samples:    samples, Rounds: rounds,
		StartMS: acceptanceFixedNowMS - span, EndMS: acceptanceFixedNowMS,
	}
}

func acceptanceEnvInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("%s=%q 不是整数", name, raw)
	}
	return value
}

func acceptanceTempDir(t *testing.T, raw string) string {
	t.Helper()
	if !filepath.IsAbs(raw) {
		t.Fatalf("验收输出目录必须是绝对路径: %s", raw)
	}
	if err := os.MkdirAll(raw, 0o755); err != nil {
		t.Fatal(err)
	}
	realDir, err := filepath.EvalSymlinks(raw)
	if err != nil {
		t.Fatal(err)
	}
	allowed := []string{os.TempDir(), "/tmp"}
	for _, root := range allowed {
		realRoot, rootErr := filepath.EvalSymlinks(root)
		if rootErr != nil {
			continue
		}
		rel, relErr := filepath.Rel(realRoot, realDir)
		if relErr == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && !filepath.IsAbs(rel) {
			return realDir
		}
	}
	t.Fatalf("验收输出目录必须位于 /tmp 或当前系统临时目录的专用子目录: %s", realDir)
	return ""
}

func newAcceptanceTemplate(t *testing.T, raw json.RawMessage, aggregation map[string]history.SeriesSpec) acceptanceTemplate {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded map[string][]any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("解码 runtime 快照: %v", err)
	}
	names := make([]string, 0, len(decoded))
	for name := range decoded {
		names = append(names, name)
	}
	sort.Strings(names)
	template := acceptanceTemplate{kinds: map[history.Kind]int{}, baseBytes: len(raw)}
	shape := sha256.New()
	for _, name := range names {
		encodedName, err := json.Marshal(name)
		if err != nil {
			t.Fatal(err)
		}
		plot := acceptancePlot{name: name, encodedName: encodedName, cells: make([]acceptanceCell, len(decoded[name]))}
		_, _ = fmt.Fprintf(shape, "%s:%d:", name, len(plot.cells))
		for index, rawValue := range decoded[name] {
			cell := acceptanceCell{kind: kindAt(aggregation[name], index), index: index}
			switch value := rawValue.(type) {
			case nil:
				cell.null = true
				_, _ = shape.Write([]byte("null;"))
			case json.Number:
				cell.base, err = strconv.ParseFloat(value.String(), 64)
				if err != nil {
					t.Fatalf("解码 %s[%d]: %v", name, index, err)
				}
				if cell.kind == "" {
					cell.kind = history.Gauge
				}
				template.numericCells++
				template.kinds[cell.kind]++
				_, _ = fmt.Fprintf(shape, "%s;", cell.kind)
			default:
				t.Fatalf("runtime 快照 %s[%d] 类型为 %T，期望数字或 null", name, index, rawValue)
			}
			plot.cells[index] = cell
		}
		template.plots = append(template.plots, plot)
	}
	template.shapeHash = hex.EncodeToString(shape.Sum(nil))
	for _, kind := range []history.Kind{history.Gauge, history.Rate, history.Delta, history.Histogram} {
		if template.kinds[kind] == 0 {
			t.Fatalf("完整 runtime 快照缺少 %s 类型数值单元", kind)
		}
	}
	return template
}

func (template acceptanceTemplate) render(seq, timestamp int64) json.RawMessage {
	buffer := make([]byte, 0, template.baseBytes+128)
	buffer = append(buffer, '{')
	for plotIndex, plot := range template.plots {
		if plotIndex > 0 {
			buffer = append(buffer, ',')
		}
		buffer = append(buffer, plot.encodedName...)
		buffer = append(buffer, ':', '[')
		for index, cell := range plot.cells {
			if index > 0 {
				buffer = append(buffer, ',')
			}
			if cell.null {
				buffer = append(buffer, "null"...)
				continue
			}
			value := template.value(cell, seq, timestamp)
			if cell.kind == history.Event {
				buffer = strconv.AppendInt(buffer, int64(value), 10)
			} else {
				buffer = strconv.AppendFloat(buffer, value, 'g', -1, 64)
			}
		}
		buffer = append(buffer, ']')
	}
	buffer = append(buffer, '}')
	return buffer
}

func (acceptanceTemplate) value(cell acceptanceCell, seq, timestamp int64) float64 {
	switch cell.kind {
	case history.Histogram:
		return cell.base + float64(seq)*float64(cell.index%5+1)
	case history.Delta:
		return float64((seq+int64(cell.index))%7 + 1)
	case history.Event:
		return float64(timestamp)
	case history.Gauge, history.Rate:
		return cell.base + float64((seq+int64(cell.index))%17)*0.125
	default:
		return cell.base
	}
}

// rebaseAcceptanceTemplate 从持久样本恢复生成模板的基准值，使跨进程复用不受
// runtime 瞬时计数器变化影响，并保证并发验证仍是对原 fixture 的幂等重写。
func rebaseAcceptanceTemplate(t *testing.T, template acceptanceTemplate, raw json.RawMessage, seq, timestamp int64) acceptanceTemplate {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded map[string][]any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("解码复用 fixture 基准样本: %v", err)
	}
	for plotIndex := range template.plots {
		plot := &template.plots[plotIndex]
		values, ok := decoded[plot.name]
		if !ok || len(values) != len(plot.cells) {
			t.Fatalf("复用 fixture 基准样本形状不匹配: plot=%s cells=%d want=%d", plot.name, len(values), len(plot.cells))
		}
		for cellIndex := range plot.cells {
			cell := &plot.cells[cellIndex]
			if cell.null {
				if values[cellIndex] != nil {
					t.Fatalf("复用 fixture 基准样本 null 不匹配: %s[%d]", plot.name, cellIndex)
				}
				continue
			}
			number, ok := values[cellIndex].(json.Number)
			if !ok {
				t.Fatalf("复用 fixture 基准样本 %s[%d] 类型为 %T", plot.name, cellIndex, values[cellIndex])
			}
			value, err := strconv.ParseFloat(number.String(), 64)
			if err != nil {
				t.Fatalf("解析复用 fixture 基准样本 %s[%d]: %v", plot.name, cellIndex, err)
			}
			switch cell.kind {
			case history.Histogram:
				cell.base = value - float64(seq)*float64(cell.index%5+1)
			case history.Gauge, history.Rate:
				cell.base = value - float64((seq+int64(cell.index))%17)*0.125
			case history.Delta, history.Event:
				// 这两类完全由 seq/timestamp 决定，不依赖 runtime 基准值。
			default:
				cell.base = value
			}
		}
	}
	if got := template.render(seq, timestamp); !bytes.Equal(got, raw) {
		t.Fatal("复用 fixture 基准样本不能由恢复后的模板精确重现")
	}
	return template
}

func (template acceptanceTemplate) firstCell(kind history.Kind) (string, int, acceptanceCell, bool) {
	for _, plot := range template.plots {
		for index, cell := range plot.cells {
			if !cell.null && cell.kind == kind {
				return plot.name, index, cell, true
			}
		}
	}
	return "", 0, acceptanceCell{}, false
}

func acceptanceConfigHash(config json.RawMessage, aggregation map[string]history.SeriesSpec) string {
	aggregationJSON, _ := json.Marshal(aggregation)
	input := append(append(append([]byte(nil), config...), 0), aggregationJSON...)
	sum := sha256.Sum256(input)
	return hex.EncodeToString(sum[:])
}

func acceptanceFixtureID(settings acceptanceSettings, configHash, shapeHash string) string {
	return acceptanceFixtureIDForPayloadVersion(settings, configHash, shapeHash, aggregatePayloadVersion)
}

func acceptanceFixtureIDForPayloadVersion(settings acceptanceSettings, configHash, shapeHash string, payloadVersion int) string {
	identity, _ := json.Marshal(struct {
		ToolID                  string `json:"tool_id"`
		FormatVersion           int    `json:"format_version"`
		AggregatePayloadVersion int    `json:"aggregate_payload_version"`
		Samples                 int    `json:"samples"`
		IntervalMS              int64  `json:"interval_ms"`
		FixedNowMS              int64  `json:"fixed_now_ms"`
		ConfigHash              string `json:"config_hash"`
		ShapeHash               string `json:"shape_hash"`
		GoVersion               string `json:"go_version"`
		GOOS                    string `json:"goos"`
		GOARCH                  string `json:"goarch"`
	}{acceptanceToolID, acceptanceFormatVersion, payloadVersion, settings.Samples, acceptanceIntervalMS,
		acceptanceFixedNowMS, configHash, shapeHash, runtime.Version(), runtime.GOOS, runtime.GOARCH})
	sum := sha256.Sum256(identity)
	return hex.EncodeToString(sum[:])
}

func acceptanceReadMarker(path string) (acceptanceMarker, error) {
	u := databaseURL(path)
	query := u.Query()
	query.Set("mode", "ro")
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return acceptanceMarker{}, err
	}
	defer db.Close()
	var marker acceptanceMarker
	err = db.QueryRow(`
SELECT tool_id, format_version, fixture_id, state, sample_count, interval_ms,
       fixed_now_ms, config_hash, shape_hash, numeric_cells, run_id
FROM statsviz_acceptance_fixture WHERE singleton = 1`).Scan(
		&marker.ToolID, &marker.FormatVersion, &marker.FixtureID, &marker.State,
		&marker.SampleCount, &marker.IntervalMS, &marker.FixedNowMS,
		&marker.ConfigHash, &marker.ShapeHash, &marker.NumericCells, &marker.RunID)
	return marker, err
}

func acceptanceReadSampleCount(path string, runID int64) (int64, error) {
	u := databaseURL(path)
	query := u.Query()
	query.Set("mode", "ro")
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var count int64
	err = db.QueryRow(`SELECT count(*) FROM samples WHERE run_id = ?`, runID).Scan(&count)
	return count, err
}

// acceptanceFixtureMatches 校验可能影响 fixture 内容的完整身份。
func acceptanceFixtureMatches(marker, expected acceptanceMarker) bool {
	if marker.ToolID != expected.ToolID || marker.FormatVersion != expected.FormatVersion ||
		marker.FixtureID != expected.FixtureID || marker.SampleCount != expected.SampleCount ||
		marker.IntervalMS != expected.IntervalMS || marker.FixedNowMS != expected.FixedNowMS ||
		marker.ConfigHash != expected.ConfigHash || marker.ShapeHash != expected.ShapeHash ||
		marker.NumericCells != expected.NumericCells {
		return false
	}
	return true
}

// acceptanceFixtureReusable 只承认已完整生成的数据。incomplete 表示后续验收尚未
// 全部通过；RunID 和实际样本数共同证明 fixture 的生成阶段已经完成。
func acceptanceFixtureReusable(marker, expected acceptanceMarker, rawCount int64) bool {
	if !acceptanceFixtureMatches(marker, expected) || marker.RunID <= 0 {
		return false
	}
	if marker.State != "complete" && marker.State != "incomplete" {
		return false
	}
	return rawCount == int64(expected.SampleCount)
}

func acceptancePrepareDB(t *testing.T, settings acceptanceSettings, expected acceptanceMarker) bool {
	t.Helper()
	_, statErr := os.Stat(settings.DBPath)
	if errors.Is(statErr, os.ErrNotExist) {
		for _, sidecar := range []string{settings.DBPath + "-wal", settings.DBPath + "-shm"} {
			if _, err := os.Stat(sidecar); err == nil {
				t.Fatalf("拒绝覆盖无主库的 SQLite 旁路文件: %s", sidecar)
			}
		}
		return false
	}
	if statErr != nil {
		t.Fatal(statErr)
	}
	marker, err := acceptanceReadMarker(settings.DBPath)
	if err != nil || marker.ToolID != acceptanceToolID || marker.FormatVersion != acceptanceFormatVersion {
		t.Fatalf("拒绝覆盖不能确认为验收工具所有的数据库 %s: marker=%+v err=%v", settings.DBPath, marker, err)
	}
	if acceptanceFixtureMatches(marker, expected) && marker.RunID > 0 &&
		(marker.State == "complete" || marker.State == "incomplete") {
		rawCount, countErr := acceptanceReadSampleCount(settings.DBPath, marker.RunID)
		if countErr != nil {
			t.Fatalf("核对验收 fixture 实际样本数失败，拒绝覆盖 %s: %v", settings.DBPath, countErr)
		}
		if acceptanceFixtureReusable(marker, expected, rawCount) {
			if marker.State == "incomplete" {
				t.Logf("复用已完整生成、尚待验收 fixture %s", marker.FixtureID)
			} else {
				t.Logf("复用完整 fixture %s", marker.FixtureID)
			}
			return true
		}
	}

	lock := flock.New(settings.DBPath + ".lock")
	locked, lockErr := lock.TryLock()
	if lockErr != nil || !locked {
		_ = lock.Close()
		t.Fatalf("旧 fixture 正在使用，不能重建: locked=%v err=%v", locked, lockErr)
	}
	defer lock.Close()
	defer lock.Unlock()
	for _, target := range []string{settings.DBPath, settings.DBPath + "-wal", settings.DBPath + "-shm"} {
		if err = os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	t.Logf("旧 fixture 状态为 %q 或参数已改变，已安全重建工具专属数据库", marker.State)
	return false
}

func acceptanceEnsureReportOwned(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	var owner struct {
		ToolID string `json:"tool_id"`
	}
	if json.Unmarshal(raw, &owner) != nil || owner.ToolID != acceptanceToolID {
		t.Fatalf("拒绝覆盖非验收工具所有的报告: %s", path)
	}
}

func acceptanceCreateMarker(ctx context.Context, store *Store, marker acceptanceMarker) error {
	tx, err := store.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS statsviz_acceptance_fixture (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    tool_id TEXT NOT NULL,
    format_version INTEGER NOT NULL,
    fixture_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('incomplete', 'complete')),
    sample_count INTEGER NOT NULL,
    interval_ms INTEGER NOT NULL,
    fixed_now_ms INTEGER NOT NULL,
    config_hash TEXT NOT NULL,
    shape_hash TEXT NOT NULL,
    numeric_cells INTEGER NOT NULL,
    run_id INTEGER NOT NULL
)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `
INSERT INTO statsviz_acceptance_fixture(
    singleton, tool_id, format_version, fixture_id, state, sample_count,
    interval_ms, fixed_now_ms, config_hash, shape_hash, numeric_cells, run_id)
VALUES(1, ?, ?, ?, 'incomplete', ?, ?, ?, ?, ?, ?, 0)`,
		marker.ToolID, marker.FormatVersion, marker.FixtureID, marker.SampleCount,
		marker.IntervalMS, marker.FixedNowMS, marker.ConfigHash, marker.ShapeHash, marker.NumericCells); err != nil {
		return err
	}
	return tx.Commit()
}

func acceptanceSetMarkerRun(ctx context.Context, store *Store, runID int64) error {
	_, err := store.writeDB.ExecContext(ctx, `UPDATE statsviz_acceptance_fixture SET run_id = ? WHERE singleton = 1`, runID)
	return err
}

func acceptanceCompleteMarker(ctx context.Context, store *Store, fixtureID string) error {
	_, err := store.writeDB.ExecContext(ctx, `
UPDATE statsviz_acceptance_fixture SET state = 'complete'
WHERE singleton = 1 AND fixture_id = ?`, fixtureID)
	return err
}

func acceptanceWriteReport(t *testing.T, report *acceptanceReport) {
	t.Helper()
	report.Passed = !t.Failed()
	report.GeneratedAt = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Errorf("编码验收报告: %v", err)
		return
	}
	temporary := report.ReportPath + ".tmp"
	if err = os.WriteFile(temporary, append(raw, '\n'), 0o644); err != nil {
		t.Errorf("写入验收报告: %v", err)
		return
	}
	if err = os.Rename(temporary, report.ReportPath); err != nil {
		t.Errorf("发布验收报告: %v", err)
	}
}

func acceptanceMemory() acceptanceMemStats {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return acceptanceMemStats{
		AllocBytes: stats.Alloc, TotalAllocBytes: stats.TotalAlloc, SysBytes: stats.Sys,
		HeapAllocBytes: stats.HeapAlloc, HeapInuseBytes: stats.HeapInuse, NumGC: stats.NumGC,
		RSSBytes: acceptanceRSS(),
	}
}

func acceptanceRSS() int64 {
	output, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0
	}
	kilobytes, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
	if err != nil {
		return 0
	}
	return kilobytes * 1024
}

func acceptanceFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func measureAcceptanceQuery(t *testing.T, rounds int, call func() (acceptanceObservation, error)) acceptanceQueryReport {
	t.Helper()
	run := func() (acceptanceObservation, int, time.Duration) {
		started := time.Now()
		observation, err := call()
		if err != nil {
			t.Fatalf("验收查询失败: %v", err)
		}
		encoded, err := json.Marshal(observation.value)
		if err != nil {
			t.Fatalf("编码验收查询结果: %v", err)
		}
		return observation, len(encoded), time.Since(started)
	}
	_, _, first := run()
	_, _, warmup := run()
	before := acceptanceTotalAlloc()
	durations := make([]time.Duration, 0, rounds)
	responseBytes, maxBytes := 0, 0
	var last acceptanceObservation
	for index := 0; index < rounds; index++ {
		observation, size, elapsed := run()
		last = observation
		responseBytes = size
		if size > maxBytes {
			maxBytes = size
		}
		durations = append(durations, elapsed)
	}
	after := acceptanceTotalAlloc()
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	allocated := after - before
	return acceptanceQueryReport{
		Rounds: rounds, EffectiveRangeMS: last.effectiveRangeMS,
		FirstReadNS: first.Nanoseconds(), WarmupNS: warmup.Nanoseconds(),
		P50NS: acceptancePercentile(durations, 0.50).Nanoseconds(),
		P95NS: acceptancePercentile(durations, 0.95).Nanoseconds(),
		MaxNS: durations[len(durations)-1].Nanoseconds(), ResponseBytes: responseBytes,
		MaxResponseBytes: maxBytes, AllocBytesTotal: allocated,
		AllocBytesPerOp: float64(allocated) / float64(rounds),
		SelectedStepMS:  last.stepMS, PointCount: last.points,
		OSCacheExplicitlyCleared: false,
	}
}

func acceptanceTotalAlloc() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.TotalAlloc
}

func acceptancePercentile(values []time.Duration, percentile float64) time.Duration {
	index := int(float64(len(values))*percentile + 0.999999999)
	if index < 1 {
		index = 1
	}
	if index > len(values) {
		index = len(values)
	}
	return values[index-1]
}

func acceptanceWriterState(store *Store) (depth int, dropped int64, lastError string) {
	store.queueMu.Lock()
	depth, dropped = len(store.queue), store.dropped
	store.queueMu.Unlock()
	store.stateMu.RLock()
	lastError = store.lastError
	store.stateMu.RUnlock()
	return
}

func TestOpenWithClockUsesClockForInitialCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frozen-open.db")
	fixedNow := time.UnixMilli(acceptanceFixedNowMS)
	clock := func() time.Time { return fixedNow }
	options := Options{Retention: time.Hour, FlushInterval: time.Hour, QueueCapacity: batchSize}
	store, err := openWithClock(path, options, clock)
	if err != nil {
		t.Fatal(err)
	}
	cutoff := fixedNow.Add(-time.Hour).UnixMilli()
	run, err := store.StartRun(context.Background(), history.RunConfig{
		StartedAtMS: cutoff - 1, IntervalNS: int64(time.Second),
		Config: json.RawMessage(`{}`), Aggregation: map[string]history.SeriesSpec{},
	})
	if err != nil {
		t.Fatal(err)
	}
	for seq, timestamp := range []int64{cutoff - 1, cutoff} {
		if err = store.Enqueue(history.Sample{
			RunID: run.ID, Seq: int64(seq + 1), Timestamp: timestamp,
			DurationNS: int64(time.Second), Series: json.RawMessage(`{"x":[1]}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = openWithClock(path, options, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var oldCount, boundaryCount int
	if err = store.readDB.QueryRow(`SELECT count(*) FROM samples WHERE ts_ms < ?`, cutoff).Scan(&oldCount); err != nil {
		t.Fatal(err)
	}
	if err = store.readDB.QueryRow(`SELECT count(*) FROM samples WHERE ts_ms = ?`, cutoff).Scan(&boundaryCount); err != nil {
		t.Fatal(err)
	}
	if oldCount != 0 || boundaryCount != 1 {
		t.Fatalf("initial cleanup with frozen clock: old=%d boundary=%d", oldCount, boundaryCount)
	}
}
