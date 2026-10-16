// Package sqlite 使用 SQLite 持久化 statsviz 历史快照和多级聚合。
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
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PinableAgents/statsviz/history"
	"github.com/gofrs/flock"
	_ "modernc.org/sqlite"
)

const (
	defaultRetention           = 7 * 24 * time.Hour
	defaultFlushInterval       = time.Second
	defaultQueueCapacity       = 60
	batchSize                  = 60
	maxUnixMillis        int64 = 253_402_300_799_999 // 9999-12-31T23:59:59.999Z
)

// Options 控制历史保留时间、刷新周期和有界队列容量。
type Options struct {
	Retention     time.Duration
	FlushInterval time.Duration
	QueueCapacity int
}

type flushRequest struct {
	close bool
	done  chan error
}

type queuedGap struct {
	history.Gap
}

// Store 是 SQLite 历史存储实现。
type Store struct {
	path          string
	retention     time.Duration
	flushInterval time.Duration
	queueCapacity int
	writeDB       *sql.DB
	readDB        *sql.DB
	fileLock      *flock.Flock

	queueMu sync.Mutex
	queue   []history.Sample
	gaps    []queuedGap
	dropped int64

	writeMu sync.Mutex
	wake    chan struct{}
	request chan flushRequest
	done    chan struct{}

	stateMu      sync.RWMutex
	lastCommitMS int64
	lastError    string

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
	now       func() time.Time
}

// Open 打开或创建历史数据库，并获取同路径的进程独占锁。
func Open(path string, options Options) (*Store, error) {
	return openWithClock(path, options, time.Now)
}

func openWithClock(path string, options Options, now func() time.Time) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("SQLite 历史库路径不能为空")
	}
	if now == nil {
		return nil, fmt.Errorf("SQLite 历史库时钟不能为空")
	}
	if options.Retention == 0 {
		options.Retention = defaultRetention
	}
	if options.FlushInterval == 0 {
		options.FlushInterval = defaultFlushInterval
	}
	if options.QueueCapacity == 0 {
		options.QueueCapacity = defaultQueueCapacity
	}
	if options.Retention < 0 || options.FlushInterval < 0 || options.QueueCapacity < 1 {
		return nil, fmt.Errorf("SQLite 历史库选项无效")
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("解析 SQLite 历史库路径: %w", err)
	}
	if err = os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return nil, fmt.Errorf("创建 SQLite 历史库目录: %w", err)
	}
	fileLock := flock.New(absPath + ".lock")
	locked, err := fileLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("获取 SQLite 历史库锁: %w", err)
	}
	if !locked {
		return nil, fmt.Errorf("SQLite 历史库已被其他进程使用: %s", absPath)
	}

	writeDB, err := openDB(absPath, false)
	if err != nil {
		_ = fileLock.Close()
		return nil, err
	}
	cleanup := func() {
		_ = writeDB.Close()
		_ = fileLock.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = writeDB.ExecContext(ctx, "PRAGMA auto_vacuum = INCREMENTAL"); err != nil {
		cleanup()
		return nil, fmt.Errorf("设置 SQLite 增量回收: %w", err)
	}
	if err = migrate(ctx, writeDB); err != nil {
		cleanup()
		return nil, err
	}

	readDB, err := openDB(absPath, true)
	if err != nil {
		cleanup()
		return nil, err
	}
	s := &Store{
		path:          absPath,
		retention:     options.Retention,
		flushInterval: options.FlushInterval,
		queueCapacity: options.QueueCapacity,
		writeDB:       writeDB,
		readDB:        readDB,
		fileLock:      fileLock,
		wake:          make(chan struct{}, 1),
		request:       make(chan flushRequest),
		done:          make(chan struct{}),
		now:           now,
	}
	if err = s.cleanup(ctx); err != nil {
		_ = readDB.Close()
		cleanup()
		return nil, fmt.Errorf("首次清理 SQLite 历史库: %w", err)
	}
	if err = s.recoverTails(ctx); err != nil {
		_ = readDB.Close()
		cleanup()
		return nil, fmt.Errorf("恢复 SQLite 聚合尾部: %w", err)
	}
	go s.writer()
	return s, nil
}

func openDB(path string, readOnly bool) (*sql.DB, error) {
	u := &url.URL{Scheme: "file", Path: path}
	query := u.Query()
	query.Add("_pragma", "busy_timeout(2000)")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "synchronous(NORMAL)")
	if readOnly {
		query.Add("_pragma", "query_only(1)")
	}
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("打开 SQLite 历史库: %w", err)
	}
	if readOnly {
		db.SetMaxOpenConns(2)
		db.SetMaxIdleConns(2)
	} else {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("连接 SQLite 历史库: %w", err)
	}
	return db, nil
}

// StartRun 结束先前运行并由 SQLite 分配新的运行编号。
func (s *Store) StartRun(ctx context.Context, cfg history.RunConfig) (history.Run, error) {
	if s.closed.Load() {
		return history.Run{}, history.ErrClosed
	}
	if cfg.StartedAtMS <= 0 || cfg.StartedAtMS > maxUnixMillis || cfg.IntervalNS <= 0 {
		return history.Run{}, fmt.Errorf("%w: 运行配置无效", history.ErrInvalidQuery)
	}
	if len(cfg.Config) > maxRunMetadataBytes {
		return history.Run{}, history.ErrTooLarge
	}
	if !json.Valid(cfg.Config) {
		return history.Run{}, fmt.Errorf("%w: 运行配置无效", history.ErrInvalidQuery)
	}
	if cfg.Aggregation == nil {
		cfg.Aggregation = map[string]history.SeriesSpec{}
	}
	aggregationJSON, err := json.Marshal(cfg.Aggregation)
	if err != nil {
		return history.Run{}, fmt.Errorf("编码聚合配置: %w", err)
	}
	if len(aggregationJSON) > maxRunMetadataBytes-len(cfg.Config) {
		return history.Run{}, history.ErrTooLarge
	}
	hashInput := append(append(append([]byte(nil), cfg.Config...), 0), aggregationJSON...)
	hashSum := sha256.Sum256(hashInput)
	hash := hex.EncodeToString(hashSum[:])

	if err = s.Flush(ctx); err != nil {
		return history.Run{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return history.Run{}, fmt.Errorf("开始新运行事务: %w", err)
	}
	defer tx.Rollback()
	if err = sealOpenRuns(ctx, tx); err != nil {
		return history.Run{}, err
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO runs(started_at_ms, sample_interval_ns, config_hash, config_json, aggregation_json)
VALUES(?, ?, ?, ?, ?)`, cfg.StartedAtMS, cfg.IntervalNS, hash, string(cfg.Config), string(aggregationJSON))
	if err != nil {
		return history.Run{}, fmt.Errorf("创建新运行: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return history.Run{}, fmt.Errorf("读取新运行编号: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return history.Run{}, fmt.Errorf("提交新运行: %w", err)
	}
	return history.Run{
		ID: id, StartedAtMS: cfg.StartedAtMS, IntervalNS: cfg.IntervalNS,
		ConfigHash: hash, Config: cloneBytes(cfg.Config), Aggregation: cloneAggregation(cfg.Aggregation),
	}, nil
}

// Enqueue 非阻塞地加入不可变快照；队列满时丢弃最旧样本。
func (s *Store) Enqueue(sample history.Sample) error {
	if s.closed.Load() {
		return history.ErrClosed
	}
	if sample.RunID <= 0 || sample.Seq <= 0 || sample.Timestamp <= 0 || sample.Timestamp > maxUnixMillis || sample.DurationNS < 0 || !validObject(sample.Series) {
		return fmt.Errorf("%w: 样本无效", history.ErrInvalidQuery)
	}
	sample.Series = cloneBytes(sample.Series)
	dropped := false
	s.queueMu.Lock()
	if s.closed.Load() {
		s.queueMu.Unlock()
		return history.ErrClosed
	}
	if len(s.queue) >= s.queueCapacity {
		old := s.queue[0]
		copy(s.queue, s.queue[1:])
		s.queue = s.queue[:len(s.queue)-1]
		s.addGapLocked(old, "queue_overflow")
		s.dropped++
		dropped = true
	}
	s.queue = append(s.queue, sample)
	depth := len(s.queue)
	s.queueMu.Unlock()
	if depth >= batchSize {
		s.signalWriter()
	} else {
		// 每次入队都唤醒写协程，使很短的自定义刷新周期仍能及时生效。
		s.signalWriter()
	}
	if dropped {
		return history.ErrQueueFull
	}
	return nil
}

func validObject(raw json.RawMessage) bool {
	if !json.Valid(raw) {
		return false
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return false
	}
	for _, encoded := range object {
		var values []json.RawMessage
		if json.Unmarshal(encoded, &values) != nil || values == nil {
			return false
		}
		for _, item := range values {
			item = bytes.TrimSpace(item)
			if bytes.Equal(item, []byte("null")) {
				continue
			}
			// 整个 payload 已通过 json.Valid，RawMessage 也已按数组元素切分；
			// JSON 数字只能以负号或十进制数字开头，无需为每个单元创建 Decoder。
			if len(item) == 0 || (item[0] != '-' && (item[0] < '0' || item[0] > '9')) {
				return false
			}
			number, err := strconv.ParseFloat(string(item), 64)
			if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
				return false
			}
		}
	}
	return true
}

func (s *Store) addGapLocked(sample history.Sample, reason string) {
	next := history.Gap{RunID: sample.RunID, From: sample.Timestamp, To: sample.Timestamp + 1, FirstSeq: sample.Seq, LastSeq: sample.Seq, Reason: reason}
	if len(s.gaps) > 0 {
		last := &s.gaps[len(s.gaps)-1].Gap
		if last.RunID == next.RunID && last.Reason == next.Reason && last.LastSeq+1 == next.FirstSeq {
			last.LastSeq = next.LastSeq
			last.From = min64(last.From, next.From)
			last.To = max64(last.To, next.To)
			return
		}
	}
	s.gaps = append(s.gaps, queuedGap{Gap: next})
}

func (s *Store) signalWriter() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Flush 等待调用时已经排队的样本和缺口提交到数据库。
func (s *Store) Flush(ctx context.Context) error {
	if s.closed.Load() {
		// Close 开始后不再读取 closeErr；closeErr 只由 Close 在 writer 和
		// 连接全部结束后发布，避免 Flush 与 Close 并发读写同一字段。
		return history.ErrClosed
	}
	req := flushRequest{done: make(chan error, 1)}
	select {
	case s.request <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return history.ErrClosed
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return history.ErrClosed
	}
}

// Close 刷新队列、关闭连接并释放进程锁；重复调用返回同一结果。
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		req := flushRequest{close: true, done: make(chan error, 1)}
		s.request <- req
		s.closeErr = <-req.done
		<-s.done
		if s.closeErr == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			s.writeMu.Lock()
			tx, err := s.writeDB.BeginTx(ctx, nil)
			if err == nil {
				err = sealOpenRuns(ctx, tx)
				if err == nil {
					err = tx.Commit()
				}
				if err != nil {
					_ = tx.Rollback()
				}
			}
			s.writeMu.Unlock()
			cancel()
			if err != nil {
				s.closeErr = fmt.Errorf("关闭时封存运行: %w", err)
			}
		}
		s.closeErr = joinErrors(s.closeErr, s.readDB.Close())
		s.closeErr = joinErrors(s.closeErr, s.writeDB.Close())
		s.closeErr = joinErrors(s.closeErr, s.fileLock.Close())
	})
	return s.closeErr
}

type contextExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// sealOpenRuns 用每个运行最高已持久化 seq 的时间封存。墙钟回拨时
// ended_at_ms 允许早于 started_at_ms；无样本运行则回退到自身启动时间。
func sealOpenRuns(ctx context.Context, execer contextExecer) error {
	if _, err := execer.ExecContext(ctx, `
UPDATE rollups SET is_final = 1
WHERE is_final = 0
  AND run_id IN (SELECT id FROM runs WHERE ended_at_ms IS NULL)`); err != nil {
		return fmt.Errorf("封存旧运行聚合: %w", err)
	}
	if _, err := execer.ExecContext(ctx, `
UPDATE runs
SET ended_at_ms = coalesce(
    (SELECT s.ts_ms FROM samples s
     WHERE s.run_id = runs.id
     ORDER BY s.seq DESC
     LIMIT 1),
    started_at_ms)
WHERE ended_at_ms IS NULL`); err != nil {
		return fmt.Errorf("封存旧运行: %w", err)
	}
	return nil
}

func cloneBytes[T ~[]byte](value T) T {
	return append(T(nil), value...)
}

func cloneAggregation(value map[string]history.SeriesSpec) map[string]history.SeriesSpec {
	copyMap := make(map[string]history.SeriesSpec, len(value))
	for name, spec := range value {
		spec.Kinds = append([]history.Kind(nil), spec.Kinds...)
		copyMap[name] = spec
	}
	return copyMap
}

func (s *Store) setWriteError(err error) {
	s.stateMu.Lock()
	if err == nil {
		s.lastError = ""
	} else {
		s.lastError = err.Error()
	}
	s.stateMu.Unlock()
}

func (s *Store) markCommit() {
	s.stateMu.Lock()
	s.lastCommitMS = s.now().UnixMilli()
	s.lastError = ""
	s.stateMu.Unlock()
}

const openRunIDsSQL = `
SELECT id FROM runs
WHERE ended_at_ms IS NULL
ORDER BY id`

const tailSampleForRunSQL = `
SELECT run_id, seq, ts_ms, duration_ns, payload
FROM samples INDEXED BY sqlite_autoindex_samples_1
WHERE run_id = ?
ORDER BY seq DESC
LIMIT 1`

// recoverTails 只重建每个未封存运行的最后一个 10 秒桶及其父桶。
// 崩溃恢复的工作量因此与历史长度无关。
func (s *Store) recoverTails(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	rows, err := s.writeDB.QueryContext(ctx, openRunIDsSQL)
	if err != nil {
		return err
	}
	var runIDs []int64
	for rows.Next() {
		var runID int64
		if err = rows.Scan(&runID); err != nil {
			_ = rows.Close()
			return err
		}
		runIDs = append(runIDs, runID)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	var tails []history.Sample
	for _, runID := range runIDs {
		var sample history.Sample
		err = s.writeDB.QueryRowContext(ctx, tailSampleForRunSQL, runID).Scan(&sample.RunID, &sample.Seq, &sample.Timestamp, &sample.DurationNS, &sample.Series)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		tails = append(tails, sample)
	}
	if len(tails) == 0 {
		return nil
	}
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = s.rebuildAffected(ctx, tx, tails); err != nil {
		return err
	}
	return tx.Commit()
}

func joinErrors(current, next error) error {
	if current == nil {
		return next
	}
	if next == nil {
		return current
	}
	return errors.Join(current, next)
}
