-- SQLite 历史监控存储草案；连接参数由应用在迁移前设置。
-- 时间使用 UTC Unix 毫秒；采样间隔使用纳秒，避免丢失子毫秒精度。
BEGIN;

CREATE TABLE runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at_ms INTEGER NOT NULL,
    ended_at_ms INTEGER,
    sample_interval_ns INTEGER NOT NULL CHECK (sample_interval_ns > 0),
    config_hash TEXT NOT NULL,
    config_json TEXT NOT NULL,
    aggregation_json TEXT NOT NULL,
    payload_version INTEGER NOT NULL DEFAULT 1 CHECK (payload_version > 0)
);

-- 每次采集一行，payload 保存整份快照，不按指标或热力图桶拆行。
CREATE TABLE samples (
    run_id INTEGER NOT NULL REFERENCES runs(id),
    seq INTEGER NOT NULL CHECK (seq > 0),
    ts_ms INTEGER NOT NULL,
    duration_ns INTEGER NOT NULL CHECK (duration_ns >= 0),
    codec TEXT NOT NULL CHECK (codec IN ('json', 'zstd-json')),
    payload BLOB NOT NULL,
    PRIMARY KEY (run_id, seq)
) WITHOUT ROWID;

CREATE INDEX samples_time ON samples(ts_ms, run_id, seq);
CREATE INDEX samples_run_time ON samples(run_id, ts_ms, seq);

-- 每个时间桶保存所有图的聚合结果；is_final 表示不再更新，不代表数据完整。
CREATE TABLE rollups (
    run_id INTEGER NOT NULL REFERENCES runs(id),
    step_ms INTEGER NOT NULL CHECK (step_ms IN (10000, 60000, 600000)),
    bucket_start_ms INTEGER NOT NULL,
    first_seq INTEGER NOT NULL,
    last_seq INTEGER NOT NULL CHECK (last_seq >= first_seq),
    sample_count INTEGER NOT NULL CHECK (sample_count > 0),
    covered_ns INTEGER NOT NULL CHECK (covered_ns >= 0),
    is_final INTEGER NOT NULL DEFAULT 0 CHECK (is_final IN (0, 1)),
    codec TEXT NOT NULL CHECK (codec IN ('json', 'zstd-json')),
    payload BLOB NOT NULL,
    PRIMARY KEY (run_id, step_ms, bucket_start_ms)
) WITHOUT ROWID;

CREATE INDEX rollups_time ON rollups(step_ms, bucket_start_ms, run_id);

-- 恢复写入后补记丢失的采样范围；不使用零值填补缺口。
CREATE TABLE gaps (
    run_id INTEGER NOT NULL REFERENCES runs(id),
    first_seq INTEGER NOT NULL CHECK (first_seq > 0),
    last_seq INTEGER NOT NULL CHECK (last_seq >= first_seq),
    started_at_ms INTEGER NOT NULL,
    ended_at_ms INTEGER NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (run_id, first_seq)
) WITHOUT ROWID;

CREATE INDEX gaps_time ON gaps(started_at_ms, run_id, first_seq);

PRAGMA user_version = 1;
COMMIT;
