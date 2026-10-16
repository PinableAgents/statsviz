package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

const schemaVersion = 1

// schemaSQL 必须与 docs/sqlite-history-schema.sql 的实际迁移保持一致。
const schemaSQL = `
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
`

func migrate(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("读取 SQLite 版本: %w", err)
	}
	if version > schemaVersion {
		return fmt.Errorf("SQLite 历史库版本 %d 高于当前支持版本 %d", version, schemaVersion)
	}
	if version == schemaVersion {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开始 SQLite 迁移: %w", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("创建 SQLite 历史表: %w", err)
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
		return fmt.Errorf("写入 SQLite 版本: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("提交 SQLite 迁移: %w", err)
	}
	return nil
}
