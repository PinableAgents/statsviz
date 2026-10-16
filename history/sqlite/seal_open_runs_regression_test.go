package sqlite

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestSealOpenRunsOnlyUpdatesUnfinalizedOpenRunRollups(t *testing.T) {
	db := newSealOpenRunsTestDB(t)
	const (
		rollbackRunID = 1
		closedRunID   = 2
		emptyRunID    = 3
	)
	insertSealTestRun(t, db, rollbackRunID, 2000, nil)
	insertSealTestRun(t, db, closedRunID, 3000, int64(9000))
	insertSealTestRun(t, db, emptyRunID, 4000, nil)
	insertSealTestSample(t, db, rollbackRunID, 1, 2500)
	insertSealTestSample(t, db, rollbackRunID, 2, 1500)
	insertSealTestRollup(t, db, rollbackRunID, 0, true)
	insertSealTestRollup(t, db, rollbackRunID, 10_000, false)
	insertSealTestRollup(t, db, closedRunID, 0, false)

	// UPDATE OF 会在 SET 到相同值时照常触发。旧实现会碰到已经 final
	// 的行并被该触发器中止；带 is_final=0 过滤的实现不会触发它。
	if _, err := db.Exec(`
CREATE TRIGGER reject_final_rollup_rewrite
BEFORE UPDATE OF is_final ON rollups
WHEN OLD.is_final = 1
BEGIN
    SELECT RAISE(ABORT, 'final rollup rewritten');
END`); err != nil {
		t.Fatal(err)
	}

	if err := sealOpenRuns(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	assertSealTestRollupFinal(t, db, rollbackRunID, 0, true)
	assertSealTestRollupFinal(t, db, rollbackRunID, 10_000, true)
	assertSealTestRollupFinal(t, db, closedRunID, 0, false)
	assertSealTestRunEnd(t, db, rollbackRunID, 1500)
	assertSealTestRunEnd(t, db, closedRunID, 9000)
	assertSealTestRunEnd(t, db, emptyRunID, 4000)
}

func newSealOpenRunsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := openDB(t.TempDir()+"/history.db", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err = migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db
}

func insertSealTestRun(t *testing.T, db *sql.DB, id, startedAt int64, endedAt any) {
	t.Helper()
	_, err := db.Exec(`
INSERT INTO runs(id, started_at_ms, ended_at_ms, sample_interval_ns, config_hash, config_json, aggregation_json)
VALUES(?, ?, ?, ?, ?, '{}', '{}')`, id, startedAt, endedAt, int64(time.Second), id)
	if err != nil {
		t.Fatal(err)
	}
}

func insertSealTestSample(t *testing.T, db *sql.DB, runID, seq, timestamp int64) {
	t.Helper()
	_, err := db.Exec(`
INSERT INTO samples(run_id, seq, ts_ms, duration_ns, codec, payload)
VALUES(?, ?, ?, ?, 'json', '{}')`, runID, seq, timestamp, int64(time.Second))
	if err != nil {
		t.Fatal(err)
	}
}

func insertSealTestRollup(t *testing.T, db *sql.DB, runID, bucketStart int64, final bool) {
	t.Helper()
	_, err := db.Exec(`
INSERT INTO rollups(
    run_id, step_ms, bucket_start_ms, first_seq, last_seq,
    sample_count, covered_ns, is_final, codec, payload
) VALUES(?, 10000, ?, 1, 1, 1, ?, ?, 'json', '{}')`,
		runID, bucketStart, int64(time.Second), final)
	if err != nil {
		t.Fatal(err)
	}
}

func assertSealTestRollupFinal(t *testing.T, db *sql.DB, runID, bucketStart int64, want bool) {
	t.Helper()
	var got bool
	if err := db.QueryRow(`
SELECT is_final FROM rollups
WHERE run_id = ? AND step_ms = 10000 AND bucket_start_ms = ?`, runID, bucketStart).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("run %d bucket %d final=%t, want %t", runID, bucketStart, got, want)
	}
}

func assertSealTestRunEnd(t *testing.T, db *sql.DB, runID, want int64) {
	t.Helper()
	var got int64
	if err := db.QueryRow(`SELECT ended_at_ms FROM runs WHERE id = ?`, runID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("run %d ended_at_ms=%d, want %d", runID, got, want)
	}
}
