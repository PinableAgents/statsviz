package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/PinableAgents/statsviz/history"
)

const queryIndexRegressionBaseMS int64 = 1_700_000_000_000

func newQueryIndexRegressionStore(t *testing.T) *Store {
	t.Helper()
	path := t.TempDir() + "/history.db"
	writeDB, err := openDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = migrate(context.Background(), writeDB); err != nil {
		_ = writeDB.Close()
		t.Fatal(err)
	}
	readDB, err := openDB(path, true)
	if err != nil {
		_ = writeDB.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = readDB.Close()
		_ = writeDB.Close()
	})
	return &Store{
		writeDB:   writeDB,
		readDB:    readDB,
		retention: 24 * time.Hour,
		now: func() time.Time {
			return time.UnixMilli(queryIndexRegressionBaseMS + 100_000)
		},
	}
}

func insertQueryIndexRun(t *testing.T, store *Store, id int64, endedAt any) {
	t.Helper()
	_, err := store.writeDB.ExecContext(context.Background(), `
INSERT INTO runs(id, started_at_ms, ended_at_ms, sample_interval_ns, config_hash, config_json, aggregation_json)
VALUES(?, ?, ?, ?, ?, '{}', '{"value":{"kinds":["gauge"]}}')`,
		id, queryIndexRegressionBaseMS, endedAt, int64(time.Second), fmt.Sprintf("run-%d", id))
	if err != nil {
		t.Fatal(err)
	}
}

func insertQueryIndexSample(t *testing.T, store *Store, runID, seq, timestamp int64, payload string) {
	t.Helper()
	_, err := store.writeDB.ExecContext(context.Background(), `
INSERT INTO samples(run_id, seq, ts_ms, duration_ns, codec, payload)
VALUES(?, ?, ?, ?, 'json', ?)`, runID, seq, timestamp, int64(time.Second), []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
}

func TestPointUsesBoundedTimeSeeksAndPreservesSelectionSemantics(t *testing.T) {
	store := newQueryIndexRegressionStore(t)
	insertQueryIndexRun(t, store, 1, nil)
	insertQueryIndexRun(t, store, 2, nil)
	base := queryIndexRegressionBaseMS
	insertQueryIndexSample(t, store, 1, 1, base+1000, `{"value":[1]}`)
	insertQueryIndexSample(t, store, 1, 2, base+2000, `{"value":[2]}`)
	insertQueryIndexSample(t, store, 1, 3, base+1500, `{"value":[3]}`)
	insertQueryIndexSample(t, store, 1, 4, base+2000, `{"value":[9007199254740993]}`)
	insertQueryIndexSample(t, store, 2, 1, base+2000, `{"value":[21]}`)

	assertPointAfterSeek(t, store.writeDB, pointAfterGlobalSQL, []any{base + 1800}, 1)
	assertPointAfterSeek(t, store.writeDB, pointAfterRunSQL, []any{1, base + 1800}, 2)
	assertQueryPlan(t, store.writeDB, pointBeforeGlobalSQL, []any{base, base + 1800}, "samples_time", "ts_ms>?", "ts_ms<?")
	assertQueryPlan(t, store.writeDB, pointAfterGlobalSQL, []any{base + 1800}, "samples_time", "ts_ms>?")
	assertQueryPlan(t, store.writeDB, pointBeforeRunSQL, []any{1, base, base + 1800}, "samples_run_time", "run_id=?", "ts_ms>?", "ts_ms<?")
	assertQueryPlan(t, store.writeDB, pointAfterRunSQL, []any{1, base + 1800}, "samples_run_time", "run_id=?", "ts_ms>?")

	global, err := store.Point(context.Background(), history.PointRequest{At: base + 2000})
	if err != nil {
		t.Fatal(err)
	}
	if !global.Found || global.Sample.RunID != 2 || global.Sample.Seq != 1 {
		t.Fatalf("global duplicate-time point = %+v", global)
	}

	specified, err := store.Point(context.Background(), history.PointRequest{At: base + 2000, RunID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !specified.Found || specified.Sample.Seq != 4 {
		t.Fatalf("run duplicate-time point = %+v", specified)
	}
	if got := string(specified.Sample.Series); got != `{"value":[9007199254740993]}` {
		t.Fatalf("integer payload changed: %s", got)
	}

	rollback, err := store.Point(context.Background(), history.PointRequest{At: base + 1500, RunID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !rollback.Found || rollback.Sample.Seq != 3 {
		t.Fatalf("clock-rollback point = %+v", rollback)
	}

	tie, err := store.Point(context.Background(), history.PointRequest{At: base + 1750, RunID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !tie.Found || tie.Sample.Timestamp != base+1500 {
		t.Fatalf("equidistant point = %+v", tie)
	}

	defaultTolerance, err := store.Point(context.Background(), history.PointRequest{At: base + 3500, RunID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !defaultTolerance.Found || defaultTolerance.Sample.Seq != 4 {
		t.Fatalf("default target-run tolerance point = %+v", defaultTolerance)
	}
}

func TestRecoverTailsUsesPerRunPrimaryKeyTailLookup(t *testing.T) {
	store := newQueryIndexRegressionStore(t)
	insertQueryIndexRun(t, store, 1, nil)
	insertQueryIndexRun(t, store, 2, nil)
	insertQueryIndexRun(t, store, 3, queryIndexRegressionBaseMS+50_000)
	base := alignTime(queryIndexRegressionBaseMS, 600_000)
	insertQueryIndexSample(t, store, 1, 1, base+1000, `{"value":[1]}`)
	insertQueryIndexSample(t, store, 1, 2, base+21_000, `{"value":[2]}`)
	insertQueryIndexSample(t, store, 3, 1, base+31_000, `{"value":[3]}`)

	assertQueryPlan(t, store.writeDB, tailSampleForRunSQL, []any{1}, "PRIMARY KEY", "run_id=?")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.recoverTails(ctx); err != nil {
		t.Fatal(err)
	}

	var openRollups, emptyRollups, closedRollups int
	if err := store.writeDB.QueryRow(`SELECT count(*) FROM rollups WHERE run_id = 1`).Scan(&openRollups); err != nil {
		t.Fatal(err)
	}
	if err := store.writeDB.QueryRow(`SELECT count(*) FROM rollups WHERE run_id = 2`).Scan(&emptyRollups); err != nil {
		t.Fatal(err)
	}
	if err := store.writeDB.QueryRow(`SELECT count(*) FROM rollups WHERE run_id = 3`).Scan(&closedRollups); err != nil {
		t.Fatal(err)
	}
	if openRollups != len(rollupSteps) || emptyRollups != 0 || closedRollups != 0 {
		t.Fatalf("rollups open=%d empty=%d closed=%d", openRollups, emptyRollups, closedRollups)
	}

	var firstSeq, lastSeq int64
	if err := store.writeDB.QueryRow(`
SELECT first_seq, last_seq FROM rollups
WHERE run_id = 1 AND step_ms = 10000`).Scan(&firstSeq, &lastSeq); err != nil {
		t.Fatal(err)
	}
	if firstSeq != 2 || lastSeq != 2 {
		t.Fatalf("recovered 10s tail seq range = %d..%d", firstSeq, lastSeq)
	}
}

type queryIndexOpcode struct {
	opcode string
	p2     int
	p3     int
	p4     string
}

func assertPointAfterSeek(t *testing.T, db *sql.DB, query string, args []any, keyColumns int) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "EXPLAIN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var opcodes []queryIndexOpcode
	variableRegisters := map[int]struct{}{}
	for rows.Next() {
		var address, p1, p2, p3, p5 int
		var opcode string
		var p4, comment sql.NullString
		if err = rows.Scan(&address, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
			t.Fatal(err)
		}
		opcodes = append(opcodes, queryIndexOpcode{opcode: opcode, p2: p2, p3: p3, p4: p4.String})
		if opcode == "Variable" {
			variableRegisters[p2] = struct{}{}
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(variableRegisters) != keyColumns {
		t.Fatalf("after query has %d bound variables, want exactly %d seek keys", len(variableRegisters), keyColumns)
	}
	for _, opcode := range opcodes {
		if opcode.opcode != "SeekGE" {
			continue
		}
		count, parseErr := strconv.Atoi(opcode.p4)
		if parseErr != nil || count != keyColumns {
			continue
		}
		for register := opcode.p3; register < opcode.p3+keyColumns; register++ {
			if _, ok := variableRegisters[register]; !ok {
				t.Fatalf("SeekGE key register %d is not populated directly by a bound value", register)
			}
		}
		return
	}
	t.Fatalf("after query does not seek directly to all %d bound key columns", keyColumns)
}

func assertQueryPlan(t *testing.T, db *sql.DB, query string, args []any, fragments ...string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
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
	plan := strings.Join(details, "\n")
	for _, fragment := range fragments {
		if !strings.Contains(plan, fragment) {
			t.Fatalf("query plan is missing %q:\n%s", fragment, plan)
		}
	}
	if strings.Contains(plan, "SCAN samples") || strings.Contains(plan, "TEMP B-TREE") {
		t.Fatalf("query plan contains a range scan or temp sort:\n%s", plan)
	}
}
