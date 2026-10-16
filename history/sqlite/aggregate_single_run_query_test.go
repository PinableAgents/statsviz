package sqlite

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/PinableAgents/statsviz/history"
)

func TestGlobalAggregateUsesSingleRunPrimaryKeyAndKeepsMultiRunResults(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour, QueueCapacity: 300})
	defer store.Close()
	base := alignTime(time.Now().Add(-time.Hour).UnixMilli(), 600_000)
	aggregation := map[string]history.SeriesSpec{"value": {Kinds: []history.Kind{history.Gauge}}}
	run1 := startTestRun(t, store, base, int64(10*time.Second), aggregation, `{}`)
	for seq := int64(1); seq <= 120; seq++ {
		if err := enqueue(t, store, run1.ID, seq, base+(seq-1)*10_000, int64(10*time.Second), `{"value":[1]}`); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, store)
	if _, err := store.writeDB.Exec(`DELETE FROM samples WHERE run_id = ?`, run1.ID); err != nil {
		t.Fatal(err)
	}

	rangeRequest := history.RangeRequest{From: base, To: base + 1_200_000, MaxPoints: 2}
	global, err := store.Range(context.Background(), rangeRequest)
	if err != nil {
		t.Fatal(err)
	}
	perRunRequest := rangeRequest
	perRunRequest.RunID = run1.ID
	perRun, err := store.Range(context.Background(), perRunRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(global, perRun) {
		t.Fatalf("single-run global response differs from per-run response:\nglobal=%+v\nper-run=%+v", global, perRun)
	}
	if len(global.Runs) != 1 || len(global.Points) != 2 {
		t.Fatalf("single-run global runs=%d points=%d", len(global.Runs), len(global.Points))
	}
	if got := effectiveAggregateRunID(0, global.Runs); got != run1.ID {
		t.Fatalf("single-run effective run ID=%d, want %d", got, run1.ID)
	}
	assertAggregateQueryPlans(t, store, run1.ID, base)

	run2 := startTestRun(t, store, base+600_000, int64(10*time.Second), aggregation, `{}`)
	for seq := int64(1); seq <= 60; seq++ {
		if err := enqueue(t, store, run2.ID, seq, base+600_000+(seq-1)*10_000, int64(10*time.Second), `{"value":[2]}`); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, store)
	if _, err := store.writeDB.Exec(`DELETE FROM samples WHERE run_id = ?`, run2.ID); err != nil {
		t.Fatal(err)
	}

	multiple, err := store.Range(context.Background(), rangeRequest)
	if err != nil {
		t.Fatal(err)
	}
	if len(multiple.Runs) != 2 || effectiveAggregateRunID(0, multiple.Runs) != 0 {
		t.Fatalf("multi-run global runs=%d effective=%d", len(multiple.Runs), effectiveAggregateRunID(0, multiple.Runs))
	}
	seen := map[int64]bool{}
	for _, point := range multiple.Points {
		seen[point.RunID] = true
	}
	if !seen[run1.ID] || !seen[run2.ID] {
		t.Fatalf("multi-run global point runs=%v", seen)
	}
}

func assertAggregateQueryPlans(t *testing.T, store *Store, runID, base int64) {
	t.Helper()
	runPlan := aggregateQueryPlan(t, store.writeDB, aggregateComponentsRunSQL, runID, int64(600_000), base, base+1_200_000)
	if !strings.Contains(runPlan, "PRIMARY KEY (run_id=? AND step_ms=? AND bucket_start_ms>? AND bucket_start_ms<?)") || strings.Contains(runPlan, "rollups_time") || strings.Contains(runPlan, "TEMP B-TREE") {
		t.Fatalf("single-run aggregate plan does not use the primary-key range:\n%s", runPlan)
	}
	globalPlan := aggregateQueryPlan(t, store.writeDB, aggregateComponentsGlobalSQL, int64(600_000), base, base+1_200_000)
	if !strings.Contains(globalPlan, "rollups_time (step_ms=? AND bucket_start_ms>? AND bucket_start_ms<?)") || strings.Contains(globalPlan, "TEMP B-TREE") {
		t.Fatalf("multi-run aggregate plan does not retain the global time index:\n%s", globalPlan)
	}
}

func aggregateQueryPlan(t *testing.T, db *sql.DB, query string, args ...any) string {
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
	return strings.Join(details, "\n")
}
