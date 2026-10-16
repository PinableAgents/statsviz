package plot

import (
	"bytes"
	"encoding/json"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/PinableAgents/statsviz/history"
)

func TestCollectSnapshotIsReusedForEncoding(t *testing.T) {
	calls := 0
	pl, err := NewList([]UserPlot{{Scatter: &ScatterUserPlot{
		Plot: Scatter{
			Name: "custom",
			Subplots: []Subplot{{
				Name:        "value",
				Aggregation: history.Gauge,
			}},
		},
		Funcs: []func() float64{func() float64 {
			calls++
			return 123
		}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	pl.SetRunID(17)

	snapshot, err := pl.Collect()
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("user function calls = %d, want 1", calls)
	}
	if snapshot.RunID != 17 || snapshot.Seq != 1 {
		t.Fatalf("sample identity = (%d, %d), want (17, 1)", snapshot.RunID, snapshot.Seq)
	}

	var encoded bytes.Buffer
	if err := snapshot.WriteMetrics(&encoded); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("encoding called user function; calls = %d, want 1", calls)
	}
	var message struct {
		Event string `json:"event"`
		Data  struct {
			Series     json.RawMessage `json:"series"`
			Timestamp  int64           `json:"timestamp"`
			DurationNS int64           `json:"duration_ns"`
			RunID      int64           `json:"run_id"`
			Seq        int64           `json:"seq"`
		} `json:"data"`
	}
	if err := json.Unmarshal(encoded.Bytes(), &message); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message.Data.Series, snapshot.Series) {
		t.Fatalf("websocket series differs from collected raw JSON\n got: %s\nwant: %s", message.Data.Series, snapshot.Series)
	}
	if message.Data.Timestamp != snapshot.Timestamp || message.Data.DurationNS != snapshot.DurationNS || message.Data.RunID != snapshot.RunID || message.Data.Seq != snapshot.Seq {
		t.Fatalf("websocket identity does not match snapshot: %+v", message.Data)
	}

	before := bytes.Clone(snapshot.Series)
	if _, err := pl.Collect(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(snapshot.Series, before) {
		t.Fatalf("later collection mutated snapshot\n got: %s\nwant: %s", snapshot.Series, before)
	}
}

func TestSnapshotPreservesLargeIntegerJSON(t *testing.T) {
	want := json.RawMessage(`{"counter":[9007199254740993]}`)
	snapshot := Snapshot{RunID: 1, Seq: 1, Timestamp: 2, DurationNS: 3, Series: want}
	var encoded bytes.Buffer
	if err := snapshot.WriteMetrics(&encoded); err != nil {
		t.Fatal(err)
	}
	var message struct {
		Data struct {
			Series     json.RawMessage `json:"series"`
			DurationNS int64           `json:"duration_ns"`
		} `json:"data"`
	}
	if err := json.Unmarshal(encoded.Bytes(), &message); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message.Data.Series, want) {
		t.Fatalf("series = %s, want exact integer JSON %s", message.Data.Series, want)
	}
	if message.Data.DurationNS != snapshot.DurationNS {
		t.Fatalf("duration_ns = %d, want %d", message.Data.DurationNS, snapshot.DurationNS)
	}
}

func TestAggregationUsesActualPlotNames(t *testing.T) {
	pl, err := NewList(nil)
	if err != nil {
		t.Fatal(err)
	}
	agg := pl.Aggregation()

	assertKinds := func(name string, heatmap bool, want ...history.Kind) {
		t.Helper()
		got, ok := agg[name]
		if !ok {
			t.Fatalf("aggregation is missing plot %q", name)
		}
		if got.Heatmap != heatmap {
			t.Errorf("%s heatmap = %v, want %v", name, got.Heatmap, heatmap)
		}
		if !equalKinds(got.Kinds, want) {
			t.Errorf("%s kinds = %v, want %v", name, got.Kinds, want)
		}
	}

	assertKinds("cgo", false, history.Delta)
	assertKinds("gc-cycles", false, history.Delta, history.Delta, history.Delta)
	assertKinds("cpu-overall", false, history.Rate, history.Rate, history.Rate, history.Rate, history.Rate)
	assertKinds("alloc-free-rate", false, history.Rate, history.Rate)
	assertKinds("mutex-wait", false, history.Rate)
	assertKinds("garbage collection", false, history.Gauge, history.Gauge, history.Gauge, history.Gauge)
	assertKinds("runnable-time", true, history.Histogram)
	assertKinds("size-classes", true, history.Gauge)
	assertKinds("lastgc", false, history.Event)

	if goroutines := agg["goroutines"].Kinds; len(goroutines) > 1 {
		if goroutines[0] != history.Delta {
			t.Errorf("goroutines.created kind = %v, want delta", goroutines[0])
		}
		for i, kind := range goroutines[1:] {
			if kind != history.Gauge {
				t.Errorf("goroutines kind %d = %v, want gauge", i+1, kind)
			}
		}
	} else {
		assertKinds("goroutines", false, history.Gauge)
	}
}

func TestDefaultAggregationDoesNotMutateReusedConfig(t *testing.T) {
	subplots := []Subplot{{Name: "value"}}
	shared := UserPlot{Scatter: &ScatterUserPlot{
		Plot:  Scatter{Name: "shared", Subplots: subplots},
		Funcs: []func() float64{func() float64 { return 1 }},
	}}
	first, err := NewList([]UserPlot{shared})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewList([]UserPlot{shared})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = first.Config()
	}()
	go func() {
		defer wg.Done()
		_ = second.Config()
	}()
	wg.Wait()

	if subplots[0].Aggregation != "" {
		t.Fatalf("shared subplot was mutated to %q", subplots[0].Aggregation)
	}
	for _, list := range []*List{first, second} {
		layout := list.Config().Series[len(list.Config().Series)-1].(Scatter)
		if layout.Subplots[0].Aggregation != history.Gauge {
			t.Fatalf("default aggregation = %q, want gauge", layout.Subplots[0].Aggregation)
		}
	}
}

func TestCollectMultipleListsConcurrently(t *testing.T) {
	const (
		lists      = 6
		iterations = 25
	)
	all := make([]*List, lists)
	for i := range all {
		var err error
		all[i], err = NewList(nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	errCh := make(chan error, lists)
	var wg sync.WaitGroup
	for _, list := range all {
		wg.Add(1)
		go func(pl *List) {
			defer wg.Done()
			for range iterations {
				if _, err := pl.Collect(); err != nil {
					errCh <- err
					return
				}
			}
		}(list)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestCollectIdentityDurationAndLastGC(t *testing.T) {
	runtime.GC()
	var before debug.GCStats
	debug.ReadGCStats(&before)

	pl, err := NewList(nil)
	if err != nil {
		t.Fatal(err)
	}
	pl.SetRunID(9)
	first, err := pl.Collect()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	second, err := pl.Collect()
	if err != nil {
		t.Fatal(err)
	}
	if first.RunID != 9 || first.Seq != 1 || first.DurationNS != 0 {
		t.Fatalf("first identity/duration = (%d, %d, %d)", first.RunID, first.Seq, first.DurationNS)
	}
	if second.RunID != 9 || second.Seq != 2 || second.DurationNS <= 0 {
		t.Fatalf("second identity/duration = (%d, %d, %d)", second.RunID, second.Seq, second.DurationNS)
	}

	var series struct {
		LastGC []int64 `json:"lastgc"`
	}
	if err := json.Unmarshal(first.Series, &series); err != nil {
		t.Fatal(err)
	}
	lastGC := series.LastGC
	if len(lastGC) != 1 || lastGC[0] < before.LastGC.UnixMilli() || lastGC[0] > first.Timestamp {
		t.Fatalf("lastgc = %v, want a real GC timestamp in [%d, %d]", lastGC, before.LastGC.UnixMilli(), first.Timestamp)
	}
}

func equalKinds(a, b []history.Kind) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
