package sqlite

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/PinableAgents/statsviz/history"
	"github.com/PinableAgents/statsviz/internal/plot"
)

func runtimeSnapshot523(tb testing.TB) json.RawMessage {
	tb.Helper()
	plots, err := plot.NewList(nil)
	if err != nil {
		tb.Fatal(err)
	}
	snapshot, err := plots.Collect()
	if err != nil {
		tb.Fatal(err)
	}
	var object map[string][]json.RawMessage
	if err = json.Unmarshal(snapshot.Series, &object); err != nil {
		tb.Fatal(err)
	}
	dimensions := 0
	for _, values := range object {
		dimensions += len(values)
	}
	if dimensions != 523 {
		tb.Fatalf("默认 runtime 快照维度 = %d，want 523", dimensions)
	}
	return snapshot.Series
}

func TestValidObjectScalarTypesAndIntegerBytes(t *testing.T) {
	largeIntegers := json.RawMessage(`{"numbers":[9007199254740993,9223372036854775807,-9223372036854775808,null],"empty":[]}`)
	before := bytes.Clone(largeIntegers)
	if !validObject(largeIntegers) {
		t.Fatal("应接受合法 JSON 数字、极大整数和 null")
	}
	if !bytes.Equal(largeIntegers, before) {
		t.Fatalf("validObject 改写了原始整数 JSON: got %s want %s", largeIntegers, before)
	}
	if !validObject(json.RawMessage(" \n {\"values\" : [ null , -0.25e+2 , 0 ]} \t")) {
		t.Fatal("应接受带空白的合法对象与数字")
	}

	invalid := []struct {
		name string
		raw  json.RawMessage
	}{
		{name: "字符串数字", raw: json.RawMessage(`{"x":["123"]}`)},
		{name: "超出 float64 范围", raw: json.RawMessage(`{"x":[1e999999]}`)},
		{name: "布尔值", raw: json.RawMessage(`{"x":[true]}`)},
		{name: "嵌套对象", raw: json.RawMessage(`{"x":[{}]}`)},
		{name: "嵌套数组", raw: json.RawMessage(`{"x":[[1]]}`)},
		{name: "plot 为 null", raw: json.RawMessage(`{"x":null}`)},
		{name: "顶层数组", raw: json.RawMessage(`[1]`)},
		{name: "非法 JSON", raw: json.RawMessage(`{"x":[1,]}`)},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if validObject(test.raw) {
				t.Fatalf("validObject(%s) = true，want false", test.raw)
			}
		})
	}
}

func TestEnqueueRejectsOutOfRangeNumberWithoutPoisoningPending(t *testing.T) {
	store, _ := openTestStore(t, Options{FlushInterval: time.Hour})
	defer store.Close()
	now := time.Now().UnixMilli()
	run := startTestRun(t, store, now, int64(time.Second), map[string]history.SeriesSpec{
		"x": {Kinds: []history.Kind{history.Gauge}},
	}, `{}`)

	if err := enqueue(t, store, run.ID, 1, now, int64(time.Second), `{"x":[1e999999]}`); !errors.Is(err, history.ErrInvalidQuery) {
		t.Fatalf("Enqueue(超出范围数字) error = %v，want ErrInvalidQuery", err)
	}
	const validSeries = `{"x":[9007199254740993]}`
	if err := enqueue(t, store, run.ID, 1, now, int64(time.Second), validSeries); err != nil {
		t.Fatalf("Enqueue(合法样本): %v", err)
	}
	flush(t, store)

	var count int
	var series string
	if err := store.readDB.QueryRow(`SELECT count(*), payload FROM samples WHERE run_id = ?`, run.ID).Scan(&count, &series); err != nil {
		t.Fatal(err)
	}
	if count != 1 || series != validSeries {
		t.Fatalf("持久化样本 = (count=%d, payload=%s)，want (1, %s)", count, series, validSeries)
	}
}

func BenchmarkValidObjectRuntimeSnapshot523(b *testing.B) {
	raw := runtimeSnapshot523(b)
	if !validObject(raw) {
		b.Fatal("默认 runtime 快照未通过 validObject")
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !validObject(raw) {
			b.Fatal("默认 runtime 快照未通过 validObject")
		}
	}
}
