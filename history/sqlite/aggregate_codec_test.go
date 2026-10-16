package sqlite

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/PinableAgents/statsviz/history"
)

func TestAggregateCodecV1RoundTrip(t *testing.T) {
	want := aggregatePayload{Plots: map[string][]aggregateCell{
		"all-kinds": {
			{Kind: history.Gauge, WeightedSum: 12.5, WeightNS: math.MaxInt64, Sum: -3.25, Min: -9, Max: 18, Last: 7.5, Count: math.MaxInt64 - 1, HasValue: true},
			{Kind: history.Rate, WeightedSum: -2.5, WeightNS: 0, Sum: 0, Min: 0, Max: 0, Last: 0, Count: 0, HasValue: true},
			{Kind: history.Delta, WeightedSum: 0, WeightNS: 0, Sum: 42, Min: 2, Max: 40, Last: 40, Count: 2, HasValue: true},
			{Kind: history.Histogram, Sum: 99, Min: 0, Max: 90, Last: 9, Count: 3, HasValue: true},
			{Kind: history.Event, Min: 1, Max: 3, Last: 3, Count: 2, HasValue: true},
			{Kind: history.Gauge, HasValue: false},
		},
	}}

	encoded, err := encodeAggregatePayload(want)
	if err != nil {
		t.Fatalf("encodeAggregatePayload: %v", err)
	}
	if !json.Valid(encoded) {
		t.Fatalf("编码结果不是合法 JSON: %q", encoded)
	}
	if !strings.HasPrefix(string(encoded), `[1,{`) {
		t.Fatalf("编码结果未使用 v1 tuple 格式: %s", encoded)
	}

	got, err := decodeAggregatePayload(encoded)
	if err != nil {
		t.Fatalf("decodeAggregatePayload: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("往返结果不一致\n got: %#v\nwant: %#v", got, want)
	}
}

func TestAggregateCodecReadsLegacyObject(t *testing.T) {
	legacy := []byte(`{"plots":{"legacy":[{"kind":"gauge","weighted_sum":12.5,"weight_ns":9223372036854775807,"sum":3,"min":-1,"max":9,"last":4,"count":9223372036854775806,"has_value":true},{"kind":"event","has_value":false}]}}`)
	want := aggregatePayload{Plots: map[string][]aggregateCell{
		"legacy": {
			{Kind: history.Gauge, WeightedSum: 12.5, WeightNS: math.MaxInt64, Sum: 3, Min: -1, Max: 9, Last: 4, Count: math.MaxInt64 - 1, HasValue: true},
			{Kind: history.Event, HasValue: false},
		},
	}}

	got, err := decodeAggregatePayload(legacy)
	if err != nil {
		t.Fatalf("读取旧 object 格式: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("旧格式解码不一致\n got: %#v\nwant: %#v", got, want)
	}
}

func TestAggregateCodecRejectsUnknownVersionAndInvalidTuples(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "unknown version", raw: `[2,{}]`},
		{name: "missing plots", raw: `[1]`},
		{name: "extra envelope item", raw: `[1,{},0]`},
		{name: "legacy object without plots", raw: `{"v":1}`},
		{name: "short tuple", raw: `[1,{"p":[[0,true,1,2,3,4,5,6]]}]`},
		{name: "long tuple", raw: `[1,{"p":[[0,true,1,2,3,4,5,6,7,8]]}]`},
		{name: "unknown kind", raw: `[1,{"p":[[5,true,1,2,3,4,5,6,7]]}]`},
		{name: "unknown large kind", raw: `[1,{"p":[[4294967296,true,1,2,3,4,5,6,7]]}]`},
		{name: "kind is not integer", raw: `[1,{"p":[[0.5,true,1,2,3,4,5,6,7]]}]`},
		{name: "has value is not bool", raw: `[1,{"p":[[0,1,1,2,3,4,5,6,7]]}]`},
		{name: "weight is not integer", raw: `[1,{"p":[[0,true,1,2.5,3,4,5,6,7]]}]`},
		{name: "count overflows int64", raw: `[1,{"p":[[0,true,1,2,3,4,5,6,9223372036854775808]]}]`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeAggregatePayload([]byte(test.raw)); err == nil {
				t.Fatalf("非法 payload 被静默接受: %s", test.raw)
			}
		})
	}
}

func TestAggregateCodecRejectsUnknownInternalKind(t *testing.T) {
	payload := aggregatePayload{Plots: map[string][]aggregateCell{
		"invalid": {{Kind: history.Kind("future"), HasValue: true}},
	}}
	if _, err := encodeAggregatePayload(payload); err == nil {
		t.Fatal("未知内部 kind 被静默编码")
	}
}

func TestAggregateCodecLeavesRawSampleJSONUnchanged(t *testing.T) {
	raw := json.RawMessage(` {"plot":[0,null,9223372036854775807]} `)
	got, cells, err := filterRawSeries(raw, nil, 0)
	if err != nil {
		t.Fatalf("filterRawSeries: %v", err)
	}
	if cells != 2 {
		t.Fatalf("数字单元数 = %d，want 2", cells)
	}
	if string(got) != string(raw) {
		t.Fatalf("原始样本 JSON 被改写\n got: %q\nwant: %q", got, raw)
	}
}
