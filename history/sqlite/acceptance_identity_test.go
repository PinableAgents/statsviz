package sqlite

import "testing"

func TestAcceptanceFixtureIdentityIncludesAggregatePayloadVersion(t *testing.T) {
	if acceptanceFormatVersion != 2 {
		t.Fatalf("acceptanceFormatVersion = %d，want 2", acceptanceFormatVersion)
	}
	if acceptanceDBName != "statsviz-history-acceptance-v2.db" || acceptanceReportName != "statsviz-history-acceptance-v2.json" {
		t.Fatalf("v2 固定文件名不一致: db=%q report=%q", acceptanceDBName, acceptanceReportName)
	}

	settings := acceptanceSettings{Samples: acceptanceMinimumCount}
	current := acceptanceFixtureID(settings, "config", "shape")
	if current != acceptanceFixtureIDForPayloadVersion(settings, "config", "shape", aggregatePayloadVersion) {
		t.Fatal("acceptanceFixtureID 未使用当前 aggregatePayloadVersion")
	}
	if current == acceptanceFixtureIDForPayloadVersion(settings, "config", "shape", aggregatePayloadVersion+1) {
		t.Fatal("aggregate payload 版本变化未改变 fixture ID")
	}
}

func TestAcceptanceFixtureReuseState(t *testing.T) {
	expected := acceptanceMarker{
		ToolID: acceptanceToolID, FormatVersion: acceptanceFormatVersion,
		FixtureID: "fixture", State: "complete", SampleCount: 4,
		IntervalMS: acceptanceIntervalMS, FixedNowMS: acceptanceFixedNowMS,
		ConfigHash: "config", ShapeHash: "shape", NumericCells: 3,
	}

	tests := []struct {
		name     string
		marker   acceptanceMarker
		rawCount int64
		want     bool
	}{
		{name: "完整生成但尚待验收", marker: acceptanceMarker{RunID: 1, State: "incomplete"}, rawCount: 4, want: true},
		{name: "尚未记录运行", marker: acceptanceMarker{RunID: 0, State: "incomplete"}, rawCount: 4},
		{name: "实际样本不足", marker: acceptanceMarker{RunID: 1, State: "incomplete"}, rawCount: 3},
		{name: "已完整验收", marker: acceptanceMarker{RunID: 1, State: "complete"}, rawCount: 4, want: true},
		{name: "fixture 不匹配", marker: acceptanceMarker{RunID: 1, State: "incomplete", FixtureID: "other"}, rawCount: 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			marker := expected
			marker.RunID = test.marker.RunID
			marker.State = test.marker.State
			if test.marker.FixtureID != "" {
				marker.FixtureID = test.marker.FixtureID
			}
			if got := acceptanceFixtureReusable(marker, expected, test.rawCount); got != test.want {
				t.Fatalf("acceptanceFixtureReusable() = %v，want %v", got, test.want)
			}
		})
	}
}
