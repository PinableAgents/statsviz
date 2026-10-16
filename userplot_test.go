package statsviz

import (
	"errors"
	"testing"

	"github.com/PinableAgents/statsviz/history"
)

func TestTimeSeriesPlotConfigErrors(t *testing.T) {
	t.Run("empty name", func(t *testing.T) {
		tsb := TimeSeriesPlotConfig{}
		if _, err := tsb.Build(); !errors.Is(err, ErrEmptyPlotName) {
			t.Errorf("Build() returned err = %v, want %v", err, ErrEmptyPlotName)
		}
	})
	t.Run("reserved name", func(t *testing.T) {
		tsb := TimeSeriesPlotConfig{Name: "timestamp"}
		var target ErrReservedPlotName
		if _, err := tsb.Build(); !errors.As(err, &target) {
			t.Errorf("Build() returned err = %v, want %v", err, target)
		}
	})
	t.Run("no time series", func(t *testing.T) {
		tsb := TimeSeriesPlotConfig{Name: "some name"}
		if _, err := tsb.Build(); !errors.Is(err, ErrNoTimeSeries) {
			t.Errorf("Build() returned err = %v, want %v", err, ErrEmptyPlotName)
		}
	})
}

func TestTimeSeriesAggregation(t *testing.T) {
	plotConfig := TimeSeriesPlotConfig{
		Name: "aggregation",
		Series: []TimeSeries{
			{Name: "default", GetValue: func() float64 { return 1 }},
			{Name: "rate", Aggregation: history.Rate, GetValue: func() float64 { return 2 }},
		},
	}
	built, err := plotConfig.Build()
	if err != nil {
		t.Fatal(err)
	}
	subplots := built.timeseries.Plot.Subplots
	if subplots[0].Aggregation != history.Gauge || subplots[1].Aggregation != history.Rate {
		t.Fatalf("aggregations = [%s %s], want [gauge rate]", subplots[0].Aggregation, subplots[1].Aggregation)
	}

	plotConfig.Series[0].Aggregation = history.Kind("average")
	if _, err := plotConfig.Build(); err == nil {
		t.Fatal("Build accepted an unknown aggregation")
	}
}
