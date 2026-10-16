package statsviz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/PinableAgents/statsviz/history"
)

type fakeHistoryStore struct {
	startFn   func(context.Context, history.RunConfig) (history.Run, error)
	enqueueFn func(history.Sample) error
	flushFn   func(context.Context) error
	metaFn    func(context.Context) (history.Meta, error)
	rangeFn   func(context.Context, history.RangeRequest) (history.RangeResponse, error)
	pointFn   func(context.Context, history.PointRequest) (history.PointResponse, error)

	closed atomic.Bool
}

func (s *fakeHistoryStore) StartRun(ctx context.Context, cfg history.RunConfig) (history.Run, error) {
	if s.startFn != nil {
		return s.startFn(ctx, cfg)
	}
	return history.Run{ID: 42, StartedAtMS: cfg.StartedAtMS, IntervalNS: cfg.IntervalNS, Config: cfg.Config, Aggregation: cfg.Aggregation}, nil
}

func (s *fakeHistoryStore) Enqueue(sample history.Sample) error {
	if s.enqueueFn != nil {
		return s.enqueueFn(sample)
	}
	return nil
}

func (s *fakeHistoryStore) Flush(ctx context.Context) error {
	if s.flushFn != nil {
		return s.flushFn(ctx)
	}
	return nil
}

func (s *fakeHistoryStore) Meta(ctx context.Context) (history.Meta, error) {
	if s.metaFn != nil {
		return s.metaFn(ctx)
	}
	return history.Meta{}, nil
}

func (s *fakeHistoryStore) Range(ctx context.Context, req history.RangeRequest) (history.RangeResponse, error) {
	if s.rangeFn != nil {
		return s.rangeFn(ctx, req)
	}
	return history.RangeResponse{From: req.From, To: req.To}, nil
}

func (s *fakeHistoryStore) Point(ctx context.Context, req history.PointRequest) (history.PointResponse, error) {
	if s.pointFn != nil {
		return s.pointFn(ctx, req)
	}
	return history.PointResponse{RequestedAt: req.At}, nil
}

func (s *fakeHistoryStore) Close() error {
	s.closed.Store(true)
	return nil
}

func TestHistorySampleAndWebsocketShareSeries(t *testing.T) {
	samples := make(chan history.Sample, 2)
	store := &fakeHistoryStore{enqueueFn: func(sample history.Sample) error {
		samples <- sample
		return nil
	}}
	var calls atomic.Int64
	custom, err := (TimeSeriesPlotConfig{
		Name: "custom-history",
		Series: []TimeSeries{{
			Name: "value",
			GetValue: func() float64 {
				calls.Add(1)
				return 123
			},
		}},
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(History(store), TimeseriesPlot(custom), SendFrequency(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	httpServer := httptest.NewServer(srv.Ws())
	defer httpServer.Close()
	u, err := url.Parse(httpServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	u.Scheme = "ws"
	ws, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	var cfg struct {
		Data struct {
			History struct {
				Enabled    bool  `json:"enabled"`
				RunID      int64 `json:"run_id"`
				IntervalMS int64 `json:"interval_ms"`
			} `json:"history"`
		} `json:"data"`
	}
	if err := ws.ReadJSON(&cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.Data.History.Enabled || cfg.Data.History.RunID != 42 || cfg.Data.History.IntervalMS != 20 {
		t.Fatalf("unexpected history websocket config: %+v", cfg.Data.History)
	}

	for i := range 2 {
		var message struct {
			Data struct {
				RunID      int64           `json:"run_id"`
				Seq        int64           `json:"seq"`
				Timestamp  int64           `json:"timestamp"`
				DurationNS int64           `json:"duration_ns"`
				Series     json.RawMessage `json:"series"`
			} `json:"data"`
		}
		if err := ws.ReadJSON(&message); err != nil {
			t.Fatal(err)
		}
		var sample history.Sample
		select {
		case sample = <-samples:
		case <-time.After(time.Second):
			t.Fatal("history sample was not enqueued")
		}
		if message.Data.RunID != sample.RunID || message.Data.Seq != sample.Seq ||
			message.Data.Timestamp != sample.Timestamp || message.Data.DurationNS != sample.DurationNS {
			t.Fatalf("websocket identity %+v does not match sample %+v", message.Data, sample)
		}
		if !bytes.Equal(message.Data.Series, sample.Series) {
			t.Fatalf("websocket and history did not share raw series JSON\nws: %s\ndb: %s", message.Data.Series, sample.Series)
		}
		if i == 1 && sample.DurationNS <= 0 {
			t.Fatalf("second sample duration_ns = %d, want actual positive interval", sample.DurationNS)
		}
	}
	if calls.Load() < 2 {
		t.Fatalf("custom sampler calls = %d, want at least 2", calls.Load())
	}
}

func TestHistoryRoutesAndDisabledState(t *testing.T) {
	t.Run("registered root", func(t *testing.T) {
		srv, err := NewServer(Root("/metrics"))
		if err != nil {
			t.Fatal(err)
		}
		defer srv.Close()
		mux := http.NewServeMux()
		srv.Register(mux)
		resp := serveRequest(mux, http.MethodGet, "/metrics/history/meta", nil)
		if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), `"enabled":false`) {
			t.Fatalf("disabled meta: status=%d body=%s", resp.Code, resp.Body.String())
		}
		if got := resp.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control = %q, want no-store", got)
		}
		resp = serveRequest(mux, http.MethodGet, "/metrics/history/range?from=1&to=2", nil)
		if resp.Code != http.StatusServiceUnavailable {
			t.Fatalf("disabled range status = %d, want 503", resp.Code)
		}
	})

	t.Run("strip prefix", func(t *testing.T) {
		srv, err := NewServer(History(&fakeHistoryStore{}))
		if err != nil {
			t.Fatal(err)
		}
		defer srv.Close()
		handler := http.StripPrefix("/mounted/history/", srv.History())
		resp := serveRequest(handler, http.MethodGet, "/mounted/history/meta", nil)
		if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), `"enabled":true`) {
			t.Fatalf("stripped meta: status=%d body=%s", resp.Code, resp.Body.String())
		}
	})
}

func TestHistoryHTTPValidationAndErrors(t *testing.T) {
	store := &fakeHistoryStore{}
	srv, err := NewServer(History(store))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	handler := srv.History()

	tests := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{"method", http.MethodPost, "/debug/statsviz/history/meta", http.StatusMethodNotAllowed},
		{"not found", http.MethodGet, "/debug/statsviz/history/missing", http.StatusNotFound},
		{"missing from", http.MethodGet, "/debug/statsviz/history/range?to=2", http.StatusBadRequest},
		{"nan", http.MethodGet, "/debug/statsviz/history/range?from=NaN&to=2", http.StatusBadRequest},
		{"overflow", http.MethodGet, "/debug/statsviz/history/range?from=1&to=999999999999999999999", http.StatusBadRequest},
		{"negative max points", http.MethodGet, "/debug/statsviz/history/range?from=1&to=2&max_points=-1", http.StatusBadRequest},
		{"zero max points", http.MethodGet, "/debug/statsviz/history/range?from=1&to=2&max_points=0", http.StatusBadRequest},
		{"bad interval", http.MethodGet, "/debug/statsviz/history/range?from=2&to=2", http.StatusBadRequest},
		{"negative tolerance", http.MethodGet, "/debug/statsviz/history/point?at=1&tolerance_ms=-1", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := serveRequest(handler, tt.method, tt.path, nil)
			if resp.Code != tt.want {
				t.Fatalf("status = %d, want %d; body=%s", resp.Code, tt.want, resp.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil || body["error"] == nil {
				t.Fatalf("error body is not uniform JSON: %s (%v)", resp.Body.String(), err)
			}
		})
	}

	store.rangeFn = func(context.Context, history.RangeRequest) (history.RangeResponse, error) {
		return history.RangeResponse{}, history.ErrTooLarge
	}
	resp := serveRequest(handler, http.MethodGet, "/debug/statsviz/history/range?from=1&to=2", nil)
	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("ErrTooLarge status = %d, want 413", resp.Code)
	}

	store.rangeFn = func(ctx context.Context, _ history.RangeRequest) (history.RangeResponse, error) {
		<-ctx.Done()
		return history.RangeResponse{}, ctx.Err()
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	resp = serveRequest(handler, http.MethodGet, "/debug/statsviz/history/range?from=1&to=2", canceled)
	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("canceled query status = %d, want 503", resp.Code)
	}

	large := json.RawMessage(`{"value":"` + strings.Repeat("x", maxHistoryResponseBytes) + `"}`)
	store.rangeFn = func(context.Context, history.RangeRequest) (history.RangeResponse, error) {
		return history.RangeResponse{Points: []history.RangePoint{{Series: large}}}, nil
	}
	resp = serveRequest(handler, http.MethodGet, "/debug/statsviz/history/range?from=1&to=2", nil)
	if resp.Code != http.StatusRequestEntityTooLarge || resp.Body.Len() > maxHistoryResponseBytes {
		t.Fatalf("oversize response: status=%d bytes=%d", resp.Code, resp.Body.Len())
	}
}

func TestHistoryRangeKeepsConfiguredEvents(t *testing.T) {
	var captured history.RangeRequest
	store := &fakeHistoryStore{rangeFn: func(_ context.Context, req history.RangeRequest) (history.RangeResponse, error) {
		captured = req
		return history.RangeResponse{}, nil
	}}
	srv, err := NewServer(History(store))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	resp := serveRequest(srv.History(), http.MethodGet,
		"/debug/statsviz/history/range?from=1&to=2&plots=cpu-overall", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", resp.Code, resp.Body.String())
	}
	if got, want := strings.Join(captured.Plots, ","), "cpu-overall,lastgc"; got != want {
		t.Fatalf("plots = %q, want %q", got, want)
	}
}

func TestHistoryQueriesHaveTwoSecondDeadline(t *testing.T) {
	deadline := make(chan time.Duration, 1)
	store := &fakeHistoryStore{metaFn: func(ctx context.Context) (history.Meta, error) {
		when, ok := ctx.Deadline()
		if !ok {
			return history.Meta{}, errors.New("missing deadline")
		}
		deadline <- time.Until(when)
		return history.Meta{}, nil
	}}
	srv, err := NewServer(History(store))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	resp := serveRequest(srv.History(), http.MethodGet, "/debug/statsviz/history/meta", nil)
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", resp.Code, resp.Body.String())
	}
	remaining := <-deadline
	if remaining <= 0 || remaining > historyTimeout {
		t.Fatalf("query deadline remaining = %v, want (0, %v]", remaining, historyTimeout)
	}
}

func TestCloseWaitsForCollectorThenFlushesWithoutClosingStore(t *testing.T) {
	enqueueStarted := make(chan struct{})
	releaseEnqueue := make(chan struct{})
	flushCalled := make(chan struct{})
	var once sync.Once
	store := &fakeHistoryStore{
		enqueueFn: func(history.Sample) error {
			once.Do(func() { close(enqueueStarted) })
			<-releaseEnqueue
			return errors.New("disk failed")
		},
		flushFn: func(context.Context) error {
			close(flushCalled)
			return nil
		},
	}
	srv, err := NewServer(History(store), SendFrequency(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-enqueueStarted:
	case <-time.After(time.Second):
		t.Fatal("collector did not enter Enqueue")
	}

	closed := make(chan error, 1)
	go func() { closed <- srv.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before collector exited: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseEnqueue)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
	select {
	case <-flushCalled:
	default:
		t.Fatal("Close did not flush history")
	}
	if store.closed.Load() {
		t.Fatal("Server.Close closed the caller-owned store")
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("second Close returned %v", err)
	}
	var zero Server
	if err := zero.Close(); err != nil {
		t.Fatalf("zero Server.Close returned %v", err)
	}
	_ = zero.Index()
	if err := zero.Close(); err != nil {
		t.Fatalf("initialized zero Server.Close returned %v", err)
	}
}

func TestClosedZeroServerCannotStartCollector(t *testing.T) {
	var srv Server
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	resp := serveRequest(srv.Index(), http.MethodGet, "/debug/statsviz/", nil)
	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("Index after Close status = %d, want 503", resp.Code)
	}
	if srv.cancel != nil || srv.clients != nil {
		t.Fatal("closed zero Server initialized background resources")
	}
}

func TestHistoryStartRunConfigAndFailure(t *testing.T) {
	var captured history.RunConfig
	store := &fakeHistoryStore{startFn: func(_ context.Context, cfg history.RunConfig) (history.Run, error) {
		captured = cfg
		return history.Run{ID: 7}, nil
	}}
	custom, err := (TimeSeriesPlotConfig{
		Name: "request-rate",
		Series: []TimeSeries{{
			Name:        "requests",
			Aggregation: history.Rate,
			GetValue:    func() float64 { return 1 },
		}},
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(History(store), TimeseriesPlot(custom), SendFrequency(250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if captured.IntervalNS != (250*time.Millisecond).Nanoseconds() || captured.StartedAtMS == 0 || !json.Valid(captured.Config) {
		t.Fatalf("invalid run config: %+v", captured)
	}
	if got := captured.Aggregation["cgo"].Kinds; len(got) != 1 || got[0] != history.Delta {
		t.Fatalf("cgo aggregation = %v, want [delta]", got)
	}
	if got := captured.Aggregation["lastgc"].Kinds; len(got) != 1 || got[0] != history.Event {
		t.Fatalf("lastgc aggregation = %v, want [event]", got)
	}
	if got := captured.Aggregation["request-rate"].Kinds; len(got) != 1 || got[0] != history.Rate {
		t.Fatalf("custom aggregation = %v, want [rate]", got)
	}

	want := errors.New("migration failed")
	_, err = NewServer(History(&fakeHistoryStore{startFn: func(context.Context, history.RunConfig) (history.Run, error) {
		return history.Run{}, want
	}}))
	if !errors.Is(err, want) {
		t.Fatalf("NewServer error = %v, want wrapped %v", err, want)
	}
}

func serveRequest(handler http.Handler, method, target string, ctx context.Context) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	return resp
}
