package statsviz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/PinableAgents/statsviz/history"
)

const (
	defaultHistoryMaxPoints = 1500
	maxHistoryResponseBytes = 8 << 20
)

// History 启用由调用方持有生命周期的历史存储。
func History(store history.Store) Option {
	return func(s *Server) error {
		if store == nil {
			return errors.New("history store must not be nil")
		}
		s.historyStore = store
		return nil
	}
}

// History 返回 meta、range 和 point 共用的历史 HTTP 入口。
func (s *Server) History() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeHistoryError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if err := s.ensureInitialized(); err != nil {
			writeHistoryError(w, http.StatusServiceUnavailable, "history unavailable")
			return
		}

		endpoint := historyEndpoint(s.root, r.URL.Path)
		if endpoint != "meta" && endpoint != "range" && endpoint != "point" {
			writeHistoryError(w, http.StatusNotFound, "history endpoint not found")
			return
		}
		if s.historyStore == nil {
			if endpoint == "meta" {
				writeHistoryJSON(w, http.StatusOK, history.Meta{
					Enabled:    false,
					IntervalMS: s.interval.Milliseconds(),
					Runs:       []history.Run{},
					Watermarks: []history.Watermark{},
				})
				return
			}
			writeHistoryError(w, http.StatusServiceUnavailable, "history is disabled")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), historyTimeout)
		defer cancel()
		switch endpoint {
		case "meta":
			meta, err := s.historyStore.Meta(ctx)
			if err != nil {
				writeHistoryStoreError(w, err)
				return
			}
			meta.Enabled = true
			if meta.Runs == nil {
				meta.Runs = []history.Run{}
			}
			if meta.Watermarks == nil {
				meta.Watermarks = []history.Watermark{}
			}
			writeHistoryJSON(w, http.StatusOK, meta)
		case "range":
			req, err := parseRangeRequest(r)
			if err != nil {
				writeHistoryError(w, http.StatusBadRequest, err.Error())
				return
			}
			// 过滤图表时仍需事件序列来绘制叠加标记。事件数据很小，附带所有
			// 已配置事件也能兼容图表与事件关系发生过变化的历史运行。
			if len(req.Plots) > 0 {
				req.Plots = appendMissing(req.Plots, s.plots.Config().Events...)
			}
			resp, err := s.historyStore.Range(ctx, req)
			if err != nil {
				writeHistoryStoreError(w, err)
				return
			}
			normalizeRangeResponse(&resp)
			writeHistoryJSON(w, http.StatusOK, resp)
		case "point":
			req, err := parsePointRequest(r)
			if err != nil {
				writeHistoryError(w, http.StatusBadRequest, err.Error())
				return
			}
			resp, err := s.historyStore.Point(ctx, req)
			if err != nil {
				writeHistoryStoreError(w, err)
				return
			}
			writeHistoryJSON(w, http.StatusOK, resp)
		}
	}
}

func appendMissing(values []string, extra ...string) []string {
	seen := make(map[string]struct{}, len(values)+len(extra))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	for _, value := range extra {
		if _, ok := seen[value]; ok {
			continue
		}
		values = append(values, value)
		seen[value] = struct{}{}
	}
	return values
}

func historyEndpoint(root, path string) string {
	base := strings.TrimSuffix(root, "/")
	prefix := base + "/history/"
	if strings.HasPrefix(path, prefix) {
		return strings.Trim(strings.TrimPrefix(path, prefix), "/")
	}
	path = strings.Trim(path, "/")
	path = strings.TrimPrefix(path, "history/")
	return strings.Trim(path, "/")
}

func parseRangeRequest(r *http.Request) (history.RangeRequest, error) {
	from, err := requiredInt64(r, "from")
	if err != nil {
		return history.RangeRequest{}, err
	}
	to, err := requiredInt64(r, "to")
	if err != nil {
		return history.RangeRequest{}, err
	}
	if from >= to {
		return history.RangeRequest{}, errors.New("from must be less than to")
	}
	runID, err := optionalNonNegativeInt64(r, "run_id")
	if err != nil {
		return history.RangeRequest{}, err
	}
	maxPoints := defaultHistoryMaxPoints
	if raw, ok := r.URL.Query()["max_points"]; ok {
		if len(raw) == 0 || raw[0] == "" {
			return history.RangeRequest{}, errors.New("max_points must be a positive integer")
		}
		parsed, err := strconv.ParseInt(raw[0], 10, 32)
		if err != nil || parsed <= 0 {
			return history.RangeRequest{}, errors.New("max_points must be a positive integer")
		}
		maxPoints = int(parsed)
	}

	var plots []string
	if raw := r.URL.Query().Get("plots"); raw != "" {
		for _, name := range strings.Split(raw, ",") {
			name = strings.TrimSpace(name)
			if name == "" {
				return history.RangeRequest{}, errors.New("plots contains an empty name")
			}
			plots = append(plots, name)
		}
	}
	return history.RangeRequest{
		From:      from,
		To:        to,
		RunID:     runID,
		MaxPoints: maxPoints,
		Plots:     plots,
	}, nil
}

func parsePointRequest(r *http.Request) (history.PointRequest, error) {
	at, err := requiredInt64(r, "at")
	if err != nil {
		return history.PointRequest{}, err
	}
	runID, err := optionalNonNegativeInt64(r, "run_id")
	if err != nil {
		return history.PointRequest{}, err
	}
	tolerance, err := optionalNonNegativeInt64(r, "tolerance_ms")
	if err != nil {
		return history.PointRequest{}, err
	}
	return history.PointRequest{At: at, RunID: runID, ToleranceMS: tolerance}, nil
}

func requiredInt64(r *http.Request, name string) (int64, error) {
	raw, ok := r.URL.Query()[name]
	if !ok || len(raw) == 0 || raw[0] == "" {
		return 0, fmt.Errorf("%s is required", name)
	}
	value, err := strconv.ParseInt(raw[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return value, nil
}

func optionalNonNegativeInt64(r *http.Request, name string) (int64, error) {
	raw, ok := r.URL.Query()[name]
	if !ok {
		return 0, nil
	}
	if len(raw) == 0 || raw[0] == "" {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	value, err := strconv.ParseInt(raw[0], 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return value, nil
}

func normalizeRangeResponse(resp *history.RangeResponse) {
	if resp.Points == nil {
		resp.Points = []history.RangePoint{}
	}
	if resp.Runs == nil {
		resp.Runs = []history.Run{}
	}
	if resp.Gaps == nil {
		resp.Gaps = []history.Gap{}
	}
	if resp.Watermarks == nil {
		resp.Watermarks = []history.Watermark{}
	}
}

func writeHistoryStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, history.ErrInvalidQuery):
		writeHistoryError(w, http.StatusBadRequest, "invalid history query")
	case errors.Is(err, history.ErrTooLarge):
		writeHistoryError(w, http.StatusRequestEntityTooLarge, "history response is too large")
	default:
		writeHistoryError(w, http.StatusServiceUnavailable, "history unavailable")
	}
}

func writeHistoryError(w http.ResponseWriter, status int, message string) {
	writeHistoryJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: message})
}

func writeHistoryJSON(w http.ResponseWriter, status int, value any) {
	payload, err := json.Marshal(value)
	if err != nil {
		status = http.StatusServiceUnavailable
		payload = []byte(`{"error":"failed to encode history response"}`)
	}
	if len(payload) > maxHistoryResponseBytes {
		status = http.StatusRequestEntityTooLarge
		payload = []byte(`{"error":"history response is too large"}`)
	}
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}
