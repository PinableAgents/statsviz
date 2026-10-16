package sqlite

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PinableAgents/statsviz/history"
)

func TestDatabaseURLKeepsVolumeInPath(t *testing.T) {
	for _, tc := range []struct {
		name, path, wantPath string
	}{
		{"Unix", "/home/监控 数据 #100%/history.db", "/home/监控 数据 #100%/history.db"},
		{"Windows 盘符", "C:/Users/监控 数据 #100%/history.db", "/C:/Users/监控 数据 #100%/history.db"},
		{"UNC", "//server/share/监控 数据/history.db", "//server/share/监控 数据/history.db"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := url.Parse(databaseURL(tc.path).String())
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Scheme != "file" || parsed.Host != "" || parsed.Path != tc.wantPath || parsed.RawQuery != "" || parsed.Fragment != "" {
				t.Fatalf("数据库路径被解释为 URI 其他部分: %+v", parsed)
			}
		})
	}
}

func TestHistoryPathWithUnicodeAndURICharacters(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "监控 数据 #100%")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "history.db")
	store, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	started := time.Now().UnixMilli()
	run := startTestRun(t, store, started, int64(time.Second), nil, `{}`)
	if err := enqueue(t, store, run.ID, 1, started, int64(time.Second), `{"x":[42]}`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("未在原始路径创建历史库: %v", err)
	}
	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	point, err := reopened.Point(context.Background(), history.PointRequest{At: started, RunID: run.ID})
	if err != nil || !point.Found || point.Sample == nil || string(point.Sample.Series) != `{"x":[42]}` {
		t.Fatalf("重开后历史数据不可读: point=%+v err=%v", point, err)
	}
}
