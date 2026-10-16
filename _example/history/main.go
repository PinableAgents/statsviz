package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/PinableAgents/statsviz"
	example "github.com/PinableAgents/statsviz/_example"
	sqlitehistory "github.com/PinableAgents/statsviz/history/sqlite"
)

func main() {
	// 使用独立文件，避免与应用的业务数据库共享连接和锁。
	dbPath := envOr("STATSVIZ_HISTORY_DB", "statsviz-history.db")
	store, err := sqlitehistory.Open(dbPath, sqlitehistory.Options{
		Retention: 7 * 24 * time.Hour,
	})
	if err != nil {
		log.Fatal(err)
	}

	srv, err := statsviz.NewServer(statsviz.History(store))
	if err != nil {
		_ = store.Close()
		log.Fatal(err)
	}
	// Statsviz 使用独立 mux 和可配置的回环地址监听器，与应用的公开服务分离。
	mux := http.NewServeMux()
	srv.Register(mux)

	listener, err := net.Listen("tcp", envOr("STATSVIZ_ADDR", "127.0.0.1:8095"))
	if err != nil {
		_ = srv.Close()
		_ = store.Close()
		log.Fatal(err)
	}
	httpServer := &http.Server{Handler: mux}
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- httpServer.Serve(listener)
	}()
	go example.Work()
	log.Printf("Statsviz: http://%s/debug/statsviz/", listener.Addr())
	log.Printf("SQLite history: %s", dbPath)

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	select {
	case <-signalCtx.Done():
	case err = <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server: %v", err)
		}
	}
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = httpServer.Shutdown(shutdownCtx)
	cancel()
	// 先停止采集并刷新队列，再关闭调用方持有的 SQLite Store。
	if err = srv.Close(); err != nil {
		log.Printf("flush history: %v", err)
	}
	if err = store.Close(); err != nil {
		log.Printf("close history store: %v", err)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
